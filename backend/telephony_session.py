from __future__ import annotations

import asyncio
import base64
import json
import os

from fastapi import WebSocket
from starlette.websockets import WebSocketState

import cartesia_client
import cartesia_realtime
import sheets_logger
import telephony_audio
from session_base import BaseSession, GREETING_SENTINEL
from telephony_vad import ServerVAD

# G.711 μ-law is always 8kHz for PSTN — this is a telephony-transport constant,
# not a Cartesia one, so it doesn't belong in cartesia_realtime.py alongside
# STT_SAMPLE_RATE.
TELEPHONY_SAMPLE_RATE = 8000

# 20ms of 8kHz 16-bit linear PCM (2 bytes/sample). Switched from 160-byte
# G.711 μ-law frames after confirming (2026-08-28, session 5007580724) that
# instrumented, error-free, connection-checked send_bytes() calls for 400+
# consecutive μ-law frames still produced total silence on the caller's end
# -- the bytes definitely leave our process and reach a live, open
# WebSocket, so the drop has to be happening inside VoxEngine's
# ws.sendMediaTo(call) route itself. The one asymmetry visible in
# VoxEngine's own session log: the call->ws leg's SendMedia command logs
# `encoding = ULAW` (and its 160-byte-per-20ms framing matches ULAW's 1
# byte/sample exactly), but the ws->call leg's SendMedia command -- same
# {encoding: WebSocketAudioEncoding.ULAW} option, same API shape -- never
# shows an encoding field at all. Per Voximplant's own docs, an unset/
# unrecognized encoding on this parameter defaults to PCM8 (their name for
# 16-bit linear PCM at 8kHz, not 8-bit samples), i.e. 320 bytes/20ms, not
# 160 -- so if that option silently isn't taking effect only for this
# direction, we've been feeding it half-size μ-law frames it may be
# decoding as malformed/undersized PCM8 samples. Sending PCM8-shaped frames
# directly sidesteps the question of whether the encoding option works at
# all for this leg.
TELEPHONY_FRAME_SAMPLES = 160
TELEPHONY_FRAME_BYTES = TELEPHONY_FRAME_SAMPLES * 2

# How far ahead of strict real-time the outbound pacer is allowed to stay
# (see _pace_outbound_audio). Razor-exact frame-by-frame real-time pacing
# (no lookahead at all) produced COMPLETE SILENCE on a real call (confirmed
# live 2026-08-28, session 5007178736), even though the same pacer/queue
# logic verified correct in an isolated local simulation (400/400 frames
# drained at ~20ms average intervals) -- so the bug isn't in our pacing
# arithmetic. The version before that (no pacing at all, full burst) gave
# garbled-but-audible playback, not silence. Leading hypothesis: VoxEngine's
# native ws.sendMediaTo(call) route needs some buffered lookahead to
# start/keep playing cleanly (like any jitter buffer), and zero-margin
# pacing -- already subject to real asyncio/network jitter under load --
# left it with nothing to draw on. This keeps a fixed ~200ms of audio
# permanently sent ahead of the strict real-time schedule instead.
OUTBOUND_LOOKAHEAD_S = 0.2

# Fallback unmute timing: there's no client on this transport to report real
# playback completion the way the browser's "bot_playback_done" does (see
# BaseSession.pump_tts_audio's "done" handling, which only means Cartesia
# finished *generating* audio, not that Voximplant finished *playing* it into
# the call) — estimate playback duration from character count instead.
# CHARS_PER_SECOND is calibrated from one real call's greeting (~110
# characters, ~8.2s of audio). PLAYBACK_ESTIMATE_PAD_S covers
# buffering/network latency before audio actually reaches the caller —
# erring toward unmuting a bit late is far less disruptive than unmuting
# while the bot is still actually talking.
CHARS_PER_SECOND = 13.0
PLAYBACK_ESTIMATE_PAD_S = 1.5

# The browser transport's barge-in thresholds (settings_store.DEFAULT_VAD)
# are tuned for a mic signal that's already gone through getUserMedia's
# built-in echo cancellation, so the bot's own played-back audio is stripped
# out before VAD ever sees it. PSTN calls have no such AEC -- raw line audio
# reaches us as-is, including the far end's line/hybrid echo of our own
# greeting bouncing back. Confirmed live: at those thresholds the bot
# barge-in-cancelled its own greeting ~3s after playback started, well
# before any real caller speech was possible. Only barge-in needs raising --
# normal (not-bot-speaking) speech detection isn't exposed to this echo path.
#
# The false trigger above happened at ENERGY_THRESHOLD=0.035 -- line echo is
# heavily attenuated relative to direct near-end speech, so the much higher
# 0.15 here is the primary defense against it, not the duration. Confirmed
# live (2026-08): at 700ms, a real short reply ("алё") never registered at
# all -- the word itself often doesn't last 700ms of continuous above-
# threshold audio. Lowered to 400ms, still well above the browser
# transport's own 250ms barge-in default, to keep an extra safety margin
# against echo.
#
# 0.15 turned out to be past the other edge: measured against real speech run
# through this exact μ-law + resample path, it stops firing entirely at around
# -22 dBFS average speech level, which is an ordinary (not even quiet) PSTN
# level -- only near-clipping audio crosses it. So barge-in was effectively
# switched off, and with the unmute bug that used to accompany it (see
# _on_outbound_queue_drained) that left the caller unable to be heard at all.
# 0.08 is the compromise: still >2x the browser default that echo defeated,
# but reachable by a normally-loud caller.
TELEPHONY_BARGE_IN_ENERGY_THRESHOLD = 0.08
TELEPHONY_BARGE_IN_SPEECH_START_MS = 400

# Slack between "the last frame of this turn left the pacer" and treating the
# bot as done talking -- see _unmute_after_drain.
PLAYBACK_DRAIN_PAD_S = 0.4


def _extract_media_payload(data: dict | None) -> bytes | None:
    """Pull raw μ-law bytes out of a Voximplant JSON media-event message
    ({"event":"media","media":{"payload":"<base64>"}})."""
    if not isinstance(data, dict) or data.get("event") != "media":
        return None
    payload_b64 = (data.get("media") or {}).get("payload")
    if not payload_b64:
        return None
    return base64.b64decode(payload_b64)


class TelephonySession(BaseSession):
    """Phone call (via Voximplant): inbound audio (caller -> us) is μ-law@8kHz
    bridged over a WebSocket Voximplant's VoxEngine scenario opens to us,
    decoded and fed into Cartesia STT the same way CallSession feeds its mic
    audio -- server-side VAD instead of mic-worklet.js, since there's no
    browser here.

    Outbound audio (bot -> caller) is synthesized locally with Cartesia, the
    same as CallSession/the browser transport (see BaseSession.pump_tts_audio
    and this class's _emit_tts_audio override below), then resampled to
    8kHz/μ-law and pushed to VoxEngine as binary WebSocket frames, paced to
    near-real-time (with a small lookahead buffer, not razor-exact -- see
    OUTBOUND_LOOKAHEAD_S) by _pace_outbound_audio (Cartesia synthesizes far
    faster than real time -- sending its output unpaced garbled playback on
    a real call, confirmed live 2026-08-28; exact zero-lookahead pacing then
    produced complete silence on a later call, also confirmed live).
    bridge.voxengine.js routes those frames into the call via
    ws.sendMediaTo(call, {encoding: ULAW}), the mirror image of
    call.sendMediaTo(ws, ...) which already carries the caller's audio to us.

    An earlier version of this class instead handed reply text to VoxEngine's
    own native Cartesia connector (Cartesia.createRealtimeTTSPlayer) and let
    it synthesize/play the audio itself -- that connector never played more
    than the first ~300ms of any utterance on a real call, independent of API
    key/model/voice/message shape. See bridge.voxengine.js's docstring for
    the full story."""

    def __init__(
        self,
        ws: WebSocket,
        call_id: str | None = None,
        phone_number: str | None = None,
        client_name: str | None = None,
    ):
        super().__init__(ws)
        self.call_id = call_id
        self.recorder.phone_number = phone_number
        self.recorder.client_name = client_name
        if client_name:
            self.history[0]["content"] += (
                f"\n\nИмя собеседника: {client_name}. Обратись по имени естественно, "
                f"не более 1-2 раз за разговор."
            )
        self.stt_ws = None
        vad_settings = dict(self.settings["vad"])
        vad_settings["barge_in_energy_threshold"] = TELEPHONY_BARGE_IN_ENERGY_THRESHOLD
        vad_settings["barge_in_speech_start_ms"] = TELEPHONY_BARGE_IN_SPEECH_START_MS
        self.vad = ServerVAD(vad_settings, sample_rate=cartesia_realtime.STT_SAMPLE_RATE)
        self._inbound_chunks_received = 0
        self._outbound_chunks_sent = 0
        self._turn_char_count = 0
        self._unmute_task: asyncio.Task | None = None
        # Set once Cartesia has generated a turn's last audio chunk, cleared
        # when the pacer has drained that audio onto the wire -- together
        # they're this transport's stand-in for the browser's real
        # end-of-playback signal. See _on_outbound_queue_drained.
        self._generation_done_context: str | None = None
        # Cartesia synthesizes far faster than real time (confirmed live:
        # 2026-08-28, an ~8s greeting arrived in full within ~900ms of the
        # first chunk) -- pushing that straight to send_bytes floods the
        # call's real-time RTP leg with frames all at once instead of one
        # 20ms frame per 20ms of wall time, which VoxEngine's native
        # ws.sendMediaTo(call) route can't play back cleanly (heard live as
        # garbled crackle/rustle, not speech). This queue + _pace_outbound_audio
        # decouple synthesis speed from playback speed.
        self._outbound_queue: asyncio.Queue[bytes] = asyncio.Queue()

    async def run(self):
        self.voice_id = self.settings["voice_id"] or await cartesia_client.get_default_voice_id()
        self.stt_ws = await cartesia_realtime.open_stt_ws()
        self.tts_ws = await cartesia_realtime.open_tts_ws()

        tasks = [
            asyncio.create_task(self.pump_phone_audio()),
            asyncio.create_task(self.pump_stt_transcripts()),
            asyncio.create_task(self.pump_tts_audio()),
            asyncio.create_task(self._pace_outbound_audio()),
        ]
        # Bot speaks first, like answering a phone call -- same fixed,
        # operator-edited greeting as the browser transport (settings_store.py).
        self.launch_bot_turn(GREETING_SENTINEL)

        try:
            # The call ends as soon as any one pump exits (e.g. Voximplant
            # hangs up); gather() would hang forever waiting on the other
            # open Cartesia connections.
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                exc = task.exception()
                if exc:
                    raise exc
        finally:
            if self._unmute_task and not self._unmute_task.done():
                self._unmute_task.cancel()
            for task in tasks:
                task.cancel()
            for ws in (self.stt_ws, self.tts_ws):
                try:
                    await ws.close()
                except Exception:
                    pass
            try:
                await self.recorder.finalize()
            except Exception:
                pass
            try:
                await sheets_logger.append_call_row(self.recorder.sheet_summary())
            except Exception:
                pass

    async def send_json(self, payload: dict) -> None:
        """No UI is listening on this transport, but every event still gets
        logged -- silently discarding "error" events here would be exactly
        the kind of swallowed failure this whole codebase deliberately never
        does."""
        print(f"[telephony:{self.recorder.call_id}] {payload}")

    async def _send_control(self, payload: dict) -> None:
        if self.ws.client_state == WebSocketState.CONNECTED:
            await self.ws.send_json(payload)

    async def _send_tts_text(self, text: str, context_id: str, keep_open: bool) -> None:
        """BaseSession's hook synthesizes text via self.tts_ws same as the
        browser transport. On top of that, telephony has no client to report
        real playback completion (see CHARS_PER_SECOND above), so arm the
        character-count backstop here, at the same point the old
        VoxEngine-control-message version used to."""
        await super()._send_tts_text(text, context_id, keep_open)
        self._turn_char_count += len(text)
        if not keep_open:
            self._schedule_unmute_fallback(context_id, self._turn_char_count)
            self._turn_char_count = 0

    def _schedule_unmute_fallback(self, context_id: str, char_count: int) -> None:
        if self._unmute_task and not self._unmute_task.done():
            self._unmute_task.cancel()
        estimated_s = char_count / CHARS_PER_SECOND + PLAYBACK_ESTIMATE_PAD_S
        print(f"[telephony:{self.recorder.call_id}] unmute backstop armed: {estimated_s:.1f}s "
              f"for {char_count} chars")
        self._unmute_task = asyncio.create_task(self._unmute_after_estimate(context_id, estimated_s))

    async def _unmute_after_estimate(self, context_id: str, estimated_s: float) -> None:
        await asyncio.sleep(estimated_s)
        # Stale if a barge-in or a newer turn already moved active_context_id
        # on -- this fallback only fires for a turn that finished naturally,
        # uninterrupted.
        if context_id == self.active_context_id:
            print(f"[telephony:{self.recorder.call_id}] unmuting via char-count backstop "
                  f"(the outbound queue never drained -- pacer stalled?)")
            self._unmute(context_id)

    def _unmute(self, context_id: str) -> None:
        if context_id != self.active_context_id or not self.bot_speaking:
            return
        self.bot_speaking = False
        self.vad.set_bot_speaking(False)

    async def _on_tts_generation_done(self, context_id: str) -> None:
        """Cartesia has generated the last chunk of this turn, so whatever is
        still sitting in _outbound_queue is now the entire rest of the turn --
        which makes the queue draining a real end-of-playback signal rather
        than an estimate. Recorded here; _pace_outbound_audio acts on it."""
        self._generation_done_context = context_id

    def _on_outbound_queue_drained(self) -> None:
        """The pacer sends one 20ms frame per 20ms of wall time, so an empty
        queue after generation finished means the bot has (as near as this
        transport can know) just stopped talking. This is telephony's
        equivalent of the browser's "bot_playback_done", and it replaces the
        character-count estimate as the primary unmute: that estimate
        (CHARS_PER_SECOND) silently muted whole calls whenever a reply ran
        long -- a 380-character greeting estimated 30.8s of playback and so
        gated out every caller chunk of an 18-second call, which is why the
        bot appeared not to hear the caller at all (confirmed from production
        logs, 2026-08-28). The estimate stays on only as a backstop for a
        stalled pacer."""
        context_id = self._generation_done_context
        if context_id is None:
            return
        self._generation_done_context = None
        if self._unmute_task and not self._unmute_task.done():
            self._unmute_task.cancel()
        self._unmute_task = asyncio.create_task(self._unmute_after_drain(context_id))

    async def _unmute_after_drain(self, context_id: str) -> None:
        # The last frame is on the wire but not yet out of VoxEngine's own
        # jitter buffer and into the caller's ear; unmuting on the exact
        # frame boundary would let the tail of the bot's own voice echo back
        # in as "caller speech".
        await asyncio.sleep(PLAYBACK_DRAIN_PAD_S)
        self._unmute(context_id)

    async def _emit_tts_audio(self, pcm: bytes) -> None:
        """Override of BaseSession's hook: resample the synthesized
        PCM16@24kHz chunk down to telephony's PCM16@8kHz and hand it to
        VoxEngine as raw binary WS frames -- no G.711 encoding step. See
        TELEPHONY_FRAME_BYTES's docstring for why: μ-law frames on this
        leg were confirmed being sent successfully (instrumented,
        error-free, connection-checked) yet never audible, and VoxEngine's
        own session log shows this leg's SendMedia command silently
        dropping the `encoding` option that the other (confirmed-working)
        leg honors -- so this sends the format VoxEngine's docs say it
        defaults to instead of trusting that option to take effect.

        Sliced into 320-byte/20ms frames, but NOT sent directly here --
        only enqueued. Cartesia synthesizes far faster than real time, so
        this hook can get called with several seconds of audio within a
        couple hundred milliseconds; _pace_outbound_audio is what actually
        drains the queue onto the wire at the real 20ms/frame cadence the
        call's live RTP leg needs."""
        pcm_8k = telephony_audio.resample_linear(pcm, cartesia_realtime.TTS_SAMPLE_RATE, TELEPHONY_SAMPLE_RATE)
        self._outbound_chunks_sent += 1
        if self._outbound_chunks_sent == 1:
            print(f"[telephony:{self.recorder.call_id}] first outbound TTS audio ready to send "
                  f"({len(pcm_8k)} bytes pcm16@8k, sliced into {TELEPHONY_FRAME_BYTES}-byte binary frames)")
        for offset in range(0, len(pcm_8k), TELEPHONY_FRAME_BYTES):
            frame = pcm_8k[offset:offset + TELEPHONY_FRAME_BYTES]
            await self._outbound_queue.put(frame)

    async def _pace_outbound_audio(self) -> None:
        """Drains self._outbound_queue onto the WS at one TELEPHONY_FRAME_BYTES
        frame per TELEPHONY_FRAME_BYTES/TELEPHONY_SAMPLE_RATE seconds (20ms),
        regardless of how bursty _emit_tts_audio's enqueuing is -- but
        staying OUTBOUND_LOOKAHEAD_S ahead of strict real time throughout,
        not locked to it frame-for-frame (see that constant's docstring for
        why: exact pacing produced silence on a real call). Concretely:
        next_send trails "now" by OUTBOUND_LOOKAHEAD_S whenever it's reset,
        which bursts through the first ~OUTBOUND_LOOKAHEAD_S/frame_interval
        frames immediately (priming that lookahead buffer) before settling
        into real steady-state 20ms/frame pacing that keeps the same margin.
        Reset (rather than left to keep drifting) after an idle gap bigger
        than one frame + the lookahead -- between turns, or while the
        caller is talking -- so a long pause doesn't leave a huge backlog
        that would otherwise dump back-to-back the instant new audio
        arrives."""
        frame_interval = TELEPHONY_FRAME_SAMPLES / TELEPHONY_SAMPLE_RATE
        next_send: float | None = None
        frames_sent = 0
        frames_dropped = 0
        while True:
            frame = await self._outbound_queue.get()
            now = asyncio.get_event_loop().time()
            if next_send is None or now - next_send > frame_interval + OUTBOUND_LOOKAHEAD_S:
                next_send = now - OUTBOUND_LOOKAHEAD_S
            elif next_send > now:
                await asyncio.sleep(next_send - now)
            if self.ws.client_state == WebSocketState.CONNECTED:
                try:
                    await self.ws.send_bytes(frame)
                except Exception as exc:
                    # Silent WS-send failures here would be indistinguishable
                    # from "nothing to send" -- this is exactly the gap that
                    # let a real call go completely silent with no error
                    # anywhere in the logs (2026-08-28, session 5007264702).
                    print(f"[telephony:{self.recorder.call_id}] _pace_outbound_audio: "
                          f"send_bytes failed after {frames_sent} frames sent: {exc!r}")
                    raise
                frames_sent += 1
                if frames_sent == 1 or frames_sent % 50 == 0:
                    print(f"[telephony:{self.recorder.call_id}] {frames_sent} outbound audio "
                          f"frames sent to Voximplant so far")
            else:
                frames_dropped += 1
                if frames_dropped == 1 or frames_dropped % 50 == 0:
                    print(f"[telephony:{self.recorder.call_id}] _pace_outbound_audio: dropped "
                          f"{frames_dropped} frames so far -- ws.client_state is "
                          f"{self.ws.client_state!r}, not CONNECTED")
            next_send += frame_interval
            if self._outbound_queue.empty():
                self._on_outbound_queue_drained()

    def _clear_outbound_queue(self) -> None:
        """Barge-in: drop whatever's still queued from the interrupted turn
        so it doesn't keep draining out after the caller's already been
        told (via tts_cancel/ws.stopMediaTo) that playback stopped."""
        while not self._outbound_queue.empty():
            try:
                self._outbound_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    async def pump_phone_audio(self):
        while True:
            message = await self.ws.receive()
            if message["type"] == "websocket.disconnect":
                return

            audio_bytes = message.get("bytes")
            if audio_bytes is None:
                text = message.get("text")
                try:
                    data = json.loads(text) if text is not None else None
                except ValueError:
                    data = None

                if isinstance(data, dict) and data.get("type") == "diag":
                    # Piggybacked from bridge.voxengine.js -- VoxEngine's own
                    # Logger.write output isn't reachable from here (needs a
                    # browser-session login to fetch), so the scenario reports
                    # its own media-routing lifecycle over this same
                    # WebSocket instead.
                    print(f"[telephony:{self.recorder.call_id}] [voxengine diag] {data}")
                    continue

                audio_bytes = _extract_media_payload(data)
                if audio_bytes is None:
                    # Logged in full: confirmed live these are "start"
                    # ({"event":"start","start":{"mediaFormat":{"encoding":
                    # "ULAW","sampleRate":8000}}}) and "stop" ({"event":"stop",
                    # "stop":{"mediaInfo":{"bytesSent":N,"duration":N}}}) --
                    # neither carries a streamSid or anything else actionable,
                    # so there's nothing to do here beyond this log line.
                    print(f"[telephony:{self.recorder.call_id}] non-media event from Voximplant: {text!r}")
                    continue

            self._inbound_chunks_received += 1
            if self._inbound_chunks_received == 1:
                print(f"[telephony:{self.recorder.call_id}] first inbound audio chunk received "
                      f"({len(audio_bytes)} bytes) -- Voximplant->backend audio direction is alive")
            elif self._inbound_chunks_received % 50 == 0:
                print(f"[telephony:{self.recorder.call_id}] {self._inbound_chunks_received} inbound "
                      f"audio chunks received so far")

            # Edge case (accepted, not fixed -- the browser transport has the
            # exact same gap in mic-worklet.js's equivalent state machine):
            # ServerVAD only evaluates barge_in at the silence->speech
            # transition. If the caller is *already* mid-utterance in the
            # narrow window before bot_speaking flips True (e.g. talking right
            # as a turn starts), that speech never gets flagged as a barge-in
            # and gets silently dropped until it ends and a fresh speech_start
            # fires. Confirmed via a live test forcing this exact race;
            # interrupting anywhere else mid-turn (the realistic case) works
            # correctly -- also confirmed live.
            self.vad.set_bot_speaking(self.bot_speaking)
            pcm_8k = telephony_audio.ulaw_to_pcm16(audio_bytes)
            pcm_16k = telephony_audio.resample_linear(pcm_8k, TELEPHONY_SAMPLE_RATE, cartesia_realtime.STT_SAMPLE_RATE)
            event = self.vad.process(pcm_16k)
            if event.speech_start:
                print(f"[telephony:{self.recorder.call_id}] VAD: speech_start (barge_in={event.barge_in})")
            if event.speech_end:
                print(f"[telephony:{self.recorder.call_id}] VAD: speech_end")

            if event.barge_in:
                # Caller started talking over the bot -- cancel the in-flight
                # turn and tell VoxEngine to stop playback right away (the
                # media route keeps forwarding whatever's already buffered on
                # the call leg unless we explicitly tell it to stop). Mirrors
                # CallSession's "interrupt" control message.
                stale_context_id = self.active_context_id
                self.active_context_id = None  # any late chunk/done for the old turn is now stale
                if self.bot_turn_task and not self.bot_turn_task.done():
                    self.bot_turn_task.cancel()
                if self._unmute_task and not self._unmute_task.done():
                    self._unmute_task.cancel()
                self.bot_speaking = False
                self.vad.set_bot_speaking(False)
                self._clear_outbound_queue()
                # The cancelled turn's characters never got their closing
                # (keep_open=False) flush, which is the only place this
                # counter is normally reset -- leaving them to be added to
                # the NEXT turn's count, inflating every later playback
                # estimate monotonically once a call has been interrupted.
                self._turn_char_count = 0
                self._generation_done_context = None
                if stale_context_id is not None:
                    await self._send_control({"type": "tts_cancel", "context_id": stale_context_id})

            if not self.bot_speaking:
                for chunk in event.forward_chunks:
                    await self.stt_ws.send(chunk)
                    self.recorder.record_mic_chunk(chunk)

                if event.speech_end:
                    # Marked here, not on flush_done in pump_stt_transcripts:
                    # this is the moment the caller was actually done talking
                    # (VAD hangover already elapsed) -- marking it later, after
                    # the STT finalize round-trip, would exclude that round-trip
                    # from the logged latency numbers.
                    self.recorder.mark_speech_end()
                    await self.stt_ws.send("finalize")
