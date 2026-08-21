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
from call_recorder import RECORDING_SAMPLE_RATE
from session_base import BaseSession, GREETING_SENTINEL
from telephony_vad import ServerVAD

# G.711 μ-law is always 8kHz for PSTN — this is a telephony-transport constant,
# not a Cartesia one, so it doesn't belong in cartesia_realtime.py alongside
# STT_SAMPLE_RATE.
TELEPHONY_SAMPLE_RATE = 8000

# Fallback unmute timing: confirmed live, twice, that NEITHER
# PlayerEvents.PlaybackFinished NOR AudioChunksPlaybackFinished ever fires --
# not even for a single well-formed continue:false request (the fixed
# greeting, which never goes through the LLM streaming path at all). Without
# some signal, bot_speaking never resets and the whole rest of the call gets
# evaluated against the stricter barge-in thresholds instead of normal
# listening ones. Since we can't get a real completion signal, estimate
# playback duration from character count instead -- CHARS_PER_SECOND is
# calibrated from one real call's greeting (~110 characters, Player.Started
# to Player.Buffering was ~8.2s). PLAYBACK_ESTIMATE_PAD_S covers
# buffering/network latency before audio actually reaches the caller --
# erring toward unmuting a bit late is far less disruptive than unmuting
# while the bot is still actually talking.
CHARS_PER_SECOND = 13.0
PLAYBACK_ESTIMATE_PAD_S = 1.5

# Sessions register themselves here (by the call_id VoxEngine generates and
# passes to both WebSockets it opens for one call) so the recording
# sub-channel below can find the right CallRecorder to feed. Module-level,
# not per-instance, because the two WebSocket connections for one call arrive
# as two independent FastAPI requests with no other shared state.
_active_sessions: dict[str, "TelephonySession"] = {}
_REGISTRATION_WAIT_S = 3.0


def _extract_media_payload(data: dict | None) -> bytes | None:
    """Pull raw μ-law bytes out of a Voximplant JSON media-event message
    ({"event":"media","media":{"payload":"<base64>"}}) -- shared by the main
    inbound-audio pump and the recording sub-channel pump below, both of
    which receive this exact same envelope shape from Voximplant."""
    if not isinstance(data, dict) or data.get("event") != "media":
        return None
    payload_b64 = (data.get("media") or {}).get("payload")
    if not payload_b64:
        return None
    return base64.b64decode(payload_b64)


async def pump_recording_audio(ws: WebSocket, call_id: str) -> None:
    """Second WebSocket VoxEngine opens per call (see bridge.voxengine.js),
    purely to mirror a copy of the Cartesia player's synthesized audio back
    to us for the call recording. This has to be a SEPARATE connection from
    the main one: Voximplant's JSON media envelope carries no field
    distinguishing "this is the caller" from "this is the bot" -- mixing
    both into the main pump would feed the bot's own voice into VAD/STT as
    if the caller had said it.

    VoxEngine may open this connection essentially concurrently with the
    main one, so the matching TelephonySession might not have registered
    itself in _active_sessions yet -- wait briefly rather than dropping the
    very start of the call's recording."""
    session = _active_sessions.get(call_id)
    waited = 0.0
    while session is None and waited < _REGISTRATION_WAIT_S:
        await asyncio.sleep(0.1)
        waited += 0.1
        session = _active_sessions.get(call_id)
    if session is None:
        print(f"[telephony recording] no session registered for call_id={call_id!r} after "
              f"{_REGISTRATION_WAIT_S}s, dropping connection")
        return

    while True:
        message = await ws.receive()
        if message["type"] == "websocket.disconnect":
            return
        audio_bytes = message.get("bytes")
        if audio_bytes is None:
            try:
                data = json.loads(message.get("text") or "")
            except ValueError:
                continue
            audio_bytes = _extract_media_payload(data)
            if audio_bytes is None:
                continue
        pcm_8k = telephony_audio.ulaw_to_pcm16(audio_bytes)
        pcm_recording_rate = telephony_audio.resample_linear(pcm_8k, TELEPHONY_SAMPLE_RATE, RECORDING_SAMPLE_RATE)
        session.recorder.record_tts_chunk(pcm_recording_rate)


class TelephonySession(BaseSession):
    """Phone call (via Voximplant): inbound audio (caller -> us) is μ-law@8kHz
    bridged over a WebSocket Voximplant's VoxEngine scenario opens to us,
    decoded and fed into Cartesia STT the same way CallSession feeds its mic
    audio -- server-side VAD instead of mic-worklet.js, since there's no
    browser here.

    Outbound audio (bot -> caller) is NOT bridged the same way. Four attempts
    at pushing synthesized audio back over our own WebSocket for Voximplant to
    route into the call (raw binary, two JSON envelope shapes, 160-byte fixed
    framing matching Voximplant's own inbound chunk size) all reached the
    WebSocket confirmed-live but never produced audible sound on a real call.
    Research into Voximplant's API turned up a native Cartesia connector
    (`require(Modules.Cartesia)`, `Cartesia.createRealtimeTTSPlayer`) built
    for exactly this -- getting synthesized speech into a live call -- so
    instead of fighting the raw-audio path further, TelephonySession sends
    reply TEXT to VoxEngine (see BaseSession._send_tts_text override below)
    and lets VoxEngine's own Cartesia integration synthesize and play it,
    the same way `call.sendMediaTo(ws)` (a native media unit sending TO a
    websocket) is the one direction that's ever actually worked here --
    `player.sendMediaTo(call)` uses that same, proven direction, just with
    the player as the source instead of the call.

    A second WebSocket (see pump_recording_audio above and
    bridge.voxengine.js) mirrors a copy of the bot's synthesized audio back
    to us purely for the call recording -- otherwise we'd never see it at
    all, since VoxEngine plays it straight into the call itself."""

    def __init__(self, ws: WebSocket, call_id: str | None = None):
        super().__init__(ws)
        self.call_id = call_id
        self.stt_ws = None
        self.vad = ServerVAD(self.settings["vad"], sample_rate=cartesia_realtime.STT_SAMPLE_RATE)
        self._inbound_chunks_received = 0
        self._turn_char_count = 0
        self._unmute_task: asyncio.Task | None = None

    async def run(self):
        if self.call_id:
            _active_sessions[self.call_id] = self
        self.voice_id = self.settings["voice_id"] or await cartesia_client.get_default_voice_id()
        self.stt_ws = await cartesia_realtime.open_stt_ws()
        # No self.tts_ws here (unlike CallSession) -- VoxEngine's own Cartesia
        # connector does synthesis now; see the class docstring above.

        tasks = [
            asyncio.create_task(self.pump_phone_audio()),
            asyncio.create_task(self.pump_stt_transcripts()),
        ]
        # Bot speaks first, like answering a phone call -- same fixed,
        # operator-edited greeting as the browser transport (settings_store.py).
        self.launch_bot_turn(GREETING_SENTINEL)

        try:
            # The call ends as soon as any one pump exits (e.g. Voximplant
            # hangs up); gather() would hang forever waiting on the other
            # open Cartesia connection.
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                exc = task.exception()
                if exc:
                    raise exc
        finally:
            if self.call_id:
                _active_sessions.pop(self.call_id, None)
            if self._unmute_task and not self._unmute_task.done():
                self._unmute_task.cancel()
            for task in tasks:
                task.cancel()
            try:
                await self.stt_ws.close()
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
        does. Unlike before, this no longer drives any mute/unmute timing --
        VoxEngine tells us directly when playback finishes (see
        pump_phone_audio's "playback_finished" handling), so there's nothing
        left to estimate here."""
        print(f"[telephony:{self.recorder.call_id}] {payload}")

    async def _send_control(self, payload: dict) -> None:
        if self.ws.client_state == WebSocketState.CONNECTED:
            await self.ws.send_json(payload)

    async def _send_tts_text(self, text: str, context_id: str, keep_open: bool) -> None:
        """Override of BaseSession's hook: hand the text to VoxEngine's own
        Cartesia connector instead of synthesizing it ourselves (see class
        docstring). model_id/language are sent per-message rather than
        hardcoded in the VoxEngine scenario so an operator changing them via
        /prompt.html takes effect on the next call without touching the
        scenario script."""
        self._turn_char_count += len(text)
        await self._send_control({
            "type": "tts_text",
            "context_id": context_id,
            "text": text,
            "continue": keep_open,
            "voice_id": self.voice_id,
            "model_id": os.environ.get("CARTESIA_TTS_MODEL", "sonic-3.5"),
            "language": os.environ.get("CARTESIA_LANGUAGE", "ru"),
        })
        if not keep_open:
            self._schedule_unmute_fallback(context_id, self._turn_char_count)
            self._turn_char_count = 0

    def _schedule_unmute_fallback(self, context_id: str, char_count: int) -> None:
        if self._unmute_task and not self._unmute_task.done():
            self._unmute_task.cancel()
        estimated_s = char_count / CHARS_PER_SECOND + PLAYBACK_ESTIMATE_PAD_S
        self._unmute_task = asyncio.create_task(self._unmute_after_estimate(context_id, estimated_s))

    async def _unmute_after_estimate(self, context_id: str, estimated_s: float) -> None:
        await asyncio.sleep(estimated_s)
        # Stale if a barge-in or a newer turn already moved active_context_id
        # on -- this fallback only fires for a turn that finished naturally,
        # uninterrupted.
        if context_id == self.active_context_id:
            self.bot_speaking = False
            self.vad.set_bot_speaking(False)

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

                if isinstance(data, dict) and data.get("type") == "playback_finished":
                    # Our own control message (the "type" key distinguishes it
                    # from Voximplant's own "event"-keyed media/start/stop
                    # messages below). In practice this has never actually
                    # fired on a real call (see _unmute_after_estimate's
                    # fallback, which is what actually does this job) -- kept
                    # as a possible EARLY unmute in case it ever does.
                    if data.get("context_id") == self.active_context_id:
                        if self._unmute_task and not self._unmute_task.done():
                            self._unmute_task.cancel()
                        self.bot_speaking = False
                        self.vad.set_bot_speaking(False)
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
                # turn and tell VoxEngine to stop playback right away (unlike
                # the browser/estimated-duration path, VoxEngine's Cartesia
                # player keeps playing whatever's already buffered unless we
                # explicitly tell it to stop). Mirrors CallSession's
                # "interrupt" control message.
                stale_context_id = self.active_context_id
                self.active_context_id = None  # any late chunk/done for the old turn is now stale
                if self.bot_turn_task and not self.bot_turn_task.done():
                    self.bot_turn_task.cancel()
                if self._unmute_task and not self._unmute_task.done():
                    self._unmute_task.cancel()
                self.bot_speaking = False
                self.vad.set_bot_speaking(False)
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
