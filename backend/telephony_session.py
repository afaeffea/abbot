from __future__ import annotations

import asyncio
import base64
import json
import time

from fastapi import WebSocket

import cartesia_client
import cartesia_realtime
import sheets_logger
import telephony_audio
from session_base import BaseSession, GREETING_SENTINEL
from telephony_vad import ServerVAD

# G.711 μ-law is always 8kHz for PSTN — this is a telephony-transport constant,
# not a Cartesia one, so it doesn't belong in cartesia_realtime.py alongside
# STT_SAMPLE_RATE/TTS_SAMPLE_RATE.
TELEPHONY_SAMPLE_RATE = 8000

# Voximplant's own inbound chunks to us are consistently exactly this size
# (confirmed live: "first inbound audio chunk received (160 bytes)" on every
# call) -- standard 20ms G.711 telephony packetization. Our outbound chunks
# were previously whatever arbitrary size Cartesia's TTS produced after
# resampling (never 160-aligned), which may be why raw-binary output was
# silently dropped -- buffering to this fixed frame size is untested but
# well-evidenced from what Voximplant demonstrably does on its own side.
OUTBOUND_FRAME_BYTES = 160


class TelephonySession(BaseSession):
    """Phone call (via Voximplant): μ-law@8kHz audio bridged over a WebSocket
    Voximplant's VoxEngine scenario opens to us <-> BaseSession's Gemini/
    tool-calling/TTS pipeline, same as CallSession but with no browser on the
    other end -- server-side VAD instead of mic-worklet.js, and duration-
    estimate playback tracking instead of the browser's explicit
    "bot_playback_done" message."""

    def __init__(self, ws: WebSocket):
        super().__init__(ws)
        self.stt_ws = None
        self.vad = ServerVAD(self.settings["vad"], sample_rate=cartesia_realtime.STT_SAMPLE_RATE)
        self._turn_audio_start: float | None = None
        self._turn_audio_bytes = 0
        self._unmute_task: asyncio.Task | None = None
        self._inbound_chunks_received = 0
        self._outbound_buffer = bytearray()

    async def run(self):
        self.voice_id = self.settings["voice_id"] or await cartesia_client.get_default_voice_id()
        self.stt_ws = await cartesia_realtime.open_stt_ws()
        self.tts_ws = await cartesia_realtime.open_tts_ws()

        tasks = [
            asyncio.create_task(self.pump_phone_audio()),
            asyncio.create_task(self.pump_stt_transcripts()),
            asyncio.create_task(self.pump_tts_audio()),
        ]
        # Bot speaks first, like answering a phone call -- same fixed,
        # operator-edited greeting as the browser transport (settings_store.py).
        self.launch_bot_turn(GREETING_SENTINEL)

        try:
            # The call ends as soon as any one pump exits (e.g. Voximplant
            # hangs up); gather() would hang forever waiting on the other two
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
        does (see e.g. bot_turn's own error handling, README's "конвейер не
        глотает ошибки"). The one event we also *act* on is "bot_done": it
        only means Cartesia finished *generating* audio for the turn, not
        that Voximplant finished *playing* it into the call. There's no
        client here to report actual playback completion (that's what the
        browser's "bot_playback_done" message is for), so we estimate it from
        how many μ-law bytes we actually sent."""
        print(f"[telephony:{self.recorder.call_id}] {payload}")
        if payload.get("type") == "bot_done":
            await self._flush_outbound_audio()
            self._schedule_unmute()

    def _schedule_unmute(self) -> None:
        if self._unmute_task and not self._unmute_task.done():
            self._unmute_task.cancel()
        self._unmute_task = asyncio.create_task(self._unmute_after_playback())

    async def _unmute_after_playback(self) -> None:
        start = self._turn_audio_start
        total_bytes = self._turn_audio_bytes
        self._turn_audio_start = None
        self._turn_audio_bytes = 0
        if start is not None and total_bytes > 0:
            # μ-law is 1 byte/sample @ 8kHz, so total_bytes/RATE = audio duration.
            # We may have sent all of it well before now (generation is faster
            # than real-time) -- wait out whatever's left since the first byte
            # went out, mirroring the browser's nextStartTime-vs-currentTime
            # scheduling check in frontend/app.js.
            total_duration = total_bytes / TELEPHONY_SAMPLE_RATE
            elapsed = time.monotonic() - start
            remaining = max(0.0, total_duration - elapsed)
            if remaining > 0:
                await asyncio.sleep(remaining)
        self.bot_speaking = False
        self.vad.set_bot_speaking(False)

    async def _emit_tts_audio(self, pcm: bytes) -> None:
        if self._turn_audio_start is None:
            self._turn_audio_start = time.monotonic()
        pcm_8k = telephony_audio.resample_linear(pcm, cartesia_realtime.TTS_SAMPLE_RATE, TELEPHONY_SAMPLE_RATE)
        ulaw = telephony_audio.pcm16_to_ulaw(pcm_8k)
        self._turn_audio_bytes += len(ulaw)
        # Attempt history (all confirmed live against real calls -- see git
        # log): JSON envelopes (bare and matching Voximplant's own outgoing
        # sequenceNumber/chunk/timestamp shape) both confirmed *arriving*
        # (WebSocketEvents.MESSAGE fired for each one) but silent on the call.
        # Plain raw-binary frames (this session's very first implementation)
        # were ALSO already tried and ALSO silent. What's new here: chunking
        # to a fixed OUTBOUND_FRAME_BYTES size matching Voximplant's own
        # packetization, instead of whatever arbitrary size Cartesia's TTS
        # happened to produce -- untested until now.
        self._outbound_buffer.extend(ulaw)
        while len(self._outbound_buffer) >= OUTBOUND_FRAME_BYTES:
            frame = bytes(self._outbound_buffer[:OUTBOUND_FRAME_BYTES])
            del self._outbound_buffer[:OUTBOUND_FRAME_BYTES]
            await self.ws.send_bytes(frame)

    async def _flush_outbound_audio(self) -> None:
        """Send whatever's left in the buffer (< one full frame) at the end
        of a turn, so the tail of a reply is never silently dropped."""
        if self._outbound_buffer:
            await self.ws.send_bytes(bytes(self._outbound_buffer))
            self._outbound_buffer.clear()

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
                event = data.get("event") if isinstance(data, dict) else None
                if event == "media":
                    payload_b64 = (data.get("media") or {}).get("payload")
                    if not payload_b64:
                        continue
                    audio_bytes = base64.b64decode(payload_b64)
                else:
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
                # turn and any pending playback-duration wait, start listening
                # right away. Mirrors CallSession's "interrupt" control message.
                self.active_context_id = None  # any late chunk/done for the old turn is now stale
                if self.bot_turn_task and not self.bot_turn_task.done():
                    self.bot_turn_task.cancel()
                if self._unmute_task and not self._unmute_task.done():
                    self._unmute_task.cancel()
                self.bot_speaking = False
                self.vad.set_bot_speaking(False)
                # Drop any not-yet-flushed partial frame from the cancelled
                # turn -- otherwise it would silently prepend stale audio to
                # the start of the next turn's stream.
                self._outbound_buffer.clear()

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
