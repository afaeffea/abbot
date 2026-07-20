from __future__ import annotations

import asyncio
import json

from websockets.exceptions import ConnectionClosed
from fastapi import WebSocket

import cartesia_client
import cartesia_realtime
from session_base import BaseSession, GREETING_TRIGGER


class CallSession(BaseSession):
    """Voice call: Cartesia STT (mic audio) -> BaseSession's Gemini/tool-calling
    -> Cartesia TTS, all over one WebSocket."""

    def __init__(self, ws: WebSocket):
        super().__init__(ws)
        self.stt_ws = None

    async def run(self):
        self.voice_id = await cartesia_client.get_default_voice_id()
        self.stt_ws = await cartesia_realtime.open_stt_ws()
        self.tts_ws = await cartesia_realtime.open_tts_ws()

        await self.send_json({"type": "status", "text": "Соединение установлено"})

        tasks = [
            asyncio.create_task(self.pump_mic()),
            asyncio.create_task(self.pump_stt_transcripts()),
            asyncio.create_task(self.pump_tts_audio()),
        ]
        # Bot speaks first, like answering a phone call. The trigger is a hidden
        # instruction, not real user speech, so it's appended to history but
        # never shown in the transcript (a plain system-only prompt with no
        # user turn was observed to make the model ramble off-topic).
        self.launch_bot_turn(GREETING_TRIGGER)

        try:
            # The call ends as soon as any one pump exits (e.g. the client disconnects);
            # gather() would hang forever waiting on the other two open connections.
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                exc = task.exception()
                if exc:
                    raise exc
        finally:
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

    async def pump_mic(self):
        while True:
            message = await self.ws.receive()
            if message["type"] == "websocket.disconnect":
                return
            audio_bytes = message.get("bytes")
            text = message.get("text")
            if audio_bytes is not None:
                if not self.bot_speaking:
                    await self.stt_ws.send(audio_bytes)
                    self.recorder.record_mic_chunk(audio_bytes)
            elif text is not None:
                try:
                    control = json.loads(text)
                except ValueError:
                    continue
                control_type = control.get("type")
                if control_type == "end_of_utterance" and not self.bot_speaking:
                    await self.stt_ws.send("finalize")
                elif control_type == "bot_playback_done":
                    # The client only sends this once it has actually finished
                    # playing the queued audio (TTS generation can finish well
                    # before playback does) — unmuting any earlier caused the
                    # mic to pick up the bot's own trailing audio and re-trigger.
                    self.bot_speaking = False
                    await self.send_json({"type": "status", "text": "Слушаю вас…"})
                elif control_type == "interrupt":
                    # The client detected the caller starting to talk over the
                    # bot and already stopped local playback; cancel the
                    # in-flight turn server-side and start listening right away.
                    if self.bot_speaking:
                        self.active_context_id = None  # any late chunk/done for the old turn is now stale
                        if self.bot_turn_task and not self.bot_turn_task.done():
                            self.bot_turn_task.cancel()
                        self.bot_speaking = False
                        await self.send_json({"type": "status", "text": "Вы прервали бота, слушаю вас…"})

    async def pump_stt_transcripts(self):
        utterance = ""
        try:
            async for raw in self.stt_ws:
                data = json.loads(raw)
                msg_type = data.get("type")
                if msg_type == "transcript":
                    if data.get("is_final"):
                        utterance += data.get("text", "")
                    else:
                        preview = (utterance + data.get("text", "")).strip()
                        if preview:
                            await self.send_json({"type": "user_partial", "text": preview})
                elif msg_type == "flush_done":
                    text = utterance.strip()
                    utterance = ""
                    if text:
                        self.recorder.mark_speech_end()
                        await self.send_json({"type": "user_final", "text": text})
                        self.launch_bot_turn(text)
                elif msg_type == "error":
                    await self.send_json({"type": "error", "text": f"Cartesia STT: {data.get('message')}"})
        except ConnectionClosed:
            pass
