from __future__ import annotations

import asyncio
import json

from fastapi import WebSocket

import cartesia_client
import cartesia_realtime
from session_base import BaseSession, GREETING_TRIGGER


class ChatSession(BaseSession):
    """Typed text in, spoken (+ text) reply out: same Gemini/tool-calling/TTS
    pipeline as CallSession, minus Cartesia STT and mic audio."""

    async def run(self):
        self.voice_id = await cartesia_client.get_default_voice_id()
        self.tts_ws = await cartesia_realtime.open_tts_ws()

        await self.send_json({"type": "status", "text": "Соединение установлено"})

        tasks = [
            asyncio.create_task(self.pump_text_input()),
            asyncio.create_task(self.pump_tts_audio()),
        ]
        self.launch_bot_turn(GREETING_TRIGGER)

        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                exc = task.exception()
                if exc:
                    raise exc
        finally:
            for task in tasks:
                task.cancel()
            try:
                await self.tts_ws.close()
            except Exception:
                pass
            try:
                await self.recorder.finalize()
            except Exception:
                pass

    async def pump_text_input(self):
        while True:
            message = await self.ws.receive()
            if message["type"] == "websocket.disconnect":
                return
            text = message.get("text")
            if text is None:
                continue
            try:
                control = json.loads(text)
            except ValueError:
                continue
            control_type = control.get("type")
            if control_type == "user_text" and not self.bot_speaking:
                user_text = (control.get("text") or "").strip()
                if user_text:
                    self.recorder.mark_speech_end()
                    await self.send_json({"type": "user_final", "text": user_text})
                    self.launch_bot_turn(user_text)
            elif control_type == "bot_playback_done":
                # Same reasoning as CallSession: TTS generation finishing isn't the
                # same as the client finishing playback — only unmute typing once
                # the client confirms it's actually done playing the audio.
                self.bot_speaking = False
                await self.send_json({"type": "status", "text": "Жду сообщение…"})
