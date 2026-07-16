from __future__ import annotations

import asyncio
import base64
import json
import re
import uuid

from websockets.exceptions import ConnectionClosed
from fastapi import WebSocket
from starlette.websockets import WebSocketState

import cartesia_client
import cartesia_realtime
import llm_client
from prompt import SYSTEM_PROMPT

SENTENCE_END_RE = re.compile(r"[.!?…]\s*$")
MIN_SENTENCE_CHARS = 30
MAX_REPLY_TOKENS = 300
GREETING_TRIGGER = (
    "[Звонок только что начался, это самое первое сообщение. Поздоровайся, "
    "представься и спроси, чем помочь — не дожидайся вопроса.]"
)


class CallSession:
    def __init__(self, ws: WebSocket):
        self.ws = ws
        self.history: list[dict] = [{"role": "system", "content": SYSTEM_PROMPT}]
        self.bot_speaking = False
        self.stt_ws = None
        self.tts_ws = None
        self.voice_id = None
        self.turn_lock = asyncio.Lock()
        self.bot_turn_task: asyncio.Task | None = None
        self.active_context_id: str | None = None

    async def send_json(self, payload: dict):
        if self.ws.client_state == WebSocketState.CONNECTED:
            await self.ws.send_json(payload)

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

    async def _guarded_bot_turn(self, user_text: str | None):
        async with self.turn_lock:
            await self.bot_turn(user_text)

    def launch_bot_turn(self, user_text: str | None):
        if self.bot_turn_task and not self.bot_turn_task.done():
            self.bot_turn_task.cancel()
        self.bot_turn_task = asyncio.create_task(self._guarded_bot_turn(user_text))

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
                        await self.send_json({"type": "user_final", "text": text})
                        self.launch_bot_turn(text)
                elif msg_type == "error":
                    await self.send_json({"type": "error", "text": f"Cartesia STT: {data.get('message')}"})
        except ConnectionClosed:
            pass

    async def pump_tts_audio(self):
        try:
            async for raw in self.tts_ws:
                data = json.loads(raw)
                msg_type = data.get("type")
                if msg_type in ("chunk", "done") and data.get("context_id") != self.active_context_id:
                    continue  # stale message from a turn that was since interrupted
                if msg_type == "chunk":
                    await self.ws.send_bytes(base64.b64decode(data["data"]))
                elif msg_type == "done":
                    # NB: this only means Cartesia finished *generating* audio for
                    # this turn, not that the browser finished *playing* it. The
                    # mic stays muted (bot_speaking stays True) until the client
                    # reports actual playback completion via "bot_playback_done".
                    await self.send_json({"type": "bot_done"})
                elif msg_type == "error":
                    self.bot_speaking = False
                    await self.send_json({"type": "error", "text": f"Cartesia TTS: {data.get('message')}"})
        except ConnectionClosed:
            pass

    async def bot_turn(self, user_text: str | None):
        self.bot_speaking = True
        if user_text:
            self.history.append({"role": "user", "content": user_text})

        await self.send_json({"type": "status", "text": "Обращаюсь к Gemini…"})

        context_id = str(uuid.uuid4())
        self.active_context_id = context_id
        full_reply = ""
        sentence_buffer = ""
        tts_started = False

        try:
            stream = await llm_client.get_client().chat.completions.create(
                model=llm_client.model_name(),
                messages=self.history,
                stream=True,
                max_tokens=MAX_REPLY_TOKENS,
            )
            async for chunk in stream:
                delta = chunk.choices[0].delta.content
                if not delta:
                    continue
                full_reply += delta
                sentence_buffer += delta
                await self.send_json({"type": "bot_text_delta", "text": delta})

                if len(sentence_buffer) >= MIN_SENTENCE_CHARS and SENTENCE_END_RE.search(sentence_buffer):
                    if not tts_started:
                        await self.send_json({"type": "status", "text": "Синтезирую речь…"})
                        tts_started = True
                    await self.tts_ws.send(
                        cartesia_realtime.build_tts_request(sentence_buffer, context_id, True, self.voice_id)
                    )
                    sentence_buffer = ""
        except Exception as exc:
            self.bot_speaking = False
            await self.send_json({"type": "error", "text": f"OpenAI: {exc}"})
            return

        if not tts_started:
            await self.send_json({"type": "status", "text": "Синтезирую речь…"})
        await self.tts_ws.send(
            cartesia_realtime.build_tts_request(sentence_buffer or " ", context_id, False, self.voice_id)
        )

        self.history.append({"role": "assistant", "content": full_reply})
        await self.send_json({"type": "bot_text_done"})
