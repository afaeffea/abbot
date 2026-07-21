from __future__ import annotations

import asyncio
import base64
import json
import re
import uuid

from websockets.exceptions import ConnectionClosed
from fastapi import WebSocket
from starlette.websockets import WebSocketState

import cartesia_realtime
import llm_client
import prompt_store
from call_recorder import CallRecorder

SENTENCE_END_RE = re.compile(r"[.!?…]\s*$")
MIN_SENTENCE_CHARS = 30
MAX_REPLY_TOKENS = 400
MAX_TOOL_ROUNDS = 3
GREETING_TRIGGER = (
    "[Разговор только что начался, это самое первое сообщение. Поздоровайся, "
    "представься и спроси, чем помочь — не дожидайся вопроса.]"
)

TOOLS = [{"type": "function", "function": {
    "name": "confirm_booking",
    "description": "Вызови, когда собеседник подтвердил все детали брони, непосредственно перед словесным подтверждением.",
    "parameters": {
        "type": "object",
        "properties": {
            "service": {"type": "string", "description": "Название услуги/тарифа"},
            "date": {"type": "string", "description": "Дата посещения"},
            "time": {"type": "string", "description": "Время посещения"},
            "people_count": {"type": "integer", "description": "Количество человек"},
        },
        "required": ["service", "date", "time", "people_count"],
    },
}}]


class BaseSession:
    """LLM streaming + tool-calling + Cartesia TTS logic shared by CallSession
    (voice: adds Cartesia STT and mic audio) and ChatSession (typed input
    instead of STT, same LLM/TTS/logging pipeline otherwise)."""

    def __init__(self, ws: WebSocket):
        self.ws = ws
        self.history: list[dict] = [{"role": "system", "content": prompt_store.get_current_prompt()}]
        self.bot_speaking = False
        self.tts_ws = None
        self.voice_id = None
        self.turn_lock = asyncio.Lock()
        self.bot_turn_task: asyncio.Task | None = None
        self.active_context_id: str | None = None
        self.recorder = CallRecorder()

    async def send_json(self, payload: dict):
        if self.ws.client_state == WebSocketState.CONNECTED:
            await self.ws.send_json(payload)

    async def _guarded_bot_turn(self, user_text: str | None):
        async with self.turn_lock:
            await self.bot_turn(user_text)

    def launch_bot_turn(self, user_text: str | None):
        if self.bot_turn_task and not self.bot_turn_task.done():
            self.bot_turn_task.cancel()
        self.bot_turn_task = asyncio.create_task(self._guarded_bot_turn(user_text))

    async def pump_tts_audio(self):
        try:
            async for raw in self.tts_ws:
                data = json.loads(raw)
                msg_type = data.get("type")
                if msg_type in ("chunk", "done") and data.get("context_id") != self.active_context_id:
                    continue  # stale message from a turn that was since interrupted
                if msg_type == "chunk":
                    pcm = base64.b64decode(data["data"])
                    self.recorder.mark_first_tts_audio(data["context_id"])
                    self.recorder.record_tts_chunk(pcm)
                    await self.ws.send_bytes(pcm)
                elif msg_type == "done":
                    # NB: this only means Cartesia finished *generating* audio for
                    # this turn, not that the client finished *playing* it. Callers
                    # keep bot_speaking True until the client reports actual
                    # playback completion via "bot_playback_done".
                    await self.send_json({"type": "bot_done"})
                elif msg_type == "error":
                    self.bot_speaking = False
                    await self.send_json({"type": "error", "text": f"Cartesia TTS: {data.get('message')}"})
        except ConnectionClosed:
            pass

    def _execute_tool(self, name: str, args: dict) -> dict:
        if name == "confirm_booking":
            return {"status": "ok", "message": "Бронь зафиксирована (прототип, реального сохранения в базе нет)."}
        return {"status": "error", "message": f"Неизвестная функция: {name}"}

    async def _stream_completion(self, context_id: str):
        """One completions call streamed to the client + Cartesia TTS. Returns
        (full_reply, sentence_buffer_leftover, tool_calls, finish_reason, tts_started)."""
        full_reply = ""
        sentence_buffer = ""
        tts_started = False
        tool_calls: dict[int, dict] = {}
        finish_reason = "stop"

        # reasoning_effort: this Gemini route otherwise spends most of
        # max_tokens on hidden reasoning tokens, truncating the actual spoken
        # answer well before it finishes. "minimal" avoided that but, tested
        # empirically against this route, measurably increased how often the
        # model degenerates mid-reply (raw tool-call-like text or JSON
        # fragments leaking into spoken content, off-topic gibberish,
        # repetition loops) — "low" was clearly more stable in the same
        # testing while still keeping replies fast.
        stream = await llm_client.get_client().chat.completions.create(
            model=llm_client.model_name(),
            messages=self.history,
            stream=True,
            max_tokens=MAX_REPLY_TOKENS,
            reasoning_effort="low",
            tools=TOOLS,
            tool_choice="auto",
        )
        async for chunk in stream:
            choice = chunk.choices[0]
            if choice.finish_reason:
                finish_reason = choice.finish_reason
            delta = choice.delta

            degenerate = False
            if delta.content:
                piece = delta.content
                # A literal "{" or "}" never appears in a legitimate reply —
                # the prompt forbids markdown/formatting entirely, so this is
                # a reliable (zero false-positive, observed in production)
                # signal the model has started emitting raw tool-call/JSON
                # syntax as spoken text instead of a real tool call. Cut the
                # reply there rather than speaking/displaying garbage to a
                # real caller.
                brace_positions = [i for i in (piece.find("{"), piece.find("}")) if i != -1]
                if brace_positions:
                    piece = piece[: min(brace_positions)]
                    degenerate = True

                if piece:
                    if not full_reply:
                        self.recorder.mark_first_llm_token(context_id)
                    full_reply += piece
                    sentence_buffer += piece
                    await self.send_json({"type": "bot_text_delta", "text": piece})

                    if len(sentence_buffer) >= MIN_SENTENCE_CHARS and SENTENCE_END_RE.search(sentence_buffer):
                        if not tts_started:
                            await self.send_json({"type": "status", "text": "Синтезирую речь…"})
                            tts_started = True
                        await self.tts_ws.send(
                            cartesia_realtime.build_tts_request(sentence_buffer, context_id, True, self.voice_id)
                        )
                        sentence_buffer = ""

            if delta.tool_calls:
                for tc in delta.tool_calls:
                    entry = tool_calls.setdefault(tc.index, {"id": None, "name": "", "arguments": ""})
                    if tc.id:
                        entry["id"] = tc.id
                    if tc.function:
                        if tc.function.name:
                            entry["name"] += tc.function.name
                        if tc.function.arguments:
                            entry["arguments"] += tc.function.arguments

            if degenerate:
                finish_reason = "stop"
                break

        return full_reply, sentence_buffer, list(tool_calls.values()), finish_reason, tts_started

    async def bot_turn(self, user_text: str | None):
        self.bot_speaking = True
        if user_text:
            self.history.append({"role": "user", "content": user_text})

        await self.send_json({"type": "status", "text": "Обращаюсь к Gemini…"})

        context_id = str(uuid.uuid4())
        self.active_context_id = context_id
        self.recorder.open_turn(context_id, user_text)

        full_reply = ""
        leftover = ""
        tts_started = False

        try:
            for _round in range(MAX_TOOL_ROUNDS):
                full_reply, leftover, tool_calls, finish_reason, round_tts_started = (
                    await self._stream_completion(context_id)
                )
                tts_started = tts_started or round_tts_started

                if finish_reason == "tool_calls" and tool_calls:
                    self.history.append({
                        "role": "assistant",
                        "content": full_reply or None,
                        "tool_calls": [
                            {
                                "id": tc["id"],
                                "type": "function",
                                "function": {"name": tc["name"], "arguments": tc["arguments"]},
                            }
                            for tc in tool_calls
                        ],
                    })
                    for tc in tool_calls:
                        try:
                            args = json.loads(tc["arguments"]) if tc["arguments"] else {}
                        except ValueError:
                            args = {}
                        self.recorder.record_tool_call(context_id, tc["name"], args)
                        result = self._execute_tool(tc["name"], args)
                        self.history.append({
                            "role": "tool",
                            "tool_call_id": tc["id"],
                            "content": json.dumps(result, ensure_ascii=False),
                        })
                    continue

                break
        except asyncio.CancelledError:
            self.recorder.close_turn(context_id, full_reply, cancelled=True)
            raise
        except Exception as exc:
            self.bot_speaking = False
            await self.send_json({"type": "error", "text": f"OpenAI: {exc}"})
            self.recorder.close_turn(context_id, full_reply, cancelled=True)
            return

        if not tts_started:
            await self.send_json({"type": "status", "text": "Синтезирую речь…"})
        await self.tts_ws.send(
            cartesia_realtime.build_tts_request(leftover or " ", context_id, False, self.voice_id)
        )

        self.history.append({"role": "assistant", "content": full_reply})
        self.recorder.close_turn(context_id, full_reply, cancelled=False)
        await self.send_json({"type": "bot_text_done"})
