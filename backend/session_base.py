from __future__ import annotations

import asyncio
import base64
import json
import re
import uuid
from datetime import datetime, timedelta, timezone

from websockets.exceptions import ConnectionClosed
from fastapi import WebSocket
from starlette.websockets import WebSocketState

import cartesia_realtime
import llm_client
import settings_store
from call_recorder import CallRecorder

SENTENCE_END_RE = re.compile(r"[.!?…]\s*$")
MIN_SENTENCE_CHARS = 30
MAX_TOOL_ROUNDS = 3

# Whisper-family STT (Ink-Whisper included) hallucinates stock filler
# phrases on near-silent trailing audio: the last ~silenceHangoverMs of
# audio before VAD confirms the caller stopped talking still gets forwarded
# to STT (we have to keep listening through it to detect silence at all),
# and that's exactly where this happens. Observed in production transcripts:
# "Продолжение" and "Субтитры" tacked onto otherwise-correct text — classic
# leftovers from the kind of auto-captioned video data these models are
# trained on. Left uncleaned, these words become part of the conversation
# history sent to the LLM on every subsequent turn.
_HALLUCINATION_TAIL_RE = re.compile(
    r"[\s.,!?]*(продолжение(\s+следует)?|субтитр[а-я]*(\s+\S+){0,3}|"
    r"спасибо за просмотр|подписывайтесь(\s+на\s+канал)?)[\s.,!?]*$",
    re.IGNORECASE,
)


def _strip_hallucination_tail(text: str) -> str:
    """Only strips a short TRAILING match — never touches genuine
    mid-sentence content, and never returns an empty string even if that's
    literally all Cartesia gave us (an all-hallucination utterance is
    exceedingly rare and better shown as-is than silently dropped)."""
    cleaned = _HALLUCINATION_TAIL_RE.sub("", text).strip()
    return cleaned if cleaned else text
# The greeting used to be an LLM call (a hidden prompt telling the model to
# introduce itself), but that made it, empirically and repeatedly, the turn
# most prone to the model going off the rails — rambling into unrelated
# content, hallucinated asides, sometimes in English. A fixed opening line
# has no such risk: there's no model call to degenerate. GREETING_SENTINEL
# is an internal-only marker (never sent to the LLM) that tells bot_turn to
# take the fixed-text fast path — see BaseSession._send_fixed_greeting.
GREETING_SENTINEL = object()

MOSCOW_TZ = timezone(timedelta(hours=3))  # fixed UTC+3 — Moscow has had no DST since 2014
_WEEKDAYS_RU = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
_MONTHS_RU = [
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
]


def current_datetime_context() -> str:
    """A few sentences appended to the system prompt at session start (not
    part of the stored/editable prompt itself — see settings_store.py — since
    it must always reflect the real moment the call started, not whatever
    was true whenever the operator last saved the prompt). Hardcoded Russian
    weekday/month names rather than strftime('%A')/('%B'): those are locale-
    dependent and Railway's container has no guarantee of a ru_RU locale.
    Public (no leading underscore): reused by test_chat.py's stateless
    preview turns, which need the exact same "what day is it" grounding."""
    now = datetime.now(MOSCOW_TZ)
    weekday = _WEEKDAYS_RU[now.weekday()]
    month = _MONTHS_RU[now.month - 1]
    return (
        f"\n\nТЕКУЩАЯ ДАТА И ВРЕМЯ: сейчас {weekday}, {now.day} {month} {now.year} года, "
        f"{now.strftime('%H:%M')} по московскому времени. Используй это, чтобы правильно "
        f"понимать слова собеседника вроде «сегодня», «завтра», «в субботу», «через неделю» "
        f"и переводить их в конкретную дату, если это важно для брони или для того, будни "
        f"сейчас или выходные."
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
}}, {"type": "function", "function": {
    "name": "qualify_lead",
    "description": "Вызови в конце звонка, чтобы зафиксировать результат: готов ли собеседник купить, и как прошёл разговор.",
    "parameters": {
        "type": "object",
        "properties": {
            "ready_to_buy": {"type": "string", "enum": ["yes", "no", "unclear"]},
            "summary": {"type": "string", "description": "Краткое саммари разговора"},
        },
        "required": ["ready_to_buy", "summary"],
    },
}}]


def execute_tool(name: str, args: dict) -> dict:
    """Module-level (not a method — never touched self) so test_chat.py's
    stateless preview turns can reuse it without a BaseSession instance."""
    if name == "confirm_booking":
        return {"status": "ok", "message": "Бронь зафиксирована (прототип, реального сохранения в базе нет)."}
    if name == "qualify_lead":
        return {"status": "ok", "message": "Результат звонка зафиксирован."}
    return {"status": "error", "message": f"Неизвестная функция: {name}"}


class BaseSession:
    """LLM streaming + tool-calling + Cartesia TTS logic shared by CallSession
    (voice: adds Cartesia STT and mic audio) and ChatSession (typed input
    instead of STT, same LLM/TTS/logging pipeline otherwise)."""

    def __init__(self, ws: WebSocket):
        self.ws = ws
        # Read once per session (not per turn) — same invariant the plain
        # prompt string used to have, just covering more fields now. A save
        # via /prompt.html takes effect for the *next* call, never mid-call.
        self.settings = settings_store.get_current_settings()
        system_content = (
            f"{self.settings['persona_prompt'].strip()}\n\n{self.settings['business_facts'].strip()}"
            + current_datetime_context()
        )
        self.history: list[dict] = [{"role": "system", "content": system_content}]
        self.reasoning_effort = self.settings["reasoning_effort"]
        self.temperature = self.settings["temperature"]
        self.max_reply_tokens = self.settings["max_reply_tokens"]
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

    async def _emit_tts_audio(self, pcm: bytes) -> None:
        """Send one PCM16@24kHz TTS chunk to whatever's on the other end of
        this session's transport. Default = the browser WebSocket, unchanged
        from before this was a hook."""
        await self.ws.send_bytes(pcm)

    async def _on_tts_generation_done(self, context_id: str) -> None:
        """Called once Cartesia has generated the last audio chunk of a turn.
        No-op for the browser, which learns about real playback completion
        from the client instead ("bot_playback_done"). TelephonySession, which
        has no client to ask, uses this as the signal that the audio still
        queued for the call is now the *whole* remainder of the turn -- see
        its _pace_outbound_audio."""
        return None

    async def _send_tts_text(self, text: str, context_id: str, keep_open: bool) -> None:
        """Send one piece of reply text to Cartesia's realtime TTS websocket
        for synthesis. TelephonySession wraps this (not overrides it outright)
        to also schedule its playback-duration unmute fallback -- see
        telephony_session.py."""
        await self.tts_ws.send(
            cartesia_realtime.build_tts_request(text, context_id, keep_open, self.voice_id)
        )

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
                    await self._emit_tts_audio(pcm)
                elif msg_type == "done":
                    # NB: this only means Cartesia finished *generating* audio for
                    # this turn, not that the client finished *playing* it. Callers
                    # keep bot_speaking True until the client reports actual
                    # playback completion via "bot_playback_done".
                    await self.send_json({"type": "bot_done"})
                    await self._on_tts_generation_done(data["context_id"])
                elif msg_type == "error":
                    self.bot_speaking = False
                    await self.send_json({"type": "error", "text": f"Cartesia TTS: {data.get('message')}"})
        except ConnectionClosed as exc:
            # Returning quietly here ends the whole session on telephony (its
            # run() tears everything down as soon as any pump finishes), so a
            # Cartesia-side close would otherwise look like a normal hangup
            # with nothing in the logs at all.
            await self.send_json({"type": "tts_stream_closed", "text": str(exc)})

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
        # testing while still keeping replies fast. Now operator-tunable via
        # /prompt.html (self.reasoning_effort), "low" stays the shipped
        # default in settings_store.DEFAULT_SETTINGS.
        kwargs = dict(
            model=llm_client.model_name(),
            messages=self.history,
            stream=True,
            max_tokens=self.max_reply_tokens,
            reasoning_effort=self.reasoning_effort,
            tools=TOOLS,
            tool_choice="auto",
        )
        if self.temperature is not None:
            kwargs["temperature"] = self.temperature
        stream = await llm_client.get_client().chat.completions.create(**kwargs)
        async for chunk in stream:
            # Confirmed live: TokenRouter/Gemini sends a trailing chunk with an
            # empty choices list at the end of some streams (a final
            # usage-only chunk is a known quirk of several OpenAI-compatible
            # providers) -- chunk.choices[0] raised IndexError there, which
            # bot_turn's except-block turned into a silent-looking "OpenAI:
            # list index out of range" error and aborted the turn BEFORE the
            # final continue=False piece ever got sent. That's why turns
            # never cleanly closed their Cartesia context on real calls.
            if not chunk.choices:
                continue
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
                        await self._send_tts_text(sentence_buffer, context_id, True)
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

    async def _send_fixed_greeting(self, context_id: str):
        """The call-opening greeting bypasses the LLM entirely — see
        GREETING_SENTINEL above for why. Still goes through the same task/
        cancellation machinery as a normal turn (launch_bot_turn cancels
        whatever's running), so barge-in over the greeting still works.

        Sent as two _send_tts_text calls (open with continue=True, close
        with an empty continue=False), mirroring bot_turn's normal
        streamed-sentence-then-closing-flush shape, rather than one single
        continue=False message — keeps this path consistent with every other
        turn rather than a special case."""
        greeting = self.settings["greeting"]
        self.recorder.open_turn(context_id, None)
        try:
            await self.send_json({"type": "status", "text": "Синтезирую речь…"})
            await self.send_json({"type": "bot_text_delta", "text": greeting})
            await self._send_tts_text(greeting, context_id, True)
            await self._send_tts_text(" ", context_id, False)
        except asyncio.CancelledError:
            self.recorder.close_turn(context_id, greeting, cancelled=True)
            raise
        except Exception as exc:
            self.bot_speaking = False
            await self.send_json({"type": "error", "text": f"TTS: {exc}"})
            self.recorder.close_turn(context_id, greeting, cancelled=True)
            return

        self.history.append({"role": "assistant", "content": greeting})
        self.recorder.close_turn(context_id, greeting, cancelled=False)
        await self.send_json({"type": "bot_text_done"})

    async def bot_turn(self, user_text: str | None):
        self.bot_speaking = True
        context_id = str(uuid.uuid4())
        self.active_context_id = context_id

        if user_text is GREETING_SENTINEL:
            await self._send_fixed_greeting(context_id)
            return

        if user_text:
            self.history.append({"role": "user", "content": user_text})

        await self.send_json({"type": "status", "text": "Обращаюсь к Gemini…"})
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
                        result = execute_tool(tc["name"], args)
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
        await self._send_tts_text(leftover or " ", context_id, False)

        self.history.append({"role": "assistant", "content": full_reply})
        self.recorder.close_turn(context_id, full_reply, cancelled=False)
        await self.send_json({"type": "bot_text_done"})

    async def pump_stt_transcripts(self):
        """Reads Cartesia STT events and drives turns off of them. Only
        touches self.stt_ws/send_json/launch_bot_turn/recorder — nothing
        transport-specific — so every BaseSession subclass shares this
        verbatim rather than each carrying its own copy of hallucination-tail
        stripping that could drift out of sync."""
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
                    text = _strip_hallucination_tail(utterance.strip())
                    utterance = ""
                    if text:
                        await self.send_json({"type": "user_final", "text": text})
                        self.launch_bot_turn(text)
                    # Edge case (accepted, not fixed): a false VAD trigger with
                    # no real speech leaves the speech-end mark from the audio
                    # pump pending unconsumed — it'll attach to whichever turn
                    # opens next instead of being discarded, inflating that
                    # turn's logged latency. Rare in practice now that both
                    # mic-worklet.js and ServerVAD only forward audio during
                    # confirmed speech.
                elif msg_type == "error":
                    await self.send_json({"type": "error", "text": f"Cartesia STT: {data.get('message')}"})
        except ConnectionClosed as exc:
            # Same reasoning as pump_tts_audio's handler above -- on telephony
            # this silently ends the call, so say so rather than vanishing.
            await self.send_json({"type": "stt_stream_closed", "text": str(exc)})
