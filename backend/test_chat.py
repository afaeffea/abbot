from __future__ import annotations

import json

import llm_client
from session_base import MAX_TOOL_ROUNDS, TOOLS, current_datetime_context, execute_tool

# This tool has no auth (matches the rest of the site) — anyone who can
# reach the deployed instance could otherwise use it to run unbounded LLM
# calls at the operator's expense. These caps are cheap insurance, not
# paranoid gatekeeping.
MAX_TEST_HISTORY_TURNS = 40
MAX_TEST_HISTORY_CHARS = 20000
MAX_TEST_MESSAGE_CHARS = 4000

ALLOWED_HISTORY_ROLES = ("user", "assistant")


def validate_history(raw) -> list[dict]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError("history должен быть списком")
    if len(raw) > MAX_TEST_HISTORY_TURNS:
        raise ValueError(f"history не может быть длиннее {MAX_TEST_HISTORY_TURNS} реплик")

    history = []
    total_chars = 0
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("каждая реплика history должна быть объектом")
        role = item.get("role")
        content = item.get("content")
        if role not in ALLOWED_HISTORY_ROLES:
            # Deliberately rejects "system" too — a caller must not be able
            # to smuggle in a fake system message through this field.
            raise ValueError(f"недопустимая роль в history: {role!r}")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("content реплики history должен быть непустой строкой")
        total_chars += len(content)
        if total_chars > MAX_TEST_HISTORY_CHARS:
            raise ValueError(f"history не может быть длиннее {MAX_TEST_HISTORY_CHARS} символов суммарно")
        history.append({"role": role, "content": content})
    return history


def validate_message(raw) -> str:
    text = (raw or "").strip()
    if not text:
        raise ValueError("message не может быть пустым")
    if len(text) > MAX_TEST_MESSAGE_CHARS:
        raise ValueError(f"message не может быть длиннее {MAX_TEST_MESSAGE_CHARS} символов")
    return text


async def run_test_turn(
    *,
    persona_prompt: str,
    business_facts: str,
    reasoning_effort: str,
    temperature: float | None,
    max_reply_tokens: int,
    history: list[dict],
    message: str,
) -> dict:
    """One assistant turn against *draft* (not-yet-saved) settings, with the
    same confirm_booking tool-calling round loop and degenerate-output guard
    as the live pipeline — but no CallRecorder, no TTS, nothing persisted.
    Non-streaming: simpler than replicating the chunked delta-assembly, and
    fine for a text-only admin preview with no TTS to feed incrementally."""
    system_content = (
        f"{persona_prompt.strip()}\n\n{business_facts.strip()}" + current_datetime_context()
    )
    messages = [{"role": "system", "content": system_content}, *history, {"role": "user", "content": message}]

    tool_call_log = []
    reply = ""
    truncated = False
    debug_raw = None

    for _round in range(MAX_TOOL_ROUNDS):
        kwargs = dict(
            model=llm_client.model_name(),
            messages=messages,
            max_tokens=max_reply_tokens,
            reasoning_effort=reasoning_effort,
            tools=TOOLS,
            tool_choice="auto",
        )
        if temperature is not None:
            kwargs["temperature"] = temperature
        try:
            completion = await llm_client.get_client().chat.completions.create(**kwargs)
        except Exception as exc:
            return {
                "reply": reply, "tool_calls": tool_call_log, "truncated": truncated,
                "debug_raw": debug_raw, "debug_error": f"{type(exc).__name__}: {exc}",
                "debug_messages_sent": messages,
            }

        choice = completion.choices[0]
        content = choice.message.content or ""
        tool_calls = choice.message.tool_calls or []

        # Same reliable signal as the live streaming path (see
        # session_base._stream_completion): a literal "{"/"}" never appears
        # in a legitimate reply, since the prompt forbids all formatting.
        brace_positions = [i for i in (content.find("{"), content.find("}")) if i != -1]
        if brace_positions:
            content = content[: min(brace_positions)]
            truncated = True

        if choice.finish_reason == "tool_calls" and tool_calls:
            debug_raw = {
                "tool_call": tool_calls[0].model_dump(),
                "message": choice.message.model_dump(),
            }
            messages.append({
                "role": "assistant",
                "content": content or None,
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                    }
                    for tc in tool_calls
                ],
            })
            for tc in tool_calls:
                try:
                    args = json.loads(tc.function.arguments) if tc.function.arguments else {}
                except ValueError:
                    args = {}
                tool_call_log.append({"name": tc.function.name, "arguments": args})
                result = execute_tool(tc.function.name, args)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": json.dumps(result, ensure_ascii=False),
                })
            continue

        reply = content
        break

    return {"reply": reply, "tool_calls": tool_call_log, "truncated": truncated, "debug_raw": debug_raw}
