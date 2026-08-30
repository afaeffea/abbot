from __future__ import annotations

import os

from openai import AsyncOpenAI

_client = None

# There is a live person holding a phone on the other end of every one of
# these requests, so the SDK's 600-second default timeout is not a timeout at
# all -- it's a promise to keep a billed line open for ten minutes while
# nothing happens. Kept from the Gemini era (confirmed there that failing
# fast at 8s matters); OpenAI's own latency hasn't been measured yet, so
# revisit this number once it has been.
REQUEST_TIMEOUT_S = 8.0

# Retries are NOT cheap on a live phone call: they stack silence the caller
# is listening to. Two of them plus backoff turned one slow reply into ~30
# seconds of dead air, and the caller hung up at ten (confirmed live
# 2026-08-30). One retry keeps the worst case near the point where the
# session gives up and apologises out loud, which is a far better outcome
# than more waiting. This bound matters regardless of which provider or
# model is configured -- any of them can stall.
MAX_RETRIES = 1


def get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(
            api_key=os.environ["OPENAI_API_KEY"],
            timeout=REQUEST_TIMEOUT_S,
            max_retries=MAX_RETRIES,
        )  # no base_url override -- the SDK's own default (api.openai.com/v1) is OpenAI itself
    return _client


def model_name() -> str:
    # Not terra: a 12-trial live comparison (2026-08-30) scored luna 12/12
    # on reliably calling qualify_lead vs terra's 7/12 on the identical
    # scenario -- the earlier pick of terra was based on a 6-trial sample
    # that didn't hold up. luna is also ~10x cheaper.
    return os.environ.get("LLM_MODEL", "gpt-5.6-luna")


_GPT_5_6_FAMILY_PREFIX = "gpt-5.6-"


def completion_kwargs(*, reply_tokens: int, reasoning_effort: str | None, tools_present: bool) -> dict:
    """Encapsulates two quirks of the gpt-5.6 family (luna/terra/sol),
    confirmed live against the real API, not from docs:
    1. max_tokens is rejected outright -- needs max_completion_tokens.
    2. With tools present, reasoning_effort must be exactly "none" (any
       other value, including omitting it, gets a 400) -- and this project
       always passes tools (every call site offers at least one).
    Centralized here rather than duplicated at each call site: the model/
    provider has changed four times this session, and each call site having
    to know these quirks itself is exactly the kind of hardcoding that broke
    last time a switch happened."""
    model = model_name()
    kwargs: dict = {}
    if model.startswith(_GPT_5_6_FAMILY_PREFIX):
        kwargs["max_completion_tokens"] = reply_tokens
        if tools_present:
            kwargs["reasoning_effort"] = "none"
        elif reasoning_effort is not None:
            kwargs["reasoning_effort"] = reasoning_effort
    else:
        kwargs["max_tokens"] = reply_tokens
        if reasoning_effort is not None:
            kwargs["reasoning_effort"] = reasoning_effort
    return kwargs
