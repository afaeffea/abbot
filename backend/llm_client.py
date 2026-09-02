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
    # Chosen on a 24-dialogue live comparison against gpt-5.6-luna
    # (2026-09-02), replaying whole scripted calls to their natural end rather
    # than demanding a verdict on one turn -- the earlier single-turn version
    # of that benchmark measured impatience, not reliability, because the
    # persona explicitly forbids calling qualify_lead while questions remain.
    #
    #                        verdict correct   silently never called
    #   gpt-4.1-mini              23/24              1/24
    #   gpt-5.6-luna              12/24             12/24
    #
    # luna dropping half of all calls with no verdict is the exact failure
    # FORCE_QUALIFY_AFTER_TURNS exists to paper over. 4.1-mini is also ~2x
    # faster to first token (0.9s vs 1.7s measured), which on a phone call is
    # dead air the caller sits through. Neither model ever cut off an
    # interested buyer (0/8 each on the agreement scenario).
    return os.environ.get("LLM_MODEL", "gpt-4.1-mini")


_REASONING_MODEL_PREFIX = "gpt-5"
_GPT_5_6_FAMILY_PREFIX = "gpt-5.6-"


def completion_kwargs(*, reply_tokens: int, reasoning_effort: str | None) -> dict:
    """Encapsulates the per-model API differences, each confirmed by probing
    the real API rather than read from docs:

    * max_completion_tokens is accepted by every model in play, while
      max_tokens is rejected outright by the gpt-5 family -- so there is no
      reason to ever send max_tokens.
    * reasoning_effort exists only on the gpt-5 family. gpt-4.1-* reject it
      as an unrecognized argument whatever its value, so it has to be omitted
      entirely, not merely set low.
    * gpt-5.6 (luna/terra/sol) accepts exactly one value that is legal both
      with and without tools: "none". Tools present and any other value is a
      400, and so is omitting it; without tools it additionally rejects
      "minimal", which is the UI's own default. One safe value, so it is
      pinned rather than branched on.

    Centralized here rather than duplicated at each call site: the model has
    changed five times now, and each call site knowing these quirks itself is
    exactly the hardcoding that broke the last switch."""
    model = model_name()
    kwargs: dict = {"max_completion_tokens": reply_tokens}
    if not model.startswith(_REASONING_MODEL_PREFIX):
        return kwargs
    if model.startswith(_GPT_5_6_FAMILY_PREFIX):
        kwargs["reasoning_effort"] = "none"
    elif reasoning_effort is not None:
        kwargs["reasoning_effort"] = reasoning_effort
    return kwargs
