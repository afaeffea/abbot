import os

from openai import AsyncOpenAI

# Google's own OpenAI-compatible endpoint for the Gemini API -- fixed, not
# per-account like TokenRouter's, so it doesn't need an env var.
GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"

_client = None

# There is a live person holding a phone on the other end of every one of
# these requests, so the SDK's 600-second default timeout is not a timeout at
# all -- it's a promise to keep a billed line open for ten minutes while
# nothing happens. Direct Gemini access confirmed this can genuinely hang
# for 75s+ at 25s x 2 retries during a "high demand" episode, so 8s per
# attempt -- a healthy reply starts streaming in a second or two regardless
# of model version.
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
            api_key=os.environ["GEMINI_API_KEY"],
            base_url=GEMINI_BASE_URL,
            timeout=REQUEST_TIMEOUT_S,
            max_retries=MAX_RETRIES,
        )
    return _client


def model_name() -> str:
    # No "google/" prefix here -- that was TokenRouter's routing convention.
    # Google's own endpoint expects its native model IDs. 3.6, not 3.7: an
    # older, more established release, tried after 3.7 showed frequent
    # "high demand" 503s under direct access. (A separate, still-unexplained
    # "missing thought_signature" tool-calling bug showed up on 3.5-flash,
    # not this one -- still worth re-running the qualify_lead reliability
    # check before trusting 3.6 for a real campaign.)
    return os.environ.get("GEMINI_MODEL", "gemini-3.6-flash")
