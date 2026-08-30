import os

from openai import AsyncOpenAI

_client = None

# There is a live person holding a phone on the other end of every one of
# these requests, so the SDK's 600-second default timeout is not a timeout at
# all -- it's a promise to keep a billed line open for ten minutes while
# nothing happens. Direct Gemini access confirmed this can genuinely hang
# for 75s+ at 25s x 2 retries during a "high demand" episode, so 8s per
# attempt stays even after moving back to TokenRouter -- a healthy reply
# starts streaming in a second or two regardless of which side of the
# router actually serves it.
REQUEST_TIMEOUT_S = 8.0

# TokenRouter caps at 10 requests/minute, and one call spends 3-5 of them, so
# concurrent calls WILL collide. Retrying twice with backoff rides out a
# brief collision; more than that just stacks dead air on the line.
MAX_RETRIES = 2


def get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(
            api_key=os.environ["TOKENROUTER_API_KEY"],
            base_url=os.environ["TOKENROUTER_BASE_URL"],
            timeout=REQUEST_TIMEOUT_S,
            max_retries=MAX_RETRIES,
        )
    return _client


def model_name() -> str:
    return os.environ.get("GEMINI_MODEL", "google/gemini-3.7-flash")
