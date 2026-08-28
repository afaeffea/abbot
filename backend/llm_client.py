import os

from openai import AsyncOpenAI

_client = None

# There is a live person holding a phone on the other end of every one of
# these requests, so the SDK's 600-second default timeout is not a timeout at
# all -- it's a promise to keep a billed line open for ten minutes while
# nothing happens. Some models behind this router really do hang that long
# (measured: qwen3.6-flash and grok-4.1-fast both sat on a single reply for
# minutes). Past ~25s the call is lost anyway; failing fast at least lets the
# session apologise and move on.
REQUEST_TIMEOUT_S = 25.0

# The router allows 10 requests/minute, and one call spends 3-5 of them, so
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
