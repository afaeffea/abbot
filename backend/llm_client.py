import os

from openai import AsyncOpenAI

# Google's own OpenAI-compatible endpoint for the Gemini API -- fixed, not
# per-account like TokenRouter's, so it doesn't need an env var.
GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"

_client = None

# There is a live person holding a phone on the other end of every one of
# these requests, so the SDK's 600-second default timeout is not a timeout at
# all -- it's a promise to keep a billed line open for ten minutes while
# nothing happens. 25s (the old value) combined with MAX_RETRIES below meant
# up to ~75s+ of dead air, confirmed live during a Gemini "high demand"
# episode where requests hung instead of failing fast. A healthy Gemini
# reply starts streaming in a second or two, so 8s per attempt is still
# generous for normal jitter but caps worst-case silence at a much more
# tolerable ~24s instead.
REQUEST_TIMEOUT_S = 8.0

# Direct Gemini's Tier 1 quota (300 RPM) leaves far more headroom than
# TokenRouter's 10 RPM did, but retries are cheap insurance against any
# transient hiccup -- more than 2 just stacks dead air on a live line.
MAX_RETRIES = 2


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
    # Google's own endpoint expects its native model IDs.
    return os.environ.get("GEMINI_MODEL", "gemini-3.7-flash")
