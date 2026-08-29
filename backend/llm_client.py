import os

from openai import AsyncOpenAI

# Google's own OpenAI-compatible endpoint for the Gemini API -- fixed, not
# per-account like TokenRouter's, so it doesn't need an env var.
GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"

_client = None


def get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(
            api_key=os.environ["GEMINI_API_KEY"],
            base_url=GEMINI_BASE_URL,
        )
    return _client


def model_name() -> str:
    # No "google/" prefix here -- that was TokenRouter's routing convention.
    # Google's own endpoint expects its native model IDs.
    return os.environ.get("GEMINI_MODEL", "gemini-3.7-flash")
