import os

from openai import AsyncOpenAI

_client = None


def get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(
            api_key=os.environ["NORDROUTER_API_KEY"],
            base_url=os.environ["NORDROUTER_BASE_URL"],
        )
    return _client


def model_name() -> str:
    return os.environ.get("GEMINI_MODEL", "google/gemini-3.5-flash:default")
