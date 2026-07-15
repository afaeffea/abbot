import os

from openai import AsyncOpenAI

_client = None


def get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(api_key=os.environ["OPENAI_API_KEY"])
    return _client


def model_name() -> str:
    return os.environ.get("OPENAI_MODEL", "gpt-5.5")
