from __future__ import annotations

import os
import httpx

CARTESIA_BASE_URL = "https://api.cartesia.ai"
FALLBACK_VOICE_ID = "db6b0ed5-d5d3-463d-ae85-518a07d3c2b4"  # Skylar, en-US (documented example voice)

_cached_voice_id = None


def _headers(extra: dict | None = None) -> dict:
    api_key = os.environ["CARTESIA_API_KEY"]
    version = os.environ.get("CARTESIA_VERSION", "2026-03-01")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Cartesia-Version": version,
    }
    if extra:
        headers.update(extra)
    return headers


async def _fetch_default_voice_id() -> str:
    language = os.environ.get("CARTESIA_LANGUAGE", "ru")
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(
            f"{CARTESIA_BASE_URL}/voices",
            headers=_headers(),
            params={"language": language, "limit": 1},
        )
    if response.status_code != 200:
        print(f"[cartesia] Не удалось получить список голосов ({response.status_code}): {response.text}. "
              f"Использую резервный голос {FALLBACK_VOICE_ID}.")
        return FALLBACK_VOICE_ID

    voices = response.json().get("data") or response.json().get("voices") or []
    if not voices:
        print(f"[cartesia] Голоса для языка '{language}' не найдены. "
              f"Использую резервный голос {FALLBACK_VOICE_ID}. "
              f"Задайте CARTESIA_VOICE_ID вручную для лучшего качества.")
        return FALLBACK_VOICE_ID

    voice_id = voices[0]["id"]
    print(f"[cartesia] Автоматически выбран голос '{voices[0].get('name', '?')}' ({voice_id}) для языка '{language}'.")
    return voice_id


async def get_default_voice_id() -> str:
    global _cached_voice_id
    if _cached_voice_id:
        return _cached_voice_id

    env_voice_id = os.environ.get("CARTESIA_VOICE_ID", "").strip()
    if env_voice_id:
        _cached_voice_id = env_voice_id
    else:
        _cached_voice_id = await _fetch_default_voice_id()
    return _cached_voice_id
