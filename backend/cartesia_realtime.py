from __future__ import annotations

import json
import os

import websockets

CARTESIA_WS_BASE = "wss://api.cartesia.ai"
TTS_SAMPLE_RATE = 24000  # must match the sample rate the frontend player expects
STT_SAMPLE_RATE = 16000  # must match the sample rate the frontend mic worklet sends


def _version() -> str:
    return os.environ.get("CARTESIA_VERSION", "2026-03-01")


def _api_key() -> str:
    return os.environ["CARTESIA_API_KEY"]


async def open_stt_ws():
    model = os.environ.get("CARTESIA_STT_MODEL", "ink-whisper")
    language = os.environ.get("CARTESIA_LANGUAGE", "ru")
    url = (
        f"{CARTESIA_WS_BASE}/stt/websocket"
        f"?model={model}&encoding=pcm_s16le&sample_rate={STT_SAMPLE_RATE}"
        f"&cartesia_version={_version()}&language={language}"
    )
    return await websockets.connect(url, additional_headers={"X-API-Key": _api_key()})


async def open_tts_ws():
    url = f"{CARTESIA_WS_BASE}/tts/websocket?cartesia_version={_version()}"
    return await websockets.connect(url, additional_headers={"X-API-Key": _api_key()})


def build_tts_request(transcript: str, context_id: str, keep_open: bool, voice_id: str) -> str:
    model_id = os.environ.get("CARTESIA_TTS_MODEL", "sonic-3.5")
    language = os.environ.get("CARTESIA_LANGUAGE", "ru")
    return json.dumps({
        "model_id": model_id,
        "transcript": transcript,
        "voice": {"mode": "id", "id": voice_id},
        "output_format": {
            "container": "raw",
            "encoding": "pcm_s16le",
            "sample_rate": TTS_SAMPLE_RATE,
        },
        "context_id": context_id,
        "continue": keep_open,
        "language": language,
    })
