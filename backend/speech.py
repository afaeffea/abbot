"""Provider seam for speech recognition and synthesis.

The rest of the codebase talks to STT/TTS through the two `open_*` functions
and the small event vocabulary below, so swapping providers is an env var
rather than an edit to every call site. That vocabulary is deliberately the
one Cartesia already spoke, because it fit; the Yandex adapters translate
into it rather than the other way round.

STT events:  {"type": "transcript", "text": str, "is_final": bool}
             {"type": "flush_done"}                 -- utterance is complete
             {"type": "error", "message": str}
TTS events:  {"type": "chunk", "context_id": str, "pcm": bytes}
             {"type": "done", "context_id": str}    -- generation finished
             {"type": "error", "message": str}

Note "pcm" carries raw PCM16LE bytes, not base64 -- Cartesia sends base64 and
its adapter decodes once here, so no consumer has to know that.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os

import cartesia_realtime

# Both providers are fed 16kHz mono PCM16, matching what the browser's mic
# worklet already produces and what ServerVAD is configured for. Yandex would
# happily take the telephony leg's native 8kHz and skip a resample, but the
# VAD, the recorder and the frontend all assume 16k, so that stays a separate
# change rather than a silent side effect of switching providers.
STT_SAMPLE_RATE = cartesia_realtime.STT_SAMPLE_RATE
TTS_SAMPLE_RATE = cartesia_realtime.TTS_SAMPLE_RATE


def stt_provider() -> str:
    return os.environ.get("STT_PROVIDER", "cartesia").strip().lower()


def tts_provider() -> str:
    return os.environ.get("TTS_PROVIDER", "cartesia").strip().lower()


async def open_stt():
    provider = stt_provider()
    if provider == "yandex":
        import yandex_speech
        return await yandex_speech.open_stt()
    if provider != "cartesia":
        raise RuntimeError(f"Неизвестный STT_PROVIDER: {provider!r} (ожидается cartesia или yandex)")
    return CartesiaSttStream(await cartesia_realtime.open_stt_ws())


async def open_tts():
    provider = tts_provider()
    if provider == "yandex":
        import yandex_speech
        return await yandex_speech.open_tts()
    if provider != "cartesia":
        raise RuntimeError(f"Неизвестный TTS_PROVIDER: {provider!r} (ожидается cartesia или yandex)")
    return CartesiaTtsStream(await cartesia_realtime.open_tts_ws())


async def resolve_voice_id(configured: str | None) -> str | None:
    """The voice the current TTS provider should use.

    Provider-aware because the settings page stores a Cartesia voice UUID:
    it means nothing to Yandex, whose voices are names set in
    YANDEX_TTS_VOICE. Asking Cartesia for a default voice while Yandex is
    doing the synthesis would be a REST call to an unused vendor on every
    single call -- and a hard failure once its key is gone.
    """
    if tts_provider() != "cartesia":
        return None
    if configured:
        return configured
    import cartesia_client
    return await cartesia_client.get_default_voice_id()


class CartesiaSttStream:
    """Cartesia's realtime STT websocket behind the common interface."""

    def __init__(self, ws):
        self._ws = ws

    async def send_audio(self, pcm: bytes) -> None:
        await self._ws.send(pcm)

    async def end_utterance(self) -> None:
        # Our own VAD decides where an utterance ends, so Cartesia's
        # server-side finalization is driven explicitly rather than waited on.
        await self._ws.send("finalize")

    async def close(self) -> None:
        await self._ws.close()

    async def __aiter__(self):
        async for raw in self._ws:
            data = json.loads(raw)
            kind = data.get("type")
            if kind == "transcript":
                yield {"type": "transcript",
                       "text": data.get("text", ""),
                       "is_final": bool(data.get("is_final"))}
            elif kind == "flush_done":
                yield {"type": "flush_done"}
            elif kind == "error":
                yield {"type": "error", "message": data.get("message", "")}


class CartesiaTtsStream:
    """Cartesia's realtime TTS websocket behind the common interface."""

    def __init__(self, ws):
        self._ws = ws

    async def synthesize(self, text: str, context_id: str, keep_open: bool, voice_id: str) -> None:
        await self._ws.send(cartesia_realtime.build_tts_request(text, context_id, keep_open, voice_id))

    async def close(self) -> None:
        await self._ws.close()

    async def __aiter__(self):
        async for raw in self._ws:
            data = json.loads(raw)
            kind = data.get("type")
            if kind == "chunk":
                yield {"type": "chunk",
                       "context_id": data.get("context_id"),
                       "pcm": base64.b64decode(data["data"])}
            elif kind == "done":
                yield {"type": "done", "context_id": data.get("context_id")}
            elif kind == "error":
                yield {"type": "error", "message": data.get("message", "")}
