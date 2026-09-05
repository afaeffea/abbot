"""Yandex SpeechKit v3 behind the same interface as Cartesia (see speech.py).

Two things about SpeechKit shaped this file:

1. It is gRPC, not WebSocket, so there is no single socket object to iterate.
   Both adapters expose an asyncio.Queue of the common events instead, filled
   by a background task, so consumers still just `async for`.

2. Its streaming synthesis has no equivalent of Cartesia's context_id, and no
   "generation finished" message -- the server simply stops sending after a
   ForceSynthesisEvent. So one gRPC call is opened per bot turn, and the call
   ending IS the done signal. The context_id the rest of the codebase uses for
   barge-in filtering is tracked here and stamped onto outgoing events.

Auth is a service-account API key (`Api-Key <key>`), not an IAM token, so
there is no token refresh to run.

UNVERIFIED against the live service -- written from the protobuf definitions
and message construction tested offline. The first real connection is where
the endpoint names, metadata shape and the 24kHz output rate get confirmed.
"""
from __future__ import annotations

import asyncio
import os

import grpc
from yandex.cloud.ai.stt.v3 import stt_pb2, stt_service_pb2_grpc
from yandex.cloud.ai.tts.v3 import tts_pb2, tts_service_pb2_grpc

import speech

STT_ENDPOINT = "stt.api.cloud.yandex.net:443"
TTS_ENDPOINT = "tts.api.cloud.yandex.net:443"


def _api_key() -> str:
    return os.environ["YANDEX_API_KEY"]


def _metadata() -> list:
    # x-folder-id is redundant when the key belongs to a service account (the
    # folder comes with it), but harmless and required if a user API key is
    # ever used instead.
    meta = [("authorization", f"Api-Key {_api_key()}")]
    folder = os.environ.get("YANDEX_FOLDER_ID")
    if folder:
        meta.append(("x-folder-id", folder))
    return meta


_channels: dict = {}


def _channel(endpoint: str):
    """One channel per endpoint per process, shared by every concurrent call.

    gRPC channels are multiplexed and safe to share, and the TLS handshake is
    the single biggest cost in this path: measured, opening the synthesis
    channel per session cost 1.0s, and it lands on the greeting because the
    channel connects lazily on first use, not at open. Per-session channels
    would also mean a campaign at concurrency 10 paying that handshake twenty
    times over. Reconnection after an idle drop is gRPC's own business.

    Consequence: the stream objects must NOT close these. They don't.
    """
    channel = _channels.get(endpoint)
    if channel is None:
        channel = grpc.aio.secure_channel(endpoint, grpc.ssl_channel_credentials())
        _channels[endpoint] = channel
    return channel


async def warm_up() -> None:
    """Connect both channels ahead of the first call so its greeting doesn't
    pay for the handshake. Failures are ignored on purpose -- this is an
    optimization, and a real problem should surface on the call itself with a
    useful error rather than as a crash at startup."""
    for endpoint in (STT_ENDPOINT, TTS_ENDPOINT):
        try:
            await asyncio.wait_for(_channel(endpoint).channel_ready(), timeout=10)
        except Exception as exc:
            print(f"[yandex] прогрев канала {endpoint} не удался (не фатально): "
                  f"{type(exc).__name__}: {exc}")


def build_stt_options() -> stt_pb2.StreamingRequest:
    """First message of a recognition stream.

    Text normalization is left OFF deliberately: it only rewrites numbers and
    the like in the transcript we hand the LLM, which changes nothing for us,
    and the docs warn it adds latency to every final result.

    The EOU classifier is external because our own ServerVAD already decides
    where an utterance ends -- letting SpeechKit decide too would mean two
    detectors disagreeing about the same silence.
    """
    return stt_pb2.StreamingRequest(session_options=stt_pb2.StreamingOptions(
        recognition_model=stt_pb2.RecognitionModelOptions(
            model=os.environ.get("YANDEX_STT_MODEL", "general"),
            audio_format=stt_pb2.AudioFormatOptions(raw_audio=stt_pb2.RawAudio(
                audio_encoding=stt_pb2.RawAudio.LINEAR16_PCM,
                sample_rate_hertz=speech.STT_SAMPLE_RATE,
                audio_channel_count=1)),
            text_normalization=stt_pb2.TextNormalizationOptions(
                text_normalization=stt_pb2.TextNormalizationOptions.TEXT_NORMALIZATION_DISABLED,
                profanity_filter=False,
                literature_text=False),
            language_restriction=stt_pb2.LanguageRestrictionOptions(
                restriction_type=stt_pb2.LanguageRestrictionOptions.WHITELIST,
                language_code=[os.environ.get("YANDEX_STT_LANGUAGE", "ru-RU")]),
            audio_processing_type=stt_pb2.RecognitionModelOptions.REAL_TIME),
        eou_classifier=stt_pb2.EouClassifierOptions(
            external_classifier=stt_pb2.ExternalEouClassifier())))


def _alternative_text(update) -> str:
    """First alternative only, NOT all of them joined.

    `alternatives` are competing N-best hypotheses for the same audio, not
    consecutive pieces of the utterance -- joining them turns one "алло" into
    "алло да". Caught by the offline translation test before this ever ran."""
    for alternative in update.alternatives:
        if alternative.text:
            return alternative.text.strip()
    return ""


def translate_stt_response(response) -> dict | None:
    """SpeechKit response -> the common event vocabulary, or None to ignore.

    Split out from the stream so it can be tested against synthetic protobufs
    without a live connection.

    `final_refinement` is ignored on purpose: it repeats the same utterance
    after post-processing, and the consumer accumulates every is_final
    transcript, so forwarding both would duplicate the caller's words.
    """
    kind = response.WhichOneof("Event")
    if kind == "partial":
        text = _alternative_text(response.partial)
        return {"type": "transcript", "text": text, "is_final": False} if text else None
    if kind == "final":
        text = _alternative_text(response.final)
        return {"type": "transcript", "text": text, "is_final": True} if text else None
    if kind == "eou_update":
        return {"type": "flush_done"}
    return None


class YandexSttStream:
    def __init__(self, channel, call):
        self._channel = channel
        self._call = call
        self._events: asyncio.Queue = asyncio.Queue()
        self._reader = asyncio.create_task(self._read())

    async def _read(self) -> None:
        try:
            async for response in self._call:
                event = translate_stt_response(response)
                if event is not None:
                    await self._events.put(event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._events.put({"type": "error", "message": f"{type(exc).__name__}: {exc}"})
        finally:
            await self._events.put(None)  # closes the async-for below

    async def send_audio(self, pcm: bytes) -> None:
        await self._call.write(stt_pb2.StreamingRequest(chunk=stt_pb2.AudioChunk(data=pcm)))

    async def end_utterance(self) -> None:
        await self._call.write(stt_pb2.StreamingRequest(eou=stt_pb2.Eou()))

    async def close(self) -> None:
        self._reader.cancel()
        self._call.cancel()
        # Channel deliberately left open -- it is shared, see _channel().

    async def __aiter__(self):
        while True:
            event = await self._events.get()
            if event is None:
                return
            yield event


async def open_stt() -> YandexSttStream:
    channel = _channel(STT_ENDPOINT)
    call = stt_service_pb2_grpc.RecognizerStub(channel).RecognizeStreaming(metadata=_metadata())
    await call.write(build_stt_options())
    return YandexSttStream(channel, call)


# Yandex v3 treats every string as TTS-marked, and its stress mark is a "+"
# placed BEFORE the stressed vowel ("з+амок"). Our prompts carry the Unicode
# combining acute AFTER the vowel instead ("Варпо\u0301инт"), which is what
# Cartesia understands -- passed through unchanged, Yandex does not recognise
# it and stresses the word wherever it likes. Reported live: wrong stress.
_COMBINING_ACUTE = "\u0301"


def to_yandex_stress(text: str) -> str:
    """Rewrites "vowel + combining acute" as Yandex's "+vowel".

    Provider-specific on purpose: the same combining acute is exactly what
    Cartesia wants, so this belongs in the Yandex adapter and nowhere near
    the prompts, which stay readable and provider-neutral.
    """
    if _COMBINING_ACUTE not in text:
        return text
    out = []
    for char in text:
        if char == _COMBINING_ACUTE:
            # Move the marker in front of the vowel it followed.
            if out:
                out.insert(len(out) - 1, "+")
        else:
            out.append(char)
    return "".join(out)


def build_tts_options() -> tts_pb2.StreamSynthesisRequest:
    return tts_pb2.StreamSynthesisRequest(options=tts_pb2.SynthesisOptions(
        # Not the Cartesia voice_id the settings store holds -- that is a
        # Cartesia UUID and means nothing here. Yandex voices are names.
        voice=os.environ.get("YANDEX_TTS_VOICE", "alena"),
        role=os.environ.get("YANDEX_TTS_ROLE", ""),
        # Default 1.0 reads noticeably slower than the Cartesia voice it
        # replaced -- same text measured 21% longer than the Cartesia voice -- and that was
        # reported on a live call as dragging. See YANDEX_TTS_SPEED.
        speed=float(os.environ.get("YANDEX_TTS_SPEED", "1.25")),
        output_audio_spec=tts_pb2.AudioFormatOptions(raw_audio=tts_pb2.RawAudio(
            audio_encoding=tts_pb2.RawAudio.LINEAR16_PCM,
            sample_rate_hertz=speech.TTS_SAMPLE_RATE))))


class YandexTtsStream:
    """One gRPC synthesis call per bot turn, multiplexed into one event queue.

    Cartesia keeps a single socket open for the whole call and separates turns
    by context_id; SpeechKit has no such notion, so a turn here is a whole
    call: opened on the first sentence, half-closed on the closing flush, and
    its end is what "done" means.
    """

    def __init__(self, channel):
        # ONE channel for the whole call, many calls on it -- one per turn.
        # Opening a channel per turn cost 1.03s to first audio against
        # Cartesia's 0.25s (measured), essentially all of it TCP+TLS setup
        # repeated for every sentence the bot says.
        self._channel = channel
        self._stub = tts_service_pb2_grpc.SynthesizerStub(channel)
        self._events: asyncio.Queue = asyncio.Queue()
        self._context_id: str | None = None
        self._call = None
        self._reader: asyncio.Task | None = None

    async def _open_turn(self, context_id: str) -> None:
        self._call = self._stub.StreamSynthesis(metadata=_metadata())
        await self._call.write(build_tts_options())
        self._context_id = context_id
        self._reader = asyncio.create_task(self._read(self._call, context_id))

    async def _read(self, call, context_id: str) -> None:
        try:
            async for response in call:
                if response.audio_chunk.data:
                    await self._events.put({"type": "chunk",
                                            "context_id": context_id,
                                            "pcm": response.audio_chunk.data})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._events.put({"type": "error", "message": f"{type(exc).__name__}: {exc}"})
        else:
            # The stream ending IS the end of generation for this turn.
            await self._events.put({"type": "done", "context_id": context_id})

    async def synthesize(self, text: str, context_id: str, keep_open: bool, voice_id: str) -> None:
        if self._context_id != context_id:
            await self._end_turn()
            await self._open_turn(context_id)
        await self._call.write(tts_pb2.StreamSynthesisRequest(
            synthesis_input=tts_pb2.SynthesisInput(text=to_yandex_stress(text))))
        # Flush after EVERY sentence, not only the last one. StreamSynthesis
        # buffers text until a ForceSynthesisEvent and sends nothing back
        # before it -- measured: no audio at all 4s after the first sentence,
        # then everything at once after the flush. Forcing only at the end
        # therefore threw away the whole point of streaming sentences off the
        # LLM: the caller waited for the complete reply to be written before
        # hearing a single word.
        await self._call.write(tts_pb2.StreamSynthesisRequest(
            force_synthesis=tts_pb2.ForceSynthesisEvent()))
        if not keep_open:
            await self._call.done_writing()
            self._context_id = None  # next sentence starts a fresh turn

    async def _end_turn(self) -> None:
        """Drops an unfinished turn. Leaves the channel alone -- it outlives
        every turn on the call."""
        if self._reader and not self._reader.done():
            self._reader.cancel()
        if self._call is not None:
            self._call.cancel()
        self._call = self._reader = None
        self._context_id = None

    async def close(self) -> None:
        await self._end_turn()
        # Channel deliberately left open -- it is shared, see _channel().
        await self._events.put(None)

    async def __aiter__(self):
        while True:
            event = await self._events.get()
            if event is None:
                return
            yield event


async def open_tts() -> YandexTtsStream:
    return YandexTtsStream(_channel(TTS_ENDPOINT))
