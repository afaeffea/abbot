from __future__ import annotations

import asyncio
import json
import time
import wave
from array import array
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

CALL_LOGS_DIR = Path(__file__).resolve().parent / "call_logs"
RECORDING_SAMPLE_RATE = 24000  # matches cartesia_realtime.TTS_SAMPLE_RATE
MIC_SAMPLE_RATE = 16000  # matches cartesia_realtime.STT_SAMPLE_RATE


def _resample_linear(pcm_bytes: bytes, from_rate: int, to_rate: int) -> array:
    """Same good-enough linear interpolation approach frontend/mic-worklet.js
    already uses client-side — not hi-fi, just intelligible for a review recording."""
    src = array("h")
    src.frombytes(pcm_bytes)
    if from_rate == to_rate or len(src) == 0:
        return src
    ratio = to_rate / from_rate
    out_len = int(len(src) * ratio)
    out = array("h", bytes(2 * out_len))
    for i in range(out_len):
        src_pos = i / ratio
        idx = int(src_pos)
        frac = src_pos - idx
        if idx + 1 < len(src):
            sample = src[idx] * (1 - frac) + src[idx + 1] * frac
        else:
            sample = src[idx]
        out[i] = int(max(-32768, min(32767, sample)))
    return out


class CallRecorder:
    """Accumulates per-call audio + turn timing for one CallSession, and writes
    a stereo WAV (caller=left, bot=right) plus a JSON transcript at call end."""

    def __init__(self):
        self.start_monotonic = time.monotonic()
        self.start_wall = datetime.now(timezone.utc)
        self.call_id = f"{self.start_wall:%Y%m%d-%H%M%S}-{uuid4().hex[:6]}"

        self.turns: list[dict] = []
        self.mic_chunks: list[tuple[float, bytes]] = []
        self.tts_chunks: list[tuple[float, bytes]] = []
        self.outcome: dict | None = None
        # Set by TelephonySession right after construction for outbound calls
        # (see outbound.voxengine.js/bridge.voxengine.js) -- the only way to
        # later match a finished call's transcript back to the dialed number.
        self.phone_number: str | None = None
        self.client_name: str | None = None

        self._pending_speech_end: float | None = None
        # Both of these are pending rather than written straight into the turn
        # because they happen BEFORE the turn exists -- open_turn is called from
        # inside bot_turn, which the transcript is what triggers.
        self._pending_transcript: float | None = None
        self._open_turn: dict | None = None

    def _offset(self) -> float:
        return time.monotonic() - self.start_monotonic

    def mark_speech_end(self) -> None:
        self._pending_speech_end = self._offset()

    def mark_transcript_received(self) -> None:
        """Split point between "STT was slow" and "the model was slow".
        Without it both are bundled into llm_first_token_latency_ms and the two
        cannot be told apart from the logs -- which is exactly the question that
        comes up every time a call feels sluggish."""
        self._pending_transcript = self._offset()

    def open_turn(self, context_id: str, user_text: str | None) -> None:
        self._open_turn = {
            "index": len(self.turns),
            "context_id": context_id,
            "user_text": user_text,
            "bot_text": "",
            "user_speech_end_offset": self._pending_speech_end,
            "transcript_offset": self._pending_transcript,
            "llm_first_token_offset": None,
            "tts_first_audio_offset": None,
            "tool_calls": [],
            "cancelled": False,
        }
        self._pending_speech_end = None
        self._pending_transcript = None

    def mark_first_llm_token(self, context_id: str) -> None:
        turn = self._open_turn
        if turn and turn["context_id"] == context_id and turn["llm_first_token_offset"] is None:
            turn["llm_first_token_offset"] = self._offset()

    def mark_first_tts_audio(self, context_id: str) -> None:
        """Looks in the closed turns too, not just the open one.

        A turn closes as soon as its text has been handed to TTS, but the
        audio comes back after that -- 0.2-0.6s later on Yandex, which
        synthesizes a flushed sentence rather than streaming as it goes. Only
        checking the open turn silently dropped this measurement on every
        short reply, and on production it came out null for entire calls,
        which is exactly when the number was wanted."""
        turn = self._open_turn
        if turn is None or turn["context_id"] != context_id:
            turn = next((t for t in reversed(self.turns) if t["context_id"] == context_id), None)
        if turn is not None and turn["tts_first_audio_offset"] is None:
            turn["tts_first_audio_offset"] = self._offset()

    def record_tool_call(self, context_id: str, name: str, args: dict) -> None:
        turn = self._open_turn
        if turn and turn["context_id"] == context_id:
            turn["tool_calls"].append({"name": name, "arguments": args})
        if name == "confirm_booking":
            self.outcome = {"booked": True, **args}
        elif name == "qualify_lead":
            self.outcome = {**args}

    def close_turn(self, context_id: str, bot_text: str, cancelled: bool = False) -> None:
        turn = self._open_turn
        if not turn or turn["context_id"] != context_id:
            return
        turn["bot_text"] = bot_text
        turn["cancelled"] = cancelled
        self.turns.append(turn)
        self._open_turn = None

    def record_mic_chunk(self, pcm_bytes: bytes) -> None:
        self.mic_chunks.append((self._offset(), pcm_bytes))

    def record_tts_chunk(self, pcm_bytes: bytes) -> None:
        self.tts_chunks.append((self._offset(), pcm_bytes))

    def sheet_summary(self) -> dict:
        return {
            "call_id": self.call_id,
            "started_at": self.start_wall.isoformat(),
            "duration_seconds": round(self._offset(), 1),
            "outcome": self.outcome,
            "turn_count": len(self.turns),
            "phone_number": self.phone_number,
            "client_name": self.client_name,
        }

    async def finalize(self) -> None:
        try:
            await asyncio.to_thread(self._finalize_sync)
        except Exception as exc:
            print(f"[call_recorder] Failed to finalize call {self.call_id}: {exc}")

    def _finalize_sync(self) -> None:
        call_dir = CALL_LOGS_DIR / self.call_id
        call_dir.mkdir(parents=True, exist_ok=True)
        self._write_wav(call_dir / "recording.wav")
        self._write_transcript(call_dir / "transcript.json")

    def _write_wav(self, path: Path) -> None:
        duration = self._offset()
        for chunks, rate in ((self.mic_chunks, MIC_SAMPLE_RATE), (self.tts_chunks, RECORDING_SAMPLE_RATE)):
            for offset, pcm_bytes in chunks:
                chunk_duration = (len(pcm_bytes) / 2) / rate
                duration = max(duration, offset + chunk_duration)

        total_samples = int(duration * RECORDING_SAMPLE_RATE) + RECORDING_SAMPLE_RATE  # +1s pad
        left = array("h", bytes(2 * total_samples))
        right = array("h", bytes(2 * total_samples))

        # Chunks are appended in arrival order but can land in a tight burst
        # (Cartesia TTS generates faster than real-time; the client's
        # pre-speech buffer also flushes several chunks at once) — placing
        # each purely by its own arrival offset would then make same-channel
        # chunks overlap and clobber each other, which is what was producing
        # the choppy/"robotic" recordings with bits of audio missing. A
        # monotonic per-channel cursor keeps chunks back-to-back instead.
        mic_cursor = 0
        for offset, pcm_bytes in self.mic_chunks:
            samples = _resample_linear(pcm_bytes, MIC_SAMPLE_RATE, RECORDING_SAMPLE_RATE)
            start = max(int(offset * RECORDING_SAMPLE_RATE), mic_cursor)
            end = min(start + len(samples), total_samples)
            if end > start:
                left[start:end] = samples[: end - start]
            mic_cursor = start + len(samples)

        tts_cursor = 0
        for offset, pcm_bytes in self.tts_chunks:
            samples = array("h")
            samples.frombytes(pcm_bytes)
            start = max(int(offset * RECORDING_SAMPLE_RATE), tts_cursor)
            end = min(start + len(samples), total_samples)
            if end > start:
                right[start:end] = samples[: end - start]
            tts_cursor = start + len(samples)

        # Fast C-level interleave (step-slice assignment), not a per-sample Python loop.
        interleaved = array("h", bytes(4 * total_samples))
        interleaved[0::2] = left
        interleaved[1::2] = right

        with wave.open(str(path), "wb") as wf:
            wf.setnchannels(2)
            wf.setsampwidth(2)
            wf.setframerate(RECORDING_SAMPLE_RATE)
            wf.writeframes(interleaved.tobytes())

    def _write_transcript(self, path: Path) -> None:
        def latency_ms(user_end, offset):
            if offset is None or user_end is None:
                return None
            return round((offset - user_end) * 1000)

        turns = [
            {
                "index": turn["index"],
                "user_text": turn["user_text"],
                "bot_text": turn["bot_text"],
                "user_speech_end_offset": turn["user_speech_end_offset"],
                # All three are measured from the same t=0 (the caller stopped
                # talking), so they nest: stt <= llm_first_token <= tts_first_audio.
                # Subtract neighbours to get each stage's own cost. Note the last
                # one is when audio reached the BACKEND -- resampling, the pacer,
                # VoxEngine's buffer and the RTP leg all come after it, so what
                # the caller actually experiences is strictly larger.
                "stt_latency_ms": latency_ms(turn["user_speech_end_offset"], turn["transcript_offset"]),
                "llm_first_token_latency_ms": latency_ms(turn["user_speech_end_offset"], turn["llm_first_token_offset"]),
                "tts_first_audio_latency_ms": latency_ms(turn["user_speech_end_offset"], turn["tts_first_audio_offset"]),
                "tool_calls": turn["tool_calls"],
                "cancelled": turn["cancelled"],
            }
            for turn in self.turns
        ]
        data = {
            "call_id": self.call_id,
            "started_at": self.start_wall.isoformat(),
            "duration_seconds": round(self._offset(), 1),
            "outcome": self.outcome,
            "phone_number": self.phone_number,
            "client_name": self.client_name,
            "turns": turns,
        }
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
