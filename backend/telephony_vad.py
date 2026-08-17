from __future__ import annotations

from array import array
from collections import deque
from dataclasses import dataclass, field

# Target ~500ms of pre-roll, mirroring frontend/mic-worklet.js's intent
# (preRollMaxChunks=5 at its own 100ms/chunk = 500ms). Capped by actual
# buffered duration, not a fixed chunk COUNT — telephony chunks are 20ms
# (confirmed live via Voximplant's "start" event: 160 bytes @ 8kHz), so a
# 5-chunk cap there was only 100ms and clipped the start of real speech.
PRE_ROLL_TARGET_MS = 500


@dataclass
class VADEvent:
    speech_start: bool = False
    speech_end: bool = False
    barge_in: bool = False  # speech_start while bot_speaking was already True
    forward_chunks: list[bytes] = field(default_factory=list)  # chunks to send to STT now, in order


class ServerVAD:
    """Python port of frontend/mic-worklet.js's energy-RMS voice-activity
    state machine, for transports with no browser to run the client-side
    version (phone calls). Reuses the exact same settings_store.py `vad`
    dict shape/values unchanged — same thresholds apply to both transports.

    Operates on whatever-sized PCM16 chunks the caller hands to process()
    (already resampled to STT's sample rate) — unlike the AudioWorklet, which
    is forced into fixed ~128-sample render quanta by the Web Audio API, there's
    no such constraint here, so one process() call = one VAD decision, on
    whatever chunk size arrived. This makes the exact frame where a
    speech_end hangover elapses a slightly coarser boundary than the JS
    version's — accepted, not worth matching exactly (that chunk is right at
    the tail of the silence hangover, i.e. already near-silent either way).
    """

    def __init__(self, vad_settings: dict, sample_rate: int):
        self.energy_threshold = vad_settings["energy_threshold"]
        self.speech_start_ms = vad_settings["speech_start_ms"]
        self.silence_hangover_ms = vad_settings["silence_hangover_ms"]
        self.barge_in_energy_threshold = vad_settings["barge_in_energy_threshold"]
        self.barge_in_speech_start_ms = vad_settings["barge_in_speech_start_ms"]
        self.sample_rate = sample_rate

        self.bot_speaking = False
        self.speaking = False
        self.speech_ms = 0.0
        self.silence_ms = 0.0
        self.forwarding = False
        self.pre_roll: deque[bytes] = deque()  # capped by duration in _buffer_pre_roll, not chunk count
        self._pre_roll_ms = 0.0

    def set_bot_speaking(self, value: bool) -> None:
        if value != self.bot_speaking:
            self.bot_speaking = value
            # A state flip means whatever we were mid-measuring no longer
            # applies (mirrors mic-worklet.js's port.onmessage handler) —
            # restart the count cleanly rather than mixing pre/post thresholds.
            self.speech_ms = 0.0
            self.silence_ms = 0.0

    def _buffer_pre_roll(self, pcm16_chunk: bytes, block_ms: float) -> None:
        self.pre_roll.append(pcm16_chunk)
        self._pre_roll_ms += block_ms
        while self._pre_roll_ms > PRE_ROLL_TARGET_MS and len(self.pre_roll) > 1:
            oldest = self.pre_roll.popleft()
            oldest_samples = len(oldest) // 2  # PCM16 = 2 bytes/sample
            self._pre_roll_ms -= (oldest_samples / self.sample_rate) * 1000

    def process(self, pcm16_chunk: bytes) -> VADEvent:
        samples = array("h")
        samples.frombytes(pcm16_chunk)
        if not samples:
            return VADEvent()

        # RMS normalized to [-1, 1] — the same scale mic-worklet.js's Web Audio
        # Float32 samples use, so settings_store.DEFAULT_SETTINGS["vad"]'s
        # threshold values are meaningful unchanged for this transport too.
        sum_squares = sum((s / 32768.0) ** 2 for s in samples)
        rms = (sum_squares / len(samples)) ** 0.5
        block_ms = (len(samples) / self.sample_rate) * 1000

        threshold = self.barge_in_energy_threshold if self.bot_speaking else self.energy_threshold
        required_speech_ms = self.barge_in_speech_start_ms if self.bot_speaking else self.speech_start_ms

        event = VADEvent()

        if rms > threshold:
            self.speech_ms += block_ms
            self.silence_ms = 0.0
            if not self.speaking and self.speech_ms >= required_speech_ms:
                self.speaking = True
                self.forwarding = True
                event.speech_start = True
                event.barge_in = self.bot_speaking
                event.forward_chunks = list(self.pre_roll) + [pcm16_chunk]
                self.pre_roll.clear()
                self._pre_roll_ms = 0.0
            elif self.forwarding:
                event.forward_chunks = [pcm16_chunk]
            else:
                self._buffer_pre_roll(pcm16_chunk, block_ms)
        else:
            self.silence_ms += block_ms
            self.speech_ms = 0.0
            if self.speaking and self.silence_ms >= self.silence_hangover_ms:
                self.speaking = False
                self.forwarding = False
                event.speech_end = True
            elif self.forwarding:
                event.forward_chunks = [pcm16_chunk]
            else:
                self._buffer_pre_roll(pcm16_chunk, block_ms)

        return event
