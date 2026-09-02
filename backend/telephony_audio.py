from __future__ import annotations

import audioop  # stdlib on Python <3.13; the `audioop-lts` backport (see requirements.txt) on 3.13+
import math
from array import array

# Anti-alias filter for the 24kHz -> 8kHz TTS downsample. See
# Downsampler24kTo8k for why plain interpolation is not enough here.
_DECIM_FACTOR = 3          # 24000 / 8000, exact
# Tuned by measurement, not picked off a chart. A 21-tap/3400Hz filter killed
# the aliasing fine but cost -3.4dB at 3kHz, and 2-3.5kHz is exactly where
# consonants live -- trading grit for mush is not a win. Widening the cutoff
# and lengthening the filter buys back the passband without giving up the
# stopband: 1kHz -0.0dB, 3kHz -0.2dB, 3.4kHz -1.7dB, while the aliases land at
# -58dB (5kHz), -56dB (6kHz), -77dB (7kHz). Costs ~14ms of CPU per second of
# synthesized audio; the bot talks roughly 40% of a call, so ~6ms/s per call.
_FIR_TAPS_COUNT = 41
_FIR_CUTOFF_HZ = 3800.0    # just under the 4kHz Nyquist of the 8kHz output


def _design_lowpass(num_taps: int, cutoff_hz: float, rate: int) -> tuple[float, ...]:
    """Hamming-windowed sinc, normalised to unity DC gain so the filter
    cannot change how loud the bot is."""
    taps = []
    middle = (num_taps - 1) / 2.0
    fc = cutoff_hz / rate
    for i in range(num_taps):
        k = i - middle
        sinc = 2 * fc if k == 0 else math.sin(2 * math.pi * fc * k) / (math.pi * k)
        taps.append(sinc * (0.54 - 0.46 * math.cos(2 * math.pi * i / (num_taps - 1))))
    gain = sum(taps)
    return tuple(t / gain for t in taps)


_FIR_TAPS = _design_lowpass(_FIR_TAPS_COUNT, _FIR_CUTOFF_HZ, 24000)


def ulaw_to_pcm16(data: bytes) -> bytes:
    """G.711 μ-law bytes (what Voximplant streams for a PSTN call) -> PCM16LE."""
    return audioop.ulaw2lin(data, 2)


def pcm16_to_ulaw(data: bytes) -> bytes:
    """PCM16LE -> G.711 μ-law bytes (what Voximplant expects on the call leg)."""
    return audioop.lin2ulaw(data, 2)


class Downsampler24kTo8k:
    """Low-pass filter + 3:1 decimation for the bot's own voice, carrying
    filter state across chunk boundaries.

    Why this exists instead of resample_linear: dropping 2 of every 3 samples
    without filtering first folds everything above 4kHz back down into the
    speech band at full amplitude. Measured on a 6kHz tone (the region of
    "с"/"ш"/"щ" and the air in a voice): the ghost at 2kHz came back at 0.366
    against an original of 0.366 -- no attenuation whatsoever. That is heard
    on a call as grit and harshness on every sibilant. With this filter the
    same ghost measures ~0.001 (-50dB) while a 1kHz control tone loses only
    0.3dB.

    `audioop.ratecv` is NOT a substitute -- it was measured on the same tone
    and produced the identical 0.366 ghost, because it interpolates without
    filtering too.

    Stateful on purpose. _emit_tts_audio is called once per Cartesia chunk,
    and restarting the filter on each one would put a discontinuity at every
    chunk boundary -- audible as periodic clicking. The tail of the previous
    chunk is kept and prepended, so the output is bit-identical to filtering
    the whole utterance at once.

    One instance per call: state also carries across turns, which is
    harmless (the carry-over is _FIR_TAPS_COUNT // 2 samples, ~0.4ms) and
    keeps the filter continuous."""

    def __init__(self) -> None:
        self._half = len(_FIR_TAPS) // 2
        # Zero-priming the history means the first output sample is centred on
        # the first real input sample, matching an offline filter's alignment.
        self._tail = array("h", bytes(2 * self._half))
        self._pos = self._half

    def process(self, pcm_bytes: bytes) -> bytes:
        incoming = array("h")
        incoming.frombytes(pcm_bytes)
        buf = self._tail + incoming
        taps = _FIR_TAPS
        width = len(taps)
        half = self._half
        pos = self._pos

        out = array("h")
        while pos + half < len(buf):
            start = pos - half
            acc = 0.0
            for sample, tap in zip(buf[start:start + width], taps):
                acc += sample * tap
            out.append(-32768 if acc < -32768 else (32767 if acc > 32767 else int(acc)))
            pos += _DECIM_FACTOR

        # Keep only what the next call still needs to look back at.
        keep_from = max(0, pos - half)
        self._tail = buf[keep_from:]
        self._pos = pos - keep_from
        return out.tobytes()


def resample_linear(pcm_bytes: bytes, from_rate: int, to_rate: int) -> bytes:
    """Same good-enough linear interpolation approach used elsewhere in this
    codebase (frontend/mic-worklet.js client-side, call_recorder._resample_linear
    for the review recording) — not hi-fi, just intelligible, kept consistent
    rather than reaching for a different technique here.

    NB: fine for upsampling (the inbound 8k->16k leg) and for the review
    recording, but NOT for the outbound 24k->8k leg -- see Downsampler24kTo8k."""
    src = array("h")
    src.frombytes(pcm_bytes)
    if from_rate == to_rate or len(src) == 0:
        return pcm_bytes
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
    return out.tobytes()
