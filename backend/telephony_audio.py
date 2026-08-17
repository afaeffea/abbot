from __future__ import annotations

import audioop  # stdlib on Python <3.13; the `audioop-lts` backport (see requirements.txt) on 3.13+
from array import array


def ulaw_to_pcm16(data: bytes) -> bytes:
    """G.711 μ-law bytes (what Voximplant streams for a PSTN call) -> PCM16LE."""
    return audioop.ulaw2lin(data, 2)


def resample_linear(pcm_bytes: bytes, from_rate: int, to_rate: int) -> bytes:
    """Same good-enough linear interpolation approach used elsewhere in this
    codebase (frontend/mic-worklet.js client-side, call_recorder._resample_linear
    for the review recording) — not hi-fi, just intelligible, kept consistent
    rather than reaching for a different technique here."""
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
