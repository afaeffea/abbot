"""One-call diagnostic for the ws->call audio direction.

Every attempt so far at getting the bot's synthesized voice into a real
phone call has been silent, and each real call could only test ONE guess
about what VoxEngine's `ws.sendMediaTo(call, ...)` route actually wants on
the wire (raw binary mu-law? raw binary PCM? a JSON envelope like the one
VoxEngine itself sends us in the other direction?). At ~2 rubles and a
manual "did you hear anything?" per call, guessing one format at a time is
the slow way to do this.

So this endpoint plays a numbered beep sequence instead of a bot: variant 1
sends ONE beep, variant 2 sends TWO beeps, and so on -- each variant using a
different wire format. Whoever is on the call just counts beeps, and the
count identifies which format(s) VoxEngine accepted. Silence for every
variant means the problem isn't the format at all.

Point the scenario's BACKEND_WS_URL at /ws/telephony_probe instead of
/ws/telephony to use it (see voximplant/modules/bridge.voxengine.js), and
point it back when done -- nothing here touches the real session code.
"""
from __future__ import annotations

import asyncio
import base64
import json
import math
import time
from array import array

from fastapi import WebSocket
from starlette.websockets import WebSocketState

import telephony_audio

SAMPLE_RATE = 8000
SAMPLES_PER_FRAME = 160  # 20ms, matching the framing VoxEngine sends us
FRAME_INTERVAL = SAMPLES_PER_FRAME / SAMPLE_RATE

BEEP_HZ = 800
BEEP_S = 0.25
GAP_S = 0.2
BETWEEN_VARIANTS_S = 1.2


def _tone_pcm16(seconds: float, hz: int = BEEP_HZ, amplitude: float = 0.45) -> bytes:
    """A plain loud sine at 8kHz, PCM16LE. Loud and mid-band on purpose: a
    quiet or high tone that didn't survive PSTN's 300-3400Hz band would be
    indistinguishable from the "format rejected" result we're testing for."""
    n = int(SAMPLE_RATE * seconds)
    out = array("h", bytes(2 * n))
    for i in range(n):
        out[i] = int(amplitude * 32767 * math.sin(2 * math.pi * hz * i / SAMPLE_RATE))
    return out.tobytes()


def _silence_pcm16(seconds: float) -> bytes:
    return bytes(int(SAMPLE_RATE * seconds) * 2)


def _beeps_pcm16(count: int) -> bytes:
    """`count` beeps -- this is the variant's identity, the thing the person
    on the call reports back."""
    parts = []
    for _ in range(count):
        parts.append(_tone_pcm16(BEEP_S))
        parts.append(_silence_pcm16(GAP_S))
    return b"".join(parts)


class _Sender:
    """One wire format under test. Each takes the same 20ms PCM16 frame and
    puts it on the socket the way that format would."""

    def __init__(self, name: str, send):
        self.name = name
        self.send = send


def _build_variants(ws: WebSocket) -> list[_Sender]:
    seq = {"n": 0}

    async def raw_ulaw(frame_pcm16: bytes) -> None:
        await ws.send_bytes(telephony_audio.pcm16_to_ulaw(frame_pcm16))

    async def raw_pcm16(frame_pcm16: bytes) -> None:
        await ws.send_bytes(frame_pcm16)

    async def json_ulaw(frame_pcm16: bytes) -> None:
        seq["n"] += 1
        payload = base64.b64encode(telephony_audio.pcm16_to_ulaw(frame_pcm16)).decode()
        await ws.send_text(json.dumps({
            "event": "media",
            "sequenceNumber": seq["n"],
            "media": {"payload": payload},
        }))

    async def json_pcm16(frame_pcm16: bytes) -> None:
        seq["n"] += 1
        payload = base64.b64encode(frame_pcm16).decode()
        await ws.send_text(json.dumps({
            "event": "media",
            "sequenceNumber": seq["n"],
            "media": {"payload": payload},
        }))

    async def json_ulaw_with_format(frame_pcm16: bytes) -> None:
        # Same as json_ulaw but repeating the mediaFormat VoxEngine announced
        # to us, in case the route needs to be told what it's receiving.
        seq["n"] += 1
        payload = base64.b64encode(telephony_audio.pcm16_to_ulaw(frame_pcm16)).decode()
        await ws.send_text(json.dumps({
            "event": "media",
            "sequenceNumber": seq["n"],
            "media": {
                "payload": payload,
                "mediaFormat": {"encoding": "ULAW", "sampleRate": SAMPLE_RATE},
            },
        }))

    return [
        _Sender("1: raw binary ULAW 160B", raw_ulaw),
        _Sender("2: raw binary PCM16 320B", raw_pcm16),
        _Sender("3: JSON media envelope, base64 ULAW", json_ulaw),
        _Sender("4: JSON media envelope, base64 PCM16", json_pcm16),
        _Sender("5: JSON media envelope + mediaFormat, base64 ULAW", json_ulaw_with_format),
    ]


async def _log_incoming(ws: WebSocket, call_id: str) -> None:
    """Drains and summarizes whatever VoxEngine sends us. The scenario reports
    back over this same socket (see reportDiag) whether our frames reached its
    JS message handler -- if they did, the native media route is NOT consuming
    them, which is the single most useful thing this probe can learn."""
    binary_count = 0
    while True:
        message = await ws.receive()
        if message["type"] == "websocket.disconnect":
            return
        if message.get("bytes") is not None:
            binary_count += 1
            if binary_count in (1, 250, 500):
                print(f"[probe:{call_id}] {binary_count} inbound binary frames "
                      f"({len(message['bytes'])} bytes each)")
            continue
        text = message.get("text")
        if text is not None:
            print(f"[probe:{call_id}] text from Voximplant: {text[:300]}")


async def _play(ws: WebSocket, sender: _Sender, pcm16: bytes, call_id: str) -> None:
    """Paced at one 20ms frame per 20ms of wall time -- a live RTP leg won't
    take a burst. Framing is by SAMPLES, not bytes, so it stays 20ms whether
    the format under test is 1 or 2 bytes per sample."""
    frame_bytes = SAMPLES_PER_FRAME * 2  # PCM16 source frames
    next_send = time.monotonic()
    sent = 0
    for offset in range(0, len(pcm16), frame_bytes):
        frame = pcm16[offset:offset + frame_bytes]
        if len(frame) < frame_bytes:
            frame = frame + bytes(frame_bytes - len(frame))
        now = time.monotonic()
        if next_send > now:
            await asyncio.sleep(next_send - now)
        if ws.client_state != WebSocketState.CONNECTED:
            print(f"[probe:{call_id}] socket closed mid-variant {sender.name}")
            return
        await sender.send(frame)
        sent += 1
        next_send += FRAME_INTERVAL
    print(f"[probe:{call_id}] variant {sender.name}: {sent} frames sent")


async def run_probe(ws: WebSocket, call_id: str) -> None:
    reader = asyncio.create_task(_log_incoming(ws, call_id))
    try:
        variants = _build_variants(ws)
        # A beat before the first beep: the call leg has just been answered and
        # anything sent in the first moment risks being cut by the carrier.
        await asyncio.sleep(1.0)
        for index, sender in enumerate(variants, start=1):
            print(f"[probe:{call_id}] === variant {sender.name} -- expect {index} beep(s) ===")
            await _play(ws, sender, _beeps_pcm16(index), call_id)
            await asyncio.sleep(BETWEEN_VARIANTS_S)
        print(f"[probe:{call_id}] all variants done")
        await asyncio.sleep(2.0)
    except Exception as exc:
        print(f"[probe:{call_id}] probe failed: {exc!r}")
        raise
    finally:
        reader.cancel()
