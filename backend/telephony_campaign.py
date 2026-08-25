from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import voximplant_client

# Single in-memory campaign at a time -- matches this codebase's existing
# pattern of process-lifetime state (call history during a call, etc, see
# README) rather than a persisted job queue. A server restart mid-campaign
# loses progress; acceptable for a solo-operator admin tool, revisit if that
# becomes a real problem.
_state: dict = {"status": "idle", "total": 0, "started_at": None, "results": []}
_task: asyncio.Task | None = None


def get_state() -> dict:
    return _state


def is_running() -> bool:
    return _state["status"] == "running"


async def _dial_one(semaphore: asyncio.Semaphore, index: int, phone_number: str, hold_seconds: float) -> None:
    async with semaphore:
        entry = _state["results"][index]
        try:
            await voximplant_client.start_outbound_call(phone_number)
            entry["status"] = "started"
        except Exception as exc:
            # One bad number (unreachable, bad format Voximplant itself
            # rejects, etc.) must not abort the rest of the list -- record
            # and move on, same "never swallow, but never let one failure
            # cascade" spirit as the rest of this codebase.
            entry["status"] = "error"
            entry["detail"] = str(exc)
        entry["finished_at"] = datetime.now(timezone.utc).isoformat()
        # start_outbound_call only *triggers* the call (Voximplant's
        # StartScenarios API returns almost immediately) -- the actual call
        # lifecycle runs entirely in telephony_session.py, on a WebSocket
        # connection Voximplant opens back to us later, with nothing linking
        # it back to this dial loop. So there's no real signal for "that
        # call actually ended" to release this slot on. Holding it for
        # hold_seconds instead approximates one call's real duration --
        # operators should set this close to how long calls actually run,
        # not just a short anti-spam pause, or slots will free up faster
        # than calls really finish and concurrency will be an undercount.
        await asyncio.sleep(hold_seconds)


async def _run(phone_numbers: list[str], hold_seconds: float, concurrency: int) -> None:
    semaphore = asyncio.Semaphore(concurrency)
    await asyncio.gather(*(
        _dial_one(semaphore, i, phone_number, hold_seconds)
        for i, phone_number in enumerate(phone_numbers)
    ))
    _state["status"] = "done"


def start(phone_numbers: list[str], delay_seconds: float, concurrency: int) -> None:
    global _task
    # Module-level reference required: asyncio only holds a weak reference to
    # a bare create_task() result, so without this it could be garbage
    # collected mid-campaign.
    _state["status"] = "running"
    _state["total"] = len(phone_numbers)
    _state["concurrency"] = concurrency
    _state["started_at"] = datetime.now(timezone.utc).isoformat()
    _state["results"] = [
        {"phone_number": p, "status": "pending", "detail": None, "finished_at": None}
        for p in phone_numbers
    ]
    _task = asyncio.create_task(_run(phone_numbers, delay_seconds, concurrency))
