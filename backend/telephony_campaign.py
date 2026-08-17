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


async def _run(phone_numbers: list[str], delay_seconds: float) -> None:
    for i, phone_number in enumerate(phone_numbers):
        entry = _state["results"][i]
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
        if i < len(phone_numbers) - 1:
            await asyncio.sleep(delay_seconds)
    _state["status"] = "done"


def start(phone_numbers: list[str], delay_seconds: float) -> None:
    global _task
    # Module-level reference required: asyncio only holds a weak reference to
    # a bare create_task() result, so without this it could be garbage
    # collected mid-campaign.
    _state["status"] = "running"
    _state["total"] = len(phone_numbers)
    _state["started_at"] = datetime.now(timezone.utc).isoformat()
    _state["results"] = [
        {"phone_number": p, "status": "pending", "detail": None, "finished_at": None}
        for p in phone_numbers
    ]
    _task = asyncio.create_task(_run(phone_numbers, delay_seconds))
