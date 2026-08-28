from __future__ import annotations

import json
import os

import httpx

MANAGEMENT_API_BASE = "https://api.voximplant.com/platform_api"


async def start_outbound_call(destination: str, client_name: str | None = None) -> dict:
    """Triggers the outbound VoxEngine scenario (voximplant/scenarios/outbound.voxengine.js)
    via Voximplant's Management API StartScenarios method. Raises RuntimeError
    with Voximplant's own error text on failure -- callers (main.py) turn that
    into a 502, never swallow it, same as every other external API call in
    this codebase."""
    custom_data = {"destination": destination}
    if client_name:
        custom_data["client_name"] = client_name
    params = {
        "account_id": os.environ["VOXIMPLANT_ACCOUNT_ID"],
        "api_key": os.environ["VOXIMPLANT_API_KEY"],
        "rule_id": os.environ["VOXIMPLANT_OUTBOUND_RULE_ID"],
        "script_custom_data": json.dumps(custom_data),
    }
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(f"{MANAGEMENT_API_BASE}/StartScenarios", data=params)

    if response.status_code != 200:
        raise RuntimeError(f"Voximplant Management API вернул {response.status_code}: {response.text}")

    data = response.json()
    # Voximplant's classic Management API returns HTTP 200 even for logical
    # errors (bad api_key, unknown rule_id, etc.), with the real error inside
    # the JSON body -- checking status_code alone would silently accept those.
    if isinstance(data, dict) and "error" in data:
        error = data["error"]
        message = error.get("msg", error) if isinstance(error, dict) else error
        raise RuntimeError(f"Voximplant: {message}")

    return data


# Voximplant's own SIP-ish reason text -> a word the campaign report can use.
# Matched case-insensitively as a substring, most specific first.
_END_REASON_LABELS = (
    ("busy", "занято"),
    ("decline", "сброшен"),
    ("reject", "сброшен"),
    ("terminated", "сброшен"),
    ("unavailable", "недоступен"),
    ("not found", "номер не найден"),
    ("no answer", "не ответил"),
    ("timeout", "не ответил"),
    ("forbidden", "запрещён оператором"),
)


def _label_end_reason(details: str, answered: bool) -> str:
    lowered = (details or "").lower()
    for needle, label in _END_REASON_LABELS:
        if needle in lowered:
            return label
    return "поговорили" if answered else "не ответил"


async def get_call_session(session_id: int | str) -> dict | None:
    """What actually happened to one outbound call, straight from the
    operator: answered or not, how long, and why it ended.

    The backend only ever learns about a call the caller ANSWERED -- an
    unanswered one never opens a WebSocket, so nothing reaches us at all.
    Without this the campaign script has no way to tell "nobody picked up"
    from "the call happened but its result went missing", and has to sit out
    its whole timeout before guessing (see scripts/run_campaign.py).

    Returns None while the session is still in progress -- Voximplant only
    writes history once a session ends.
    """
    params = {
        "account_id": os.environ["VOXIMPLANT_ACCOUNT_ID"],
        "api_key": os.environ["VOXIMPLANT_API_KEY"],
        "call_session_history_id": str(session_id),
        "with_calls": "true",
    }
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(f"{MANAGEMENT_API_BASE}/GetCallHistory/", params=params)
    if response.status_code != 200:
        raise RuntimeError(f"Voximplant Management API вернул {response.status_code}: {response.text}")

    data = response.json()
    results = data.get("result") or []
    if not results:
        # Either still ringing/talking, or the id is unknown. Voximplant
        # reports both the same way, and "not finished yet" is by far the
        # likelier of the two for a session we just started ourselves.
        return None

    session = results[0]
    calls = session.get("calls") or []
    leg = calls[0] if calls else {}
    details = (leg.get("end_reason") or {}).get("details", "")
    answered = bool(leg.get("successful")) and (leg.get("duration") or 0) > 0
    return {
        "session_id": session.get("call_session_history_id"),
        "finished": True,
        "answered": answered,
        "duration_seconds": leg.get("duration"),
        "cost": leg.get("cost"),
        "end_reason": details,
        "label": _label_end_reason(details, answered),
    }
