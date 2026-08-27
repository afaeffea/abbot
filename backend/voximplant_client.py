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
