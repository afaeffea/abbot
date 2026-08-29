#!/usr/bin/env python3
"""Uploads the VoxEngine scenarios to Voximplant, with the placeholders
filled in from backend/.env.

A VoxEngine "scenario" is one flat JS file, so each of ours is
voximplant/modules/bridge.voxengine.js concatenated with one of
voximplant/scenarios/*.voxengine.js. Both carry deliberate placeholders that
must never be committed with real values (BACKEND_WS_TOKEN is a secret;
CALLER_ID and the backend domain are environment-specific).

Doing that substitution by hand in the Control Panel editor is how you lose
an afternoon: forgetting CALLER_ID leaves the placeholder +70000000000 in
place, and every outbound call then fails with a bare "403 Forbidden" from
the carrier, with nothing in the backend logs to explain it (this happened,
2026-08-28). Hence this script -- it refuses to upload anything that still
contains a placeholder.

Usage:
    python deploy_voximplant.py                 # обновить оба сценария
    python deploy_voximplant.py --only outbound # только исходящий
    python deploy_voximplant.py --dry-run       # собрать и проверить, но не заливать
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent
BRIDGE = REPO_ROOT / "voximplant" / "modules" / "bridge.voxengine.js"
SCENARIOS_DIR = REPO_ROOT / "voximplant" / "scenarios"
ENV_PATH = REPO_ROOT / "backend" / ".env"

API_URL = "https://api.voximplant.com/platform_api/SetScenarioInfo/"

# Every placeholder that must be gone before an upload is allowed.
PLACEHOLDERS = ("YOUR-RAILWAY-DOMAIN", "YOUR-TELEPHONY-WS-TOKEN", "+70000000000")


def read_env() -> dict[str, str]:
    if not ENV_PATH.exists():
        sys.exit(f"Не нашёл {ENV_PATH} -- он нужен для домена, токена и номера.")
    env = {}
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^([A-Z0-9_]+)=(.*)$", line.strip())
        if match:
            env[match.group(1)] = match.group(2).strip()
    return env


def require(env: dict[str, str], key: str) -> str:
    value = env.get(key, "").strip()
    if not value:
        sys.exit(f"В backend/.env не заполнен {key}.")
    return value


def build(kind: str, env: dict[str, str]) -> str:
    """kind is "inbound" or "outbound"."""
    domain = require(env, "RAILWAY_PUBLIC_DOMAIN").replace("https://", "").replace("http://", "").rstrip("/")
    bridge = BRIDGE.read_text(encoding="utf-8")
    bridge = bridge.replace("wss://YOUR-RAILWAY-DOMAIN/ws/telephony", f"wss://{domain}/ws/telephony")
    bridge = bridge.replace("YOUR-TELEPHONY-WS-TOKEN", require(env, "TELEPHONY_WS_TOKEN"))

    scenario = (SCENARIOS_DIR / f"{kind}.voxengine.js").read_text(encoding="utf-8")
    if kind == "outbound":
        scenario = scenario.replace('var CALLER_ID = "+70000000000";',
                                    f'var CALLER_ID = "{require(env, "VOXIMPLANT_CALLER_ID")}";')

    source = bridge + "\n" + scenario
    leftover = [p for p in PLACEHOLDERS if p in source]
    if leftover:
        sys.exit(f"В собранном сценарии {kind} остались заглушки: {', '.join(leftover)}. "
                 f"Заливать нельзя -- звонки сломаются.")
    return source


def upload(kind: str, source: str, env: dict[str, str]) -> None:
    scenario_id = require(env, f"VOXIMPLANT_{kind.upper()}_SCENARIO_ID")
    response = httpx.post(API_URL, data={
        "account_id": require(env, "VOXIMPLANT_ACCOUNT_ID"),
        "api_key": require(env, "VOXIMPLANT_API_KEY"),
        "scenario_id": scenario_id,
        "scenario_script": source,
    }, timeout=60)
    body = response.json()
    if body.get("result") != 1:
        sys.exit(f"Voximplant отклонил сценарий {kind} (id {scenario_id}): {body}")
    print(f"  {kind:9} -> сценарий {scenario_id} обновлён ({len(source)} символов)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--only", choices=["inbound", "outbound"], help="Залить только один сценарий")
    parser.add_argument("--dry-run", action="store_true", help="Собрать и проверить заглушки, но не заливать")
    args = parser.parse_args()

    env = read_env()
    kinds = [args.only] if args.only else ["outbound", "inbound"]
    for kind in kinds:
        source = build(kind, env)
        if args.dry_run:
            print(f"  {kind:9} -> собран, заглушек нет ({len(source)} символов), заливка пропущена")
        else:
            upload(kind, source, env)
    print("Готово. Сценарии в Voximplant соответствуют текущему коду репозитория.")


if __name__ == "__main__":
    main()
