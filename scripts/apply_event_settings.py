#!/usr/bin/env python3
"""Switches the backend's global bot prompt (greeting/persona_prompt/
business_facts) to one event's texts before running a campaign for it, via
the existing GET/PUT /api/settings (backend/settings_store.py).

!!! IMPORTANT !!!
backend/settings_override.json lives on Railway's ephemeral filesystem, same
disk as backend/call_logs/ -- both are wiped on the next redeploy (a plain
process restart within the same deploy is fine, a `git push`/redeploy is
not). Once you've applied an event's settings and started dialing, do NOT
push/redeploy the backend until that campaign is completely finished, or the
bot will silently revert to DEFAULT_SETTINGS (the generic Warpoint booking
assistant) mid-campaign.

Usage:
    python apply_event_settings.py data/01_09 --backend-url https://abbot-production-7532.up.railway.app
"""
from __future__ import annotations

import argparse
from pathlib import Path

import httpx


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("event_dir", help="Например data/01_09 -- должен содержать greeting.txt, persona_prompt.txt, business_facts.txt; voice_id.txt -- опционально")
    parser.add_argument("--backend-url", required=True, help="Например https://abbot-production-7532.up.railway.app")
    args = parser.parse_args()

    event_dir = Path(args.event_dir)
    overrides = {}
    for field in ("greeting", "persona_prompt", "business_facts"):
        path = event_dir / f"{field}.txt"
        if not path.is_file():
            parser.error(f"Не найден файл {path}")
        overrides[field] = path.read_text(encoding="utf-8").strip()

    # Optional, and applied by the same run for a reason: the voice's gender
    # has to agree with the persona's ("Ты — Дарья... говори о себе в женском
    # роде"), so shipping them separately means one redeploy can leave a
    # female script being read by a male voice. settings_override.json is
    # wiped on every redeploy just like the prompts are, so a voice set once
    # by hand would silently revert while the prompt was restored.
    voice_path = event_dir / "voice_id.txt"
    if voice_path.is_file():
        overrides["voice_id"] = voice_path.read_text(encoding="utf-8").strip()

    with httpx.Client(timeout=30) as client:
        current = client.get(f"{args.backend_url}/api/settings").json()["settings"]
        current.update(overrides)
        resp = client.put(f"{args.backend_url}/api/settings", json=current)
        resp.raise_for_status()

    print(f"Настройки бэкенда переключены на {event_dir}:")
    for field, value in overrides.items():
        preview = value.splitlines()[0][:80] if value else "(пусто)"
        print(f"  {field}: {preview}...")


if __name__ == "__main__":
    main()
