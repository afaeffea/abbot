#!/usr/bin/env python3
"""Reads results.jsonl from one run_campaign.py run and writes results.xlsx
in the same directory -- columns plus simple aggregate counts underneath,
no charts/dashboard.

Usage:
    python export_funnel.py scripts/campaign_runs/<run_id>
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from openpyxl import Workbook

COLUMNS = [
    "name", "phone_number", "dial_status", "ready_to_buy", "summary",
    "duration_seconds", "turn_count", "started_at", "finished_at",
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("run_dir", help="Например scripts/campaign_runs/20260827-101500")
    args = parser.parse_args()

    run_dir = Path(args.run_dir)
    results_path = run_dir / "results.jsonl"
    if not results_path.is_file():
        parser.error(f"Не найден {results_path}")

    rows = [json.loads(line) for line in results_path.read_text(encoding="utf-8").splitlines() if line.strip()]

    wb = Workbook()
    ws = wb.active
    ws.title = "results"
    ws.append(COLUMNS)
    for row in rows:
        ws.append([row.get(col) for col in COLUMNS])

    dialed = [r for r in rows if r["dial_status"] == "dialed"]
    yes = sum(1 for r in dialed if r["ready_to_buy"] == "yes")
    no = sum(1 for r in dialed if r["ready_to_buy"] == "no")
    unclear = sum(1 for r in dialed if r["ready_to_buy"] == "unclear")

    ws.append([])
    ws.append(["Всего клиентов", len(rows)])
    ws.append(["Дозвонились", len(dialed)])
    ws.append(["Готовы (yes)", yes])
    ws.append(["Не готовы (no)", no])
    ws.append(["Непонятно (unclear)", unclear])

    out_path = run_dir / "results.xlsx"
    wb.save(out_path)
    print(f"Записано {out_path}")
    print(
        f"Всего: {len(rows)}, дозвонились: {len(dialed)}, "
        f"готовы: {yes}, нет: {no}, непонятно: {unclear}"
    )


if __name__ == "__main__":
    main()
