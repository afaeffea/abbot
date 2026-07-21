from __future__ import annotations

import asyncio
import json
import os

_cached_worksheet = None  # only ever cached on success — see _build_worksheet


def _build_worksheet():
    global _cached_worksheet
    if _cached_worksheet is not None:
        return _cached_worksheet

    spreadsheet_id = os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    creds_raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not spreadsheet_id or not creds_raw:
        return None  # feature not configured — cheap check, no need to cache "unconfigured"

    try:
        import gspread  # deferred: a missing/broken dependency must not crash app startup

        creds_dict = json.loads(creds_raw)
        gc = gspread.service_account_from_dict(creds_dict)
        sh = gc.open_by_key(spreadsheet_id)
        _cached_worksheet = sh.sheet1
    except Exception as exc:
        print(f"[sheets_logger] Sheets logging unavailable, skipping: {exc}")
        return None  # not cached — a transient failure gets retried on the next call
    return _cached_worksheet


def _row_from_summary(summary: dict) -> list:
    outcome = summary.get("outcome") or {}
    booked = bool(outcome.get("booked"))
    return [
        summary.get("started_at"),
        summary.get("duration_seconds"),
        "Да" if booked else "Нет",
        outcome.get("service", "") if booked else "",
        outcome.get("date", "") if booked else "",
        outcome.get("time", "") if booked else "",
        outcome.get("people_count", "") if booked else "",
        summary.get("call_id"),
        summary.get("turn_count"),
    ]


async def append_call_row(summary: dict) -> None:
    def _append():
        worksheet = _build_worksheet()
        if worksheet is None:
            return
        worksheet.append_row(_row_from_summary(summary))

    try:
        await asyncio.to_thread(_append)
    except Exception as exc:
        print(f"[sheets_logger] Failed to append row for call {summary.get('call_id')}: {exc}")
