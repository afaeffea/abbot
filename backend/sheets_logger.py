from __future__ import annotations

import asyncio
import json
import os

# Each cached only on success — see _get_spreadsheet / _get_main_worksheet /
# _get_bookings_worksheet. A transient failure is never cached, so the next
# call retries from scratch.
_cached_spreadsheet = None
_cached_main_worksheet = None
_cached_bookings_worksheet = None

BOOKINGS_SHEET_TITLE = "Успешные брони"
BOOKINGS_HEADER = ["Начало звонка", "Услуга", "Дата", "Время", "Количество человек", "ID звонка"]


def _get_spreadsheet():
    """Opens (and caches) the spreadsheet itself. Note: open_by_key does one
    metadata fetch, but .sheet1 / .worksheet(...) each do their OWN fresh
    fetch on every access regardless of this cache — see _get_main_worksheet
    and _get_bookings_worksheet, which cache those separately."""
    global _cached_spreadsheet
    if _cached_spreadsheet is not None:
        return _cached_spreadsheet

    spreadsheet_id = os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    creds_raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not spreadsheet_id or not creds_raw:
        return None  # feature not configured — cheap check, no need to cache "unconfigured"

    try:
        import gspread  # deferred: a missing/broken dependency must not crash app startup

        creds_dict = json.loads(creds_raw)
        gc = gspread.service_account_from_dict(creds_dict)
        _cached_spreadsheet = gc.open_by_key(spreadsheet_id)
    except Exception as exc:
        print(f"[sheets_logger] Sheets logging unavailable, skipping: {exc}")
        return None  # not cached — a transient failure gets retried on the next call
    return _cached_spreadsheet


def _get_main_worksheet():
    """General call log (sheet1) — every call, booked or not. Unchanged from
    before the bookings tab existed."""
    global _cached_main_worksheet
    if _cached_main_worksheet is not None:
        return _cached_main_worksheet

    sh = _get_spreadsheet()
    if sh is None:
        return None

    try:
        _cached_main_worksheet = sh.sheet1
    except Exception as exc:
        print(f"[sheets_logger] Could not open main worksheet, skipping: {exc}")
        return None  # not cached — retried on the next call
    return _cached_main_worksheet


def _get_bookings_worksheet():
    """The 'successful bookings only' tab. Created (with a header row) the
    first time it's needed — unlike sheet1, this tab is meant for a human to
    read directly, so it can't just start out empty/unlabeled."""
    global _cached_bookings_worksheet
    if _cached_bookings_worksheet is not None:
        return _cached_bookings_worksheet

    sh = _get_spreadsheet()
    if sh is None:
        return None

    try:
        import gspread  # deferred, same reasoning as _get_spreadsheet

        try:
            worksheet = sh.worksheet(BOOKINGS_SHEET_TITLE)
        except gspread.WorksheetNotFound:
            try:
                worksheet = sh.add_worksheet(
                    title=BOOKINGS_SHEET_TITLE, rows=1000, cols=len(BOOKINGS_HEADER)
                )
                worksheet.append_row(BOOKINGS_HEADER)
            except gspread.APIError:
                # Lost a race with another concurrent call that created the
                # tab first (Sheets rejects a duplicate title) — it exists
                # now, so just look it up instead of treating this as a
                # real failure.
                worksheet = sh.worksheet(BOOKINGS_SHEET_TITLE)
    except Exception as exc:
        print(f"[sheets_logger] Could not open/create bookings worksheet, skipping: {exc}")
        return None  # not cached — retried on the next call

    _cached_bookings_worksheet = worksheet
    return _cached_bookings_worksheet


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


def _booking_row_from_summary(summary: dict) -> list:
    outcome = summary.get("outcome") or {}
    return [
        summary.get("started_at"),
        outcome.get("service", ""),
        outcome.get("date", ""),
        outcome.get("time", ""),
        outcome.get("people_count", ""),
        summary.get("call_id"),
    ]


async def append_call_row(summary: dict) -> None:
    def _append_main():
        worksheet = _get_main_worksheet()
        if worksheet is not None:
            worksheet.append_row(_row_from_summary(summary))

    def _append_booking():
        outcome = summary.get("outcome") or {}
        if not outcome.get("booked"):
            return
        worksheet = _get_bookings_worksheet()
        if worksheet is not None:
            worksheet.append_row(_booking_row_from_summary(summary))

    def _append_both():
        # Each half is isolated: a bookings-tab failure (e.g. a permission
        # issue creating the new tab) must never undo/block the sheet1 row
        # already written above, and neither half may ever raise past this
        # function.
        try:
            _append_main()
        except Exception as exc:
            print(f"[sheets_logger] Failed to append row for call {summary.get('call_id')}: {exc}")

        try:
            _append_booking()
        except Exception as exc:
            print(f"[sheets_logger] Failed to append booking row for call {summary.get('call_id')}: {exc}")

    try:
        await asyncio.to_thread(_append_both)
    except Exception as exc:
        print(f"[sheets_logger] Failed to append row for call {summary.get('call_id')}: {exc}")
