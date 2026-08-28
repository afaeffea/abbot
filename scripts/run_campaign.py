#!/usr/bin/env python3
"""Reads a client list from Excel, dials each number through the already
deployed backend's /api/telephony/call (bounded concurrency, not the
sequential telephony_campaign.py module), and writes one clean JSONL row per
client to scripts/campaign_runs/<run_id>/results.jsonl -- meant purely as a
data foundation to aggregate later, not a dashboard.

Usage:
    python run_campaign.py clients.xlsx --backend-url https://abbot-production-7532.up.railway.app \
        --token "$TELEPHONY_WS_TOKEN" --concurrency 3 --label "prompt-v3"
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter

PHONE_NUMBER_RE = re.compile(r"^\+\d{7,15}$")
NAME_HEADERS = ("имя", "контакт", "фио", "name", "клиент")
# Priority order: some exports (e.g. Bitrix24 contact dumps) spread phone
# numbers across several columns per row with different fill rates -- take
# the first non-empty one per row, checking columns in this order.
PHONE_HEADERS = ("рабочий телефон", "мобильный телефон", "телефон", "phone", "номер", "другой телефон", "phone_number")

RUNS_DIR = Path(__file__).resolve().parent / "campaign_runs"


def normalize_phone(raw) -> str | None:
    # pandas.read_html parses numeric-looking phone columns as float, so
    # str(79295487944.0) is "79295487944.0" -- stripping the "." (not a
    # digit) below would leave a spurious trailing "0" digit and corrupt the
    # number. Convert back to int first so this matches the plain-string path.
    if isinstance(raw, float) and raw.is_integer():
        raw = int(raw)
    # A few cells hold multiple numbers separated by a comma/semicolon
    # (e.g. "+79817487917, +79112730331") -- take just the first.
    first = re.split(r"[,;]", str(raw).strip())[0].strip()
    digits = re.sub(r"[^\d+]", "", first)
    if digits.startswith("8") and len(digits) == 11:
        digits = "+7" + digits[1:]
    elif digits.startswith("7") and len(digits) == 11:
        digits = "+" + digits
    elif not digits.startswith("+") and len(digits) == 10:
        digits = "+7" + digits
    return digits if PHONE_NUMBER_RE.match(digits) else None


def _is_real_xlsx(path: Path) -> bool:
    # xlsx is a zip archive (starts with "PK") -- some CRM exports (e.g.
    # Bitrix24 contact dumps) are actually HTML tables saved with a .xls
    # extension, which openpyxl can't open. Sniff content instead of trusting
    # the extension so both kinds of export just work.
    with path.open("rb") as f:
        return f.read(2) == b"PK"


def _rows_from_xlsx(path: Path) -> tuple[list[str], list[tuple]]:
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    rows = ws.iter_rows(values_only=True)
    header = [str(h).strip().lower() if h else "" for h in next(rows)]
    return header, list(rows)


def _rows_from_html_table(path: Path) -> tuple[list[str], list[tuple]]:
    df = pd.read_html(path)[0]
    header = [str(h).strip().lower() for h in df.columns]
    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    return header, rows


def _find_col(header: list[str], candidates: tuple[str, ...]) -> int | None:
    for candidate in candidates:
        for i, h in enumerate(header):
            if candidate in h:
                return i
    return None


def _read_sheet(excel_path: Path) -> tuple[list[str], list[tuple], int, list[int]]:
    """Header, rows, and the resolved name/phone column positions. Shared by
    read_clients and write_results_xlsx so the results file is guaranteed to
    line its phone lookup up with whatever was actually dialed."""
    header, rows = _rows_from_xlsx(excel_path) if _is_real_xlsx(excel_path) else _rows_from_html_table(excel_path)

    name_col = _find_col(header, NAME_HEADERS)
    if name_col is None:
        raise ValueError(f"Не нашёл колонку с именем среди {NAME_HEADERS} в заголовке {header}")

    # Collect every column that could hold a phone number, in priority order
    # -- a row picks its first non-empty match, since which column is filled
    # varies per contact in some exports.
    phone_cols = []
    for candidate in PHONE_HEADERS:
        for i, h in enumerate(header):
            if candidate in h and i not in phone_cols:
                phone_cols.append(i)
    if not phone_cols:
        raise ValueError(f"Не нашёл колонку с телефоном среди {PHONE_HEADERS} в заголовке {header}")

    return header, rows, name_col, phone_cols


def _is_blank(value) -> bool:
    return value is None or value == "" or (isinstance(value, float) and pd.isna(value))


def _row_phone(row: tuple, phone_cols: list[int]) -> str | None:
    raw = next((row[i] for i in phone_cols if i < len(row) and not _is_blank(row[i])), None)
    return normalize_phone(raw) if raw else None


def read_clients(excel_path: Path) -> list[dict]:
    header, rows, name_col, phone_cols = _read_sheet(excel_path)

    clients = []
    seen_phones: set[str] = set()
    for row in rows:
        if row is None or all(v is None or (isinstance(v, float) and pd.isna(v)) for v in row):
            continue
        name = str(row[name_col]).strip() if row[name_col] and not (isinstance(row[name_col], float) and pd.isna(row[name_col])) else ""
        raw_phone = next(
            (row[i] for i in phone_cols if row[i] not in (None, "") and not (isinstance(row[i], float) and pd.isna(row[i]))),
            None,
        )
        phone = normalize_phone(raw_phone) if raw_phone else None

        # Some exports (e.g. a deals/purchases dump) have one row per
        # transaction rather than one row per client -- the same phone
        # repeats many times. Keep only the first sighting of each number so
        # we don't dial the same person several times in one campaign.
        if phone:
            if phone in seen_phones:
                continue
            seen_phones.add(phone)

        clients.append({"name": name, "raw_phone": raw_phone, "phone_number": phone})
    return clients


# Appended to the right of the source export's own columns, in this order.
RESULT_COLUMNS = (
    "квалификация",
    "итог разговора",
    "дозвон",
    "длительность, с",
    "реплик",
    "время звонка",
    "call_id",
)

QUALIFICATION_LABELS = {"yes": "готов", "no": "отказ", "unclear": "не определился"}
DIAL_LABELS = {
    "dialed": "поговорили",
    "no_phone": "нет номера",
    "dial_failed": "не удалось набрать",
    "timeout": "не дождались результата",
}


def write_results_xlsx(excel_path: Path, run_id: str, results: list[dict]) -> Path:
    """One file per run, written next to the source export: every original
    row, plus what happened on that row's call.

    Deliberately NOT written back into the source file -- that file is a raw
    CRM export, so mutating it both destroys its provenance and gets silently
    clobbered by the next export. A separate per-run file also means two runs
    over the same list stay comparable instead of overwriting each other."""
    header, rows, _name_col, phone_cols = _read_sheet(excel_path)
    by_phone = {r["phone_number"]: r for r in results if r.get("phone_number")}

    wb = Workbook()
    ws = wb.active
    ws.title = "Результаты обзвона"
    ws.append([*(h or "" for h in header), *RESULT_COLUMNS])

    for row in rows:
        if row is None or all(_is_blank(v) for v in row):
            continue
        source_cells = ["" if _is_blank(v) else v for v in row]
        result = by_phone.get(_row_phone(row, phone_cols))
        if result is None:
            # A row whose number repeated earlier in the export (only the
            # first sighting is dialed) or that had no usable number at all.
            ws.append([*source_cells, *([""] * len(RESULT_COLUMNS))])
            continue
        ws.append([
            *source_cells,
            QUALIFICATION_LABELS.get(result.get("ready_to_buy"), result.get("ready_to_buy") or ""),
            result.get("summary") or "",
            DIAL_LABELS.get(result.get("dial_status"), result.get("dial_status") or ""),
            result.get("duration_seconds") or "",
            result.get("turn_count") or "",
            result.get("started_at") or "",
            result.get("call_id") or "",
        ])

    ws.freeze_panes = "A2"
    first_result_col = len(header) + 1
    ws.column_dimensions[get_column_letter(first_result_col)].width = 16
    ws.column_dimensions[get_column_letter(first_result_col + 1)].width = 70

    out_path = excel_path.parent / f"{excel_path.stem}_результаты_{run_id}.xlsx"
    wb.save(out_path)
    return out_path


async def dial(
    client: httpx.AsyncClient, backend_url: str, token: str, phone_number: str, client_name: str | None = None
) -> dict:
    payload = {"phone_number": phone_number}
    if client_name:
        payload["client_name"] = client_name
    resp = await client.post(
        f"{backend_url}/api/telephony/call",
        json=payload,
        headers={"X-Telephony-Token": token},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


async def fetch_calls(client: httpx.AsyncClient, backend_url: str) -> list[dict]:
    resp = await client.get(f"{backend_url}/api/calls", timeout=30)
    resp.raise_for_status()
    return resp.json()


async def fetch_call_transcript(client: httpx.AsyncClient, backend_url: str, call_id: str) -> dict:
    resp = await client.get(f"{backend_url}/api/calls/{call_id}", timeout=30)
    resp.raise_for_status()
    return resp.json()


def append_result(results_path: Path, row: dict) -> None:
    with results_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


async def poller(client: httpx.AsyncClient, backend_url: str, interval: float, latest: dict, stop: asyncio.Event):
    while not stop.is_set():
        try:
            calls = await fetch_calls(client, backend_url)
            by_phone: dict[str, list[dict]] = {}
            for call in calls:
                phone = call.get("phone_number")
                if phone:
                    by_phone.setdefault(phone, []).append(call)
            latest.clear()
            latest.update(by_phone)
        except Exception as exc:
            print(f"[poller] Ошибка опроса /api/calls: {exc}")
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


async def run_client(
    entry: dict,
    sem: asyncio.Semaphore,
    client: httpx.AsyncClient,
    backend_url: str,
    token: str,
    latest: dict,
    claimed_call_ids: set,
    timeout: float,
    results: list,
    results_lock: asyncio.Lock,
    progress: dict,
    results_path: Path,
    transcripts_dir: Path,
):
    name = entry["name"]
    phone_number = entry["phone_number"]

    async def finish(row: dict) -> None:
        # Written immediately, not batched to the end of the whole campaign --
        # a crash/Ctrl-C mid-run must not lose results already in hand.
        async with results_lock:
            results.append(row)
            append_result(results_path, row)
        progress["done"] += 1

    if not phone_number:
        await finish({
            "name": name, "phone_number": None, "dial_status": "invalid_phone",
            "call_id": None, "ready_to_buy": None, "summary": None,
            "duration_seconds": None, "turn_count": None, "transcript_path": None,
            "started_at": None, "finished_at": None,
        })
        print(f"[{progress['done']}/{progress['total']}] {name}: пропущен, невалидный номер ({entry['raw_phone']!r})")
        return

    dispatched_at = datetime.now(timezone.utc).isoformat()
    async with sem:
        try:
            await dial(client, backend_url, token, phone_number, name)
        except Exception as exc:
            await finish({
                "name": name, "phone_number": phone_number, "dial_status": f"dial_error: {exc}",
                "call_id": None, "ready_to_buy": None, "summary": None,
                "duration_seconds": None, "turn_count": None, "transcript_path": None,
                "started_at": dispatched_at, "finished_at": None,
            })
            print(f"[{progress['done']}/{progress['total']}] {name} ({phone_number}): ошибка дозвона — {exc}")
            return

        print(f"[.] {name} ({phone_number}): набран, жду результат…")

        deadline = asyncio.get_event_loop().time() + timeout
        match = None
        while asyncio.get_event_loop().time() < deadline:
            candidates = [
                c for c in latest.get(phone_number, [])
                if c.get("call_id") not in claimed_call_ids and (c.get("started_at") or "") >= dispatched_at
            ]
            if candidates:
                match = max(candidates, key=lambda c: c.get("started_at") or "")
                break
            await asyncio.sleep(1)

        if match:
            claimed_call_ids.add(match["call_id"])
            call_id = match.get("call_id")
            outcome = match.get("outcome") or {}

            # Pull the full transcript down to our own disk right now, while
            # it's still there -- backend/call_logs/ lives on Railway's
            # ephemeral filesystem and is wiped on the next redeploy, so a
            # path reference alone would go stale.
            transcript_path = None
            try:
                transcript = await fetch_call_transcript(client, backend_url, call_id)
                transcripts_dir.mkdir(parents=True, exist_ok=True)
                local_path = transcripts_dir / f"{call_id}.json"
                local_path.write_text(json.dumps(transcript, ensure_ascii=False, indent=2), encoding="utf-8")
                transcript_path = str(local_path.relative_to(results_path.parent.parent))
            except Exception as exc:
                print(f"[!] {name} ({phone_number}): не удалось скачать транскрипт {call_id} — {exc}")

            row = {
                "name": name, "phone_number": phone_number, "dial_status": "dialed",
                "call_id": call_id,
                "ready_to_buy": outcome.get("ready_to_buy"),
                "summary": outcome.get("summary"),
                "duration_seconds": match.get("duration_seconds"),
                "turn_count": match.get("turn_count"),
                "transcript_path": transcript_path,
                "started_at": dispatched_at,
                "finished_at": datetime.now(timezone.utc).isoformat(),
            }
            status_bit = f"ready_to_buy={row['ready_to_buy']}" if row["ready_to_buy"] else "без вердикта"
            print(f"[+] {name} ({phone_number}): завершён — {status_bit}")
        else:
            row = {
                "name": name, "phone_number": phone_number, "dial_status": "no_match_timeout",
                "call_id": None, "ready_to_buy": None, "summary": None,
                "duration_seconds": None, "turn_count": None, "transcript_path": None,
                "started_at": dispatched_at, "finished_at": None,
            }
            print(f"[!] {name} ({phone_number}): не дождался результата за {timeout:.0f}с")

    await finish(row)


async def main_async(args: argparse.Namespace) -> None:
    clients = read_clients(Path(args.excel))
    if not clients:
        print("В файле не найдено ни одной строки с клиентами.")
        return
    if args.limit is not None:
        clients = clients[: args.limit]

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    run_meta = {
        "run_id": run_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "excel_source": str(Path(args.excel).resolve()),
        "backend_url": args.backend_url,
        "concurrency": args.concurrency,
        "total_clients": len(clients),
        "label": args.label,
    }
    (run_dir / "run_meta.json").write_text(json.dumps(run_meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Прогон {run_id}: {len(clients)} клиентов, конкурентность {args.concurrency}")

    results_path = run_dir / "results.jsonl"
    results_path.touch()
    transcripts_dir = run_dir / "transcripts"

    latest: dict = {}
    claimed_call_ids: set = set()
    results: list = []
    results_lock = asyncio.Lock()
    progress = {"done": 0, "total": len(clients)}
    stop_poller = asyncio.Event()
    sem = asyncio.Semaphore(args.concurrency)

    async with httpx.AsyncClient() as client:
        poller_task = asyncio.create_task(poller(client, args.backend_url, args.poll_interval, latest, stop_poller))
        try:
            await asyncio.gather(*(
                run_client(
                    entry, sem, client, args.backend_url, args.token,
                    latest, claimed_call_ids, args.timeout, results, results_lock, progress,
                    results_path, transcripts_dir,
                )
                for entry in clients
            ))
        finally:
            stop_poller.set()
            await poller_task

    resolved = [r for r in results if r["dial_status"] == "dialed"]
    yes = sum(1 for r in resolved if r["ready_to_buy"] == "yes")
    no = sum(1 for r in resolved if r["ready_to_buy"] == "no")
    unclear = sum(1 for r in resolved if r["ready_to_buy"] == "unclear")
    print(
        f"\nГотово: {len(resolved)}/{len(clients)} дозвонились, "
        f"из них готовы купить: {yes}, нет: {no}, непонятно: {unclear}."
    )
    print(f"Результаты: {results_path}")
    try:
        xlsx_path = write_results_xlsx(Path(args.excel), run_id, results)
        print(f"Таблица:    {xlsx_path}")
    except Exception as exc:
        # results.jsonl is already safely on disk at this point -- a
        # formatting problem in the spreadsheet must not look like the whole
        # campaign's data was lost.
        print(f"Не смог собрать xlsx с результатами ({exc}). Данные целы в {results_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("excel", help="Путь к Excel-файлу со списком клиентов (колонки: имя, телефон)")
    parser.add_argument("--backend-url", required=True, help="Например https://abbot-production-7532.up.railway.app")
    parser.add_argument("--token", default=os.environ.get("TELEPHONY_WS_TOKEN"),
                         help="X-Telephony-Token (по умолчанию из env TELEPHONY_WS_TOKEN)")
    parser.add_argument("--concurrency", type=int, default=3, help="Сколько звонков идёт одновременно")
    parser.add_argument("--timeout", type=float, default=300, help="Сколько ждать результата одного звонка, сек")
    parser.add_argument("--poll-interval", type=float, default=5, help="Как часто опрашивать /api/calls, сек")
    parser.add_argument("--label", default=None, help="Метка прогона (например версия промпта) для сравнения позже")
    parser.add_argument("--limit", type=int, default=None, help="Обзвонить только первые N клиентов из файла (для пилотов)")
    args = parser.parse_args()

    if not args.token:
        parser.error("Нужен --token или переменная окружения TELEPHONY_WS_TOKEN")

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
