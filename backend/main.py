from __future__ import annotations

import asyncio
import json
import os
import re
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

import cartesia_client
import settings_store
import telephony_campaign
import telephony_probe
import test_chat
import voximplant_client
from call_recorder import CALL_LOGS_DIR
from call_session import CallSession
from telephony_session import TelephonySession

app = FastAPI()


@app.middleware("http")
async def no_cache_frontend(request, call_next):
    # Without this, browsers (Safari in particular) can keep serving a stale
    # index.html/app.js/mic-worklet.js from disk cache after a redeploy,
    # showing old UI/logic until the user notices something's off and
    # force-reloads. no-store (not just no-cache) because a plain reload was
    # still observed serving stale content — no-cache still permits the
    # browser to reuse a cached response after a lightweight revalidation,
    # no-store forbids reusing it at all.
    response = await call_next(request)
    path = request.url.path
    if path == "/" or path.endswith((".html", ".js", ".css")):
        response.headers["Cache-Control"] = "no-store, must-revalidate"
    return response


CALL_ID_RE = re.compile(r"^\d{8}-\d{6}-[0-9a-f]{6}$")
PHONE_NUMBER_RE = re.compile(r"^\+\d{7,15}$")  # E.164
# Safety cap, not a real capacity limit -- guards against pasting a huge list
# into the campaign textarea by mistake and accidentally billing hundreds of
# calls at once.
MAX_CAMPAIGN_NUMBERS = 200
# Also just a safety cap -- the real ceiling is whatever the operator's
# Cartesia plan allows concurrently (TTS concurrency is the tightest limit,
# e.g. 2 on Cartesia's Free tier, since telephony TTS goes through
# Voximplant's connector using the same account/key), not something this
# backend can see or enforce. See /campaign.html's hint text.
MAX_CAMPAIGN_CONCURRENCY = 10


def _call_dir(call_id: str) -> Path:
    if not CALL_ID_RE.match(call_id):
        raise HTTPException(status_code=404, detail="Not found")
    return CALL_LOGS_DIR / call_id


_BACKEND_DIR = Path(__file__).resolve().parent
_FRONTEND_CANDIDATES = [
    _BACKEND_DIR.parent / "frontend",  # repo-root layout: backend/main.py -> ../frontend
    _BACKEND_DIR / "frontend",  # flattened deploy image: main.py and frontend/ as siblings
]
FRONTEND_DIR = next((p for p in _FRONTEND_CANDIDATES if p.is_dir()), None)


@app.websocket("/ws/call")
async def call_ws(websocket: WebSocket):
    await websocket.accept()
    session = CallSession(websocket)
    try:
        await session.run()
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        try:
            await session.send_json({"type": "error", "text": f"Сбой сессии: {exc}"})
        except Exception:
            pass


@app.websocket("/ws/telephony")
async def telephony_ws(websocket: WebSocket):
    # Voximplant's VoxEngine scenario is the only expected caller here (see
    # voximplant/modules/bridge.voxengine.js) -- reject before accept() if the
    # shared secret doesn't match, so a stray/malicious connection never gets
    # a chance to stream audio through a real Cartesia/LLM pipeline on our tab.
    expected_token = os.environ.get("TELEPHONY_WS_TOKEN")
    if not expected_token or websocket.query_params.get("token") != expected_token:
        await websocket.close(code=4401)
        return

    # call_id, if present, is used only to label logs/recordings for this call
    # -- see telephony_session.py.
    call_id = websocket.query_params.get("call_id")
    # phone_number, if present, is the dialed number for an outbound call
    # (see outbound.voxengine.js/bridge.voxengine.js) -- absent for inbound
    # calls, where there's no "dialed number" to attach.
    phone_number = websocket.query_params.get("phone_number")
    # client_name, if present, personalizes the greeting/system prompt for
    # outbound calls (see run_campaign.py) -- absent for inbound calls.
    client_name = websocket.query_params.get("client_name")

    await websocket.accept()
    session = TelephonySession(websocket, call_id=call_id, phone_number=phone_number, client_name=client_name)
    try:
        await session.run()
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        print(f"[main] Telephony session error: {exc}")


@app.websocket("/ws/telephony_probe")
async def telephony_probe_ws(websocket: WebSocket):
    # Diagnostic-only sibling of /ws/telephony: plays a numbered beep sequence,
    # each variant in a different wire format, so one real call can identify
    # which format VoxEngine's ws->call media route actually accepts. See
    # telephony_probe.py. Same shared secret as the real endpoint -- it places
    # no calls itself, but it does answer them.
    expected_token = os.environ.get("TELEPHONY_WS_TOKEN")
    if not expected_token or websocket.query_params.get("token") != expected_token:
        await websocket.close(code=4401)
        return

    call_id = websocket.query_params.get("call_id") or "probe"
    await websocket.accept()
    try:
        await telephony_probe.run_probe(websocket, call_id)
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        print(f"[main] Telephony probe error: {exc}")


def _check_telephony_token(x_telephony_token: str | None) -> None:
    # Unlike the rest of this app (deliberately no auth anywhere else, see
    # README), these endpoints place real, billed phone calls -- so this is
    # the one shared secret check in the whole site.
    expected_token = os.environ.get("TELEPHONY_WS_TOKEN")
    if not expected_token or x_telephony_token != expected_token:
        raise HTTPException(status_code=401, detail="Неверный или отсутствующий X-Telephony-Token")


@app.post("/api/telephony/call")
async def post_telephony_call(payload: dict, x_telephony_token: str = Header(default=None)):
    _check_telephony_token(x_telephony_token)

    phone_number = (payload.get("phone_number") or "").strip()
    if not PHONE_NUMBER_RE.match(phone_number):
        raise HTTPException(status_code=400, detail="phone_number должен быть в формате E.164, например +79991234567")
    client_name = (payload.get("client_name") or "").strip() or None

    try:
        result = await voximplant_client.start_outbound_call(phone_number, client_name)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Voximplant: {exc}")
    return {"status": "started", "voximplant_response": result}


@app.post("/api/telephony/campaign")
async def post_telephony_campaign(payload: dict, x_telephony_token: str = Header(default=None)):
    _check_telephony_token(x_telephony_token)

    if telephony_campaign.is_running():
        raise HTTPException(status_code=409, detail="Обзвон уже идёт")

    raw_numbers = payload.get("phone_numbers")
    if not isinstance(raw_numbers, list) or not raw_numbers:
        raise HTTPException(status_code=400, detail="phone_numbers должен быть непустым списком")
    if len(raw_numbers) > MAX_CAMPAIGN_NUMBERS:
        raise HTTPException(status_code=400, detail=f"Не больше {MAX_CAMPAIGN_NUMBERS} номеров за один обзвон")

    phone_numbers = [str(n).strip() for n in raw_numbers]
    invalid = [n for n in phone_numbers if not PHONE_NUMBER_RE.match(n)]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"Неверный формат номера (нужен E.164, например +79991234567): {', '.join(invalid)}",
        )

    delay_seconds = payload.get("delay_seconds", 5)
    if not isinstance(delay_seconds, (int, float)) or not (2 <= delay_seconds <= 600):
        raise HTTPException(status_code=400, detail="delay_seconds должен быть числом от 2 до 600")

    concurrency = payload.get("concurrency", 1)
    if not isinstance(concurrency, int) or not (1 <= concurrency <= MAX_CAMPAIGN_CONCURRENCY):
        raise HTTPException(
            status_code=400,
            detail=f"concurrency должен быть целым числом от 1 до {MAX_CAMPAIGN_CONCURRENCY}",
        )

    telephony_campaign.start(phone_numbers, delay_seconds, concurrency)
    return {"status": "started", "total": len(phone_numbers)}


@app.get("/api/telephony/campaign")
async def get_telephony_campaign(x_telephony_token: str = Header(default=None)):
    _check_telephony_token(x_telephony_token)
    return telephony_campaign.get_state()


@app.get("/api/calls")
async def list_calls():
    calls = []
    if CALL_LOGS_DIR.is_dir():
        for transcript_path in CALL_LOGS_DIR.glob("*/transcript.json"):
            try:
                data = json.loads(transcript_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            calls.append({
                "call_id": data.get("call_id"),
                "started_at": data.get("started_at"),
                "duration_seconds": data.get("duration_seconds"),
                "outcome": data.get("outcome"),
                "turn_count": len(data.get("turns", [])),
                "phone_number": data.get("phone_number"),
                "client_name": data.get("client_name"),
            })
    calls.sort(key=lambda c: c["started_at"] or "", reverse=True)
    return calls


@app.get("/api/calls/{call_id}")
async def get_call(call_id: str):
    transcript_path = _call_dir(call_id) / "transcript.json"
    if not transcript_path.is_file():
        raise HTTPException(status_code=404, detail="Not found")
    return Response(content=transcript_path.read_bytes(), media_type="application/json")


@app.get("/api/calls/{call_id}/audio")
async def get_call_audio(call_id: str):
    audio_path = _call_dir(call_id) / "recording.wav"
    if not audio_path.is_file():
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(audio_path, media_type="audio/wav")


def _settings_state(current: dict) -> dict:
    return {
        "settings": current,
        "defaults": settings_store.DEFAULT_SETTINGS,
        "is_customized": current != settings_store.DEFAULT_SETTINGS,
    }


@app.get("/api/settings")
async def get_settings():
    return _settings_state(await asyncio.to_thread(settings_store.get_current_settings))


@app.put("/api/settings")
async def put_settings(payload: dict):
    try:
        normalized = settings_store.normalize_settings(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    await asyncio.to_thread(settings_store.save_settings, normalized)
    return _settings_state(await asyncio.to_thread(settings_store.get_current_settings))


@app.post("/api/settings/reset")
async def post_settings_reset():
    await asyncio.to_thread(settings_store.reset_settings)
    return _settings_state(await asyncio.to_thread(settings_store.get_current_settings))


@app.get("/api/voices")
async def get_voices():
    try:
        voices = await cartesia_client.list_voices()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Не удалось получить список голосов: {exc}")
    return {"voices": voices}


@app.post("/api/settings/test-chat")
async def post_test_chat(payload: dict):
    try:
        persona_prompt = settings_store.validate_nonempty_prompt_field(payload.get("persona_prompt"), "persona_prompt")
        business_facts = settings_store.validate_nonempty_prompt_field(payload.get("business_facts"), "business_facts")
        reasoning_effort = settings_store.validate_reasoning_effort(payload.get("reasoning_effort"))
        temperature = settings_store.validate_temperature(payload.get("temperature"))
        max_reply_tokens = settings_store.validate_max_reply_tokens(payload.get("max_reply_tokens"))
        history = test_chat.validate_history(payload.get("history"))
        message = test_chat.validate_message(payload.get("message"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    try:
        return await test_chat.run_test_turn(
            persona_prompt=persona_prompt,
            business_facts=business_facts,
            reasoning_effort=reasoning_effort,
            temperature=temperature,
            max_reply_tokens=max_reply_tokens,
            history=history,
            message=message,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Модель недоступна: {exc}")


if FRONTEND_DIR:
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
else:
    print(f"[main] WARNING: frontend directory not found (checked {_FRONTEND_CANDIDATES}); "
          f"only /ws/call will work, static files will 404.")
