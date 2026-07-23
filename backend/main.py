import asyncio
import json
import re
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

import cartesia_client
import settings_store
import test_chat
from call_recorder import CALL_LOGS_DIR
from call_session import CallSession

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
