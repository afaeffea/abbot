import json
import re
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

from call_recorder import CALL_LOGS_DIR
from call_session import CallSession
from chat_session import ChatSession

app = FastAPI()

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


@app.websocket("/ws/chat")
async def chat_ws(websocket: WebSocket):
    await websocket.accept()
    session = ChatSession(websocket)
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


if FRONTEND_DIR:
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
else:
    print(f"[main] WARNING: frontend directory not found (checked {_FRONTEND_CANDIDATES}); "
          f"only /ws/call will work, static files will 404.")
