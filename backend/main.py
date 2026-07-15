from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

from call_session import CallSession

app = FastAPI()

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


if FRONTEND_DIR:
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
else:
    print(f"[main] WARNING: frontend directory not found (checked {_FRONTEND_CANDIDATES}); "
          f"only /ws/call will work, static files will 404.")
