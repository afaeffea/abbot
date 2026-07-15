from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

from call_session import CallSession

app = FastAPI()

FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"


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


app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
