FROM python:3.13-slim

# Without this, Python fully block-buffers stdout when it isn't a TTY (true of
# any containerized deploy) -- print()-based logs (see telephony_session.py's
# send_json) can sit unflushed for minutes, and worse, arrive out of order
# relative to uvicorn's own access-log lines. Observed live on Railway: log
# lines for one call appeared over a minute late and out of sequence.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/ ./backend/
COPY frontend/ ./frontend/

WORKDIR /app/backend

CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
