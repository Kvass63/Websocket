"""Точка входа: создаёт приложение FastAPI и собирает его части.

Запуск: uvicorn app.main:app --reload
"""

import logging
import mimetypes
import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.core.hub import hub
from app.transports import sse, ws

logging.basicConfig(level=logging.INFO)

mimetypes.add_type("text/javascript", ".js")

WEB_DIR = Path(__file__).parent.parent / "web"

app = FastAPI(title="Чат: WebSocket и SSE")

app.include_router(ws.router)
app.include_router(sse.router)


@app.get("/api/health")
async def health():
    """Проверка, что сервер жив."""
    return {"status": "ok"}


@app.get("/api/stats")
async def stats():
    """Число подключений, размеры очередей и PID процесса (для бенчмарка и демо)."""
    data = hub.stats()
    data["pid"] = os.getpid()
    return data


app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")
