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

# Чтобы сообщения из logging (например, об отключении медленного клиента)
# были видны в консоли сервера.
logging.basicConfig(level=logging.INFO)

# На Windows тип файлов .js берётся из реестра и на некоторых машинах
# равен text/plain. Браузер такие скрипты-модули не выполняет, поэтому
# задаём тип явно.
mimetypes.add_type("text/javascript", ".js")

# Папка web/ лежит рядом с папкой app/.
WEB_DIR = Path(__file__).parent.parent / "web"

app = FastAPI(title="Чат: WebSocket и SSE")

# Каждый транспорт описывает свои адреса в своём файле.
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


# Статика подключается последней: она отвечает на все адреса, которые
# не заняты выше. html=True значит, что по адресу "/" отдаётся index.html.
app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")
