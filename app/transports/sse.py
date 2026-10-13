"""Транспорт SSE (участник B).

ЗАГЛУШКА шага 1: оба адреса отвечают кодом 501 «не реализовано».

Что нужно сделать на шаге 2:
GET /sse/events
1. Вызвать hub.join(room, nick) прямо в функции, до начала стрима.
   При ValueError ответить 422, при NickTaken — 409.
2. Написать асинхронный генератор: в цикле ждать сообщение из
   subscriber.queue с тайм-аутом (asyncio.wait_for). Пришло сообщение —
   отдать строки "id: ...\n" (только для type == "message") и
   "data: <json>\n\n". Тайм-аут — отдать ": ping\n\n".
   Если subscriber.kicked, выйти из цикла.
3. В блоке finally генератора вызвать hub.leave(subscriber).
4. Вернуть StreamingResponse(генератор, media_type="text/event-stream").

POST /sse/send
1. Если hub.has(room, nick) вернул False, ответить 403.
2. Проверить текст через models.check_text, при ValueError ответить 422.
3. Вызвать hub.publish(room, models.make_message(...)).

Понадобятся: from app.core import models
             from app.core.hub import hub, NickTaken
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

# prefix добавляется ко всем адресам ниже: /events -> /sse/events.
router = APIRouter(prefix="/sse")


class SendBody(BaseModel):
    """Тело запроса POST /sse/send.

    FastAPI сам разбирает JSON и проверяет поля по этому классу.
    Если данные не подходят, клиент получает ответ 422.
    """

    room: str
    nick: str
    text: str
    cts: float | None = None


@router.get("/events")
async def sse_events(room: str = "", nick: str = ""):
    raise HTTPException(status_code=501, detail="SSE ещё не реализован")


@router.post("/send")
async def sse_send(body: SendBody):
    raise HTTPException(status_code=501, detail="SSE ещё не реализован")
