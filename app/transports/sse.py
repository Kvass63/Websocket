"""Транспорт SSE (участник B).

SSE (Server-Sent Events) — это обычный HTTP-ответ, который не заканчивается.
Сервер держит его открытым и дописывает в него события по мере появления:

    id: 123
    data: {"type": "message", ...}
    <пустая строка — конец события>

Строка, начинающаяся с двоеточия (": ping"), — комментарий. Браузер её
пропускает, а нужна она, чтобы соединение не закрыли по простою.

В обратную сторону (клиент -> сервер) SSE не работает, поэтому сообщения
отправляются отдельными запросами POST /sse/send.
"""

import asyncio
import json
import logging

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.core import models
from app.core.hub import NickTaken, hub

# Раз в столько секунд без сообщений отправляем ": ping".
PING_INTERVAL = 15

log = logging.getLogger("chat.sse")

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


# Последнее событие для медленного клиента перед закрытием стрима.
# У SSE нет кодов закрытия, как 4002 у WebSocket, поэтому причину
# сообщаем событием со своим именем. Строка data обязательна:
# событие без data браузер не передаёт в JavaScript.
KICKED_EVENT = "event: kicked\ndata: {}\n\n"


def format_event(message):
    """Превратить словарь-сообщение в текст одного события SSE."""
    # ensure_ascii=False: русские буквы идут как есть, а не как \u0440.
    # separators без пробелов: так же кодирует send_json у WebSocket,
    # иначе размер сообщений у двух транспортов будет нечестно разным.
    data = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
    if message["type"] == "message":
        # По полю id браузер при переподключении пришлёт Last-Event-ID.
        return f"id: {message['id']}\ndata: {data}\n\n"
    return f"data: {data}\n\n"


async def event_stream(request, subscriber):
    """Асинхронный генератор: каждый yield — кусок ответа, который сразу
    уходит клиенту. StreamingResponse крутит этот генератор, пока он
    не закончится или пока клиент не отключится.
    """
    try:
        while not subscriber.kicked:
            try:
                # Ждём сообщение из очереди, но не дольше PING_INTERVAL секунд.
                message = await asyncio.wait_for(subscriber.queue.get(), PING_INTERVAL)
            except asyncio.TimeoutError:
                # Сообщений давно не было: проверяем, жив ли клиент,
                # и шлём комментарий, чтобы соединение не простаивало.
                if await request.is_disconnected():
                    break
                yield ": ping\n\n"
                continue
            yield format_event(message)

        if subscriber.kicked:
            # Hub выкинул клиента как медленного. Оставшиеся в очереди
            # сообщения не отправляем, только причину, и завершаем стрим.
            log.info("SSE: закрываем стрим медленного клиента %s", subscriber.nick)
            yield KICKED_EVENT
    finally:
        # Сюда попадаем при любом выходе: клиент ушёл (генератор отменили),
        # клиента выкинули как медленного, сервер останавливается.
        # Для выкинутого клиента leave ничего не делает: Hub его уже убрал.
        hub.leave(subscriber)


@router.get("/events")
async def sse_events(request: Request, room: str = "", nick: str = ""):
    # join делаем здесь, до начала стрима: пока ответ не начался,
    # ещё можно вернуть код ошибки. После начала статус 200 уже отправлен.
    try:
        subscriber = hub.join(room, nick)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=f"некорректный ник или комната: {error}")
    except NickTaken:
        raise HTTPException(status_code=409, detail="ник в этой комнате уже занят")

    last_event_id = request.headers.get("last-event-id")
    if last_event_id:
        # Браузер переподключился и сообщил номер последнего полученного
        # сообщения. Истории на сервере пока нет, досылать нечего — только пишем в лог.
        log.info("SSE: %s переподключился, Last-Event-ID=%s", subscriber.nick, last_event_id)

    return StreamingResponse(
        event_stream(request, subscriber),
        media_type="text/event-stream",
        # no-cache: не сохранять ответ в кэше.
        # X-Accel-Buffering: просьба к прокси nginx не копить ответ в буфере.
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/check")
async def sse_check(room: str = "", nick: str = ""):
    """Объяснить, почему не удалось войти.

    EventSource не сообщает HTTP-статус отказа, поэтому после ошибки клиент
    спрашивает причину здесь. Проверки те же, что в hub.join, но в комнату
    этот запрос никого не добавляет.
    """
    try:
        room = models.check_room(room)
        nick = models.check_nick(nick)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=f"некорректный ник или комната: {error}")
    if hub.has(room, nick):
        raise HTTPException(status_code=409, detail="ник в этой комнате уже занят")
    return {"ok": True}


@router.post("/send")
async def sse_send(body: SendBody):
    # Обрезаем пробелы так же, как это делал hub.join при входе,
    # иначе " bob " не совпадёт с сохранённым "bob".
    try:
        room = models.check_room(body.room)
        nick = models.check_nick(body.nick)
        text = models.check_text(body.text)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error))

    # У SSE личность не привязана к соединению: ник приходит в каждом
    # запросе, поэтому проверяем, что такой участник действительно в комнате.
    if not hub.has(room, nick):
        raise HTTPException(status_code=403, detail="такого ника нет в комнате")

    hub.publish(room, models.make_message(room, nick, text, body.cts))
    return {"ok": True}
