import asyncio
import json
import logging

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.core import models
from app.core.hub import NickTaken, hub

PING_INTERVAL = 15

log = logging.getLogger("chat.sse")

router = APIRouter(prefix="/sse")


class SendBody(BaseModel):
    room: str
    nick: str
    text: str
    cts: float | None = None


KICKED_EVENT = "event: kicked\ndata: {}\n\n"


def format_event(message):
    data = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
    if message["type"] == "message":
        return f"id: {message['id']}\ndata: {data}\n\n"
    return f"data: {data}\n\n"


async def event_stream(request, subscriber):
    try:
        while not subscriber.kicked:
            try:
                message = await asyncio.wait_for(subscriber.queue.get(), PING_INTERVAL)
            except asyncio.TimeoutError:
                if await request.is_disconnected():
                    break
                yield ": ping\n\n"
                continue
            yield format_event(message)

        if subscriber.kicked:
            log.info("SSE: закрываем стрим медленного клиента %s", subscriber.nick)
            yield KICKED_EVENT
    finally:
        hub.leave(subscriber)


@router.get("/events")
async def sse_events(request: Request, room: str = "", nick: str = ""):
    try:
        subscriber = hub.join(room, nick)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=f"некорректный ник или комната: {error}")
    except NickTaken:
        raise HTTPException(status_code=409, detail="ник в этой комнате уже занят")

    last_event_id = request.headers.get("last-event-id")
    if last_event_id:
        log.info("SSE: %s переподключился, Last-Event-ID=%s", subscriber.nick, last_event_id)

    return StreamingResponse(
        event_stream(request, subscriber),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/check")
async def sse_check(room: str = "", nick: str = ""):
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
    try:
        room = models.check_room(body.room)
        nick = models.check_nick(body.nick)
        text = models.check_text(body.text)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error))

    if not hub.has(room, nick):
        raise HTTPException(status_code=403, detail="такого ника нет в комнате")

    hub.publish(room, models.make_message(room, nick, text, body.cts))
    return {"ok": True}
