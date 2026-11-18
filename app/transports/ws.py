import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.core import models
from app.core.hub import NickTaken, hub

router = APIRouter()


@router.websocket("/ws")
async def ws_endpoint(websocket: WebSocket, room: str = "", nick: str = ""):
    await websocket.accept()

    try:
        subscriber = hub.join(room, nick)
    except NickTaken:
        await websocket.close(code=4001, reason="ник уже занят в этой комнате")
        return
    except ValueError:
        await websocket.close(code=4000, reason="некорректный ник или название комнаты")
        return

    receiver = asyncio.create_task(receive_loop(websocket, subscriber))
    sender = asyncio.create_task(send_loop(websocket, subscriber))
    try:
        done, pending = await asyncio.wait(
            [receiver, sender], return_when=asyncio.FIRST_COMPLETED
        )
        for task in done:
            task.result()
    finally:
        receiver.cancel()
        sender.cancel()
        hub.leave(subscriber)


async def receive_loop(websocket, subscriber):
    while True:
        try:
            raw = await websocket.receive_text()
        except WebSocketDisconnect:
            return

        if subscriber.kicked:
            continue

        try:
            data = json.loads(raw)
        except ValueError:
            continue
        if not isinstance(data, dict):
            continue
        try:
            text = models.check_text(data.get("text"))
        except ValueError:
            continue

        message = models.make_message(subscriber.room, subscriber.nick, text, data.get("cts"))
        hub.publish(subscriber.room, message)


async def send_loop(websocket, subscriber):
    try:
        while not subscriber.kicked:
            message = await subscriber.queue.get()
            await websocket.send_json(message)
        await websocket.close(code=4002, reason="отключён как медленный клиент")
    except WebSocketDisconnect:
        return
