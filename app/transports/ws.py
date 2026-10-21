"""Транспорт WebSocket (участник A).

Адрес: WS /ws?room=<room>&nick=<nick>

Как это работает:
1. Браузер присылает обычный HTTP-запрос с заголовком Upgrade: websocket,
   websocket.accept() отвечает кодом 101, и дальше по тому же соединению
   в обе стороны идут кадры WebSocket.
2. Клиент входит в комнату через hub.join.
3. Запускаются две задачи, которые работают одновременно:
   - receive_loop ждёт сообщения от клиента и рассылает их в комнату;
   - send_loop ждёт сообщения в очереди подписчика и шлёт их клиенту.
   Одна задача не справилась бы: пока она ждёт клиента, сообщения
   из комнаты лежали бы в очереди неотправленными.
4. Когда любая задача завершилась, вторую отменяем, а клиента убираем
   из комнаты (hub.leave в блоке finally).

Коды закрытия от сервера (диапазон 4000–4999 отведён под коды приложений):
4000 — некорректный ник или комната, 4001 — ник занят,
4002 — отключён как медленный клиент.
"""

import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.core import models
from app.core.hub import NickTaken, hub

router = APIRouter()


@router.websocket("/ws")
async def ws_endpoint(websocket: WebSocket, room: str = "", nick: str = ""):
    # Сначала accept, потом close: если закрыть до accept, клиент получит
    # HTTP 403 и код 1006 и не узнает, в чём дело.
    await websocket.accept()

    # Причина закрытия ограничена 123 байтами, а русская буква в UTF-8
    # занимает 2 байта, поэтому причина короткая. Тексты такие же,
    # как у SSE, чтобы интерфейс показывал одинаковые сообщения.
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
        # Ждём, пока завершится хотя бы одна из двух задач.
        done, pending = await asyncio.wait(
            [receiver, sender], return_when=asyncio.FIRST_COMPLETED
        )
        # Если задача упала с ошибкой (не из-за отключения клиента),
        # result() выбросит эту ошибку здесь, и uvicorn покажет её в логе.
        for task in done:
            task.result()
    finally:
        # Отменить уже завершённую задачу можно, это ничего не делает.
        receiver.cancel()
        sender.cancel()
        hub.leave(subscriber)


async def receive_loop(websocket, subscriber):
    """Читать сообщения клиента и рассылать их в комнату."""
    while True:
        try:
            raw = await websocket.receive_text()
        except WebSocketDisconnect:
            # Клиент закрыл вкладку или нажал «Выйти».
            return

        # Пока мы ждали, Hub мог отключить клиента как медленного.
        # Тогда его сообщение не рассылаем, но и не выходим: если эта задача
        # завершится первой, ws_endpoint отменит send_loop раньше, чем тот
        # закроет соединение с кодом 4002, и клиент не узнает причину.
        # Цикл закончится сам: send_loop закроет соединение, и ws_endpoint
        # отменит эту задачу (или придёт WebSocketDisconnect).
        if subscriber.kicked:
            continue

        # Ожидаем {"text": "...", "cts": 123.4}; cts необязателен.
        try:
            data = json.loads(raw)
        except ValueError:
            # Пришёл не JSON. Пропускаем, соединение не рвём.
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
    """Забирать сообщения из очереди подписчика и отправлять клиенту."""
    try:
        # Флаг kicked ставит Hub, когда очередь переполнилась.
        # Тогда в очереди лежат сообщения, и get() не зависнет.
        while not subscriber.kicked:
            message = await subscriber.queue.get()
            await websocket.send_json(message)
        await websocket.close(code=4002, reason="отключён как медленный клиент")
    except WebSocketDisconnect:
        # Клиент ушёл, пока мы ему писали.
        return
