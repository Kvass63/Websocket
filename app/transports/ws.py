"""Транспорт WebSocket (участник A).

ЗАГЛУШКА шага 1: сервер принимает соединение и сразу закрывает его.

Что нужно сделать на шаге 2:
1. Вызвать websocket.accept().
2. Вызвать hub.join(room, nick). При ValueError закрыть с кодом 4000,
   при NickTaken — с кодом 4001.
3. Запустить две одновременные задачи:
   - чтение: receive_json() -> models.check_text -> hub.publish(...)
   - запись: subscriber.queue.get() -> send_json(...); если
     subscriber.kicked, закрыть с кодом 4002.
4. Когда одна задача завершилась, отменить вторую.
5. В блоке finally вызвать hub.leave(subscriber).

Понадобятся: from app.core import models
             from app.core.hub import hub, NickTaken
"""

from fastapi import APIRouter, WebSocket

router = APIRouter()


@router.websocket("/ws")
async def ws_endpoint(websocket: WebSocket, room: str = "", nick: str = ""):
    # Сначала accept, потом close: иначе клиент не увидит код закрытия.
    await websocket.accept()
    # 1011 — стандартный код «внутренняя ошибка сервера».
    await websocket.close(code=1011, reason="WebSocket ещё не реализован")
