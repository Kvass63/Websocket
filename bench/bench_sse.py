"""Нагрузочный тест SSE (участник B).

Здесь только клиент. Сценарий, замеры и запись результатов — в common.py,
одни и те же для обоих транспортов.

Чем клиент отличается от WebSocket:
- Сообщения от сервера приходят по одному долгому ответу GET /sse/events.
  Его читаем построчно и разбираем формат SSE сами (в браузере это делает
  EventSource).
- Свои сообщения отправляем отдельными запросами POST /sse/send. Каждый
  POST — отдельная задача, чтобы медленный ответ не сдвигал расписание.

Запуск (сервер перезапущен перед каждым N, без --reload):
    python bench/bench_sse.py --n 100
"""

import asyncio
import json

import httpx

import common


class SseClient:
    """Один клиент чата по SSE."""

    def __init__(self, streams, posts):
        self.streams = streams  # httpx-клиент для долгих GET /sse/events
        self.posts = posts      # httpx-клиент для POST /sse/send
        self.response = None    # открытый ответ-стрим
        self.room = ""
        self.nick = ""
        self.pending = []       # задачи с POST-запросами
        self.send_errors = 0

    async def connect(self, room, nick):
        self.room = room
        self.nick = nick
        request = self.streams.build_request(
            "GET", f"{common.HTTP_URL}/sse/events", params={"room": room, "nick": nick}
        )
        # stream=True: вернуть ответ сразу после заголовков, а тело читать
        # потом, в messages(). Сервер делает hub.join до начала ответа,
        # поэтому, когда пришли заголовки, мы уже в комнате.
        self.response = await self.streams.send(request, stream=True)
        if self.response.status_code != 200:
            # 409 (ник занят) или 422 (некорректный ник или комната).
            await self.response.aclose()
            raise RuntimeError(f"{nick} не вошёл: HTTP {self.response.status_code}")

    async def send(self, text):
        # Ответа здесь не ждём: POST уходит отдельной задачей, а сценарий
        # сразу идёт к следующему сообщению по расписанию. Задачи храним
        # в списке: asyncio держит задачи только по слабым ссылкам, и
        # незаконченную задачу без ссылки может удалить сборщик мусора.
        self.pending.append(asyncio.create_task(self.post(text)))

    async def post(self, text):
        # cts ставим прямо перед POST, а не по расписанию.
        body = {"room": self.room, "nick": self.nick, "text": text, "cts": common.now_ms()}
        try:
            # json= кодирует так же компактно, как сервер:
            # ensure_ascii=False и separators=(",", ":").
            response = await self.posts.post(f"{common.HTTP_URL}/sse/send", json=body)
        except httpx.HTTPError:
            # Сервер не ответил или соединение оборвалось.
            self.send_errors += 1
            return
        if response.status_code != 200:
            # Например 403: нас выкинули как медленного, ника в комнате нет.
            self.send_errors += 1

    async def messages(self):
        # Событие SSE — несколько строк "поле: значение", пустая строка —
        # конец события. Нам нужны поля event и data. Строку id пропускаем,
        # комментарий ": ping" тоже. Наш сервер пишет data одной строкой,
        # поэтому склеивать несколько строк data не нужно.
        event = ""
        data = ""
        try:
            async for line in self.response.aiter_lines():
                if line.startswith("event: "):
                    event = line[len("event: "):]
                elif line.startswith("data: "):
                    data = line[len("data: "):]
                elif line == "":
                    # Конец события.
                    if event == "kicked":
                        # Сервер выкинул нас как медленного клиента и сейчас
                        # закроет стрим. Это не сообщение чата.
                        return
                    if data:
                        yield json.loads(data)
                    event = ""
                    data = ""
        except (httpx.HTTPError, httpx.StreamError):
            # Соединение оборвалось или мы сами закрыли стрим в close():
            # для сценария это просто конец сообщений.
            return

    async def close(self):
        # Сначала дожидаемся своих POST: только после этого send_errors
        # окончательный.
        await asyncio.gather(*self.pending)
        # Закрываем стрим. Сервер увидит отключение и вызовет hub.leave.
        await self.response.aclose()


async def main():
    args = common.make_parser().parse_args()
    # streams — для долгих GET. По умолчанию один httpx-клиент держит не
    # больше 100 соединений, при N = 500 остальные ждали бы в очереди,
    # поэтому лимит снимаем. timeout=None: стрим может долго молчать
    # (": ping" раз в 15 с), и это не ошибка.
    # posts — один клиент с keep-alive для всех POST, как в браузере.
    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=None), timeout=None) as streams, \
               httpx.AsyncClient() as posts:
        await common.run_all("sse", lambda: SseClient(streams, posts), args.n, args.runs)


if __name__ == "__main__":
    asyncio.run(main())
