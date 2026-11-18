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
        self.streams = streams
        self.posts = posts
        self.response = None
        self.room = ""
        self.nick = ""
        self.pending = []
        self.send_errors = 0

    async def connect(self, room, nick):
        self.room = room
        self.nick = nick
        request = self.streams.build_request(
            "GET", f"{common.HTTP_URL}/sse/events", params={"room": room, "nick": nick}
        )
        self.response = await self.streams.send(request, stream=True)
        if self.response.status_code != 200:
            await self.response.aclose()
            raise RuntimeError(f"{nick} не вошёл: HTTP {self.response.status_code}")

    async def send(self, text):
        self.pending.append(asyncio.create_task(self.post(text)))

    async def post(self, text):
        body = {"room": self.room, "nick": self.nick, "text": text, "cts": common.now_ms()}
        try:
            response = await self.posts.post(f"{common.HTTP_URL}/sse/send", json=body)
        except httpx.HTTPError:
            self.send_errors += 1
            return
        if response.status_code != 200:
            self.send_errors += 1

    async def messages(self):
        event = ""
        data = ""
        try:
            async for line in self.response.aiter_lines():
                if line.startswith("event: "):
                    event = line[len("event: "):]
                elif line.startswith("data: "):
                    data = line[len("data: "):]
                elif line == "":
                    if event == "kicked":
                        return
                    if data:
                        yield json.loads(data)
                    event = ""
                    data = ""
        except (httpx.HTTPError, httpx.StreamError):
            return

    async def close(self):
        await asyncio.gather(*self.pending)
        await self.response.aclose()


async def main():
    args = common.make_parser().parse_args()
    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=None), timeout=None) as streams, \
               httpx.AsyncClient(timeout=30) as posts:
        await common.run_all("sse", lambda: SseClient(streams, posts), args.n, args.runs)


if __name__ == "__main__":
    asyncio.run(main())
