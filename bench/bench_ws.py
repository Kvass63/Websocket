import asyncio
import json
from urllib.parse import urlencode

import websockets

import common


class WsClient:
    def __init__(self, deflate=True):
        self.compression = "deflate" if deflate else None
        self.ws = None
        self.send_errors = 0

    async def connect(self, room, nick):
        query = urlencode({"room": room, "nick": nick})
        self.ws = await websockets.connect(
            f"{common.WS_URL}/ws?{query}", compression=self.compression
        )

    async def send(self, text):
        data = {"text": text, "cts": common.now_ms()}
        raw = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        try:
            await self.ws.send(raw)
        except websockets.ConnectionClosed:
            self.send_errors += 1

    async def messages(self):
        try:
            async for raw in self.ws:
                yield json.loads(raw)
        except websockets.ConnectionClosed:
            return

    async def close(self):
        await self.ws.close()


async def main():
    parser = common.make_parser()
    parser.add_argument("--no-deflate", action="store_true",
                        help="отключить сжатие кадров WebSocket")
    args = parser.parse_args()

    deflate = not args.no_deflate
    transport = "ws" if deflate else "ws-nodeflate"
    await common.run_all(transport, lambda: WsClient(deflate), args.n, args.runs)


if __name__ == "__main__":
    asyncio.run(main())
