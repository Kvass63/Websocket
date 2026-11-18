import argparse
import asyncio
import csv
import json
import math
import os
import random
import sys
import time
from pathlib import Path

import httpx
import psutil


HTTP_URL = "http://127.0.0.1:8000"
WS_URL = "ws://127.0.0.1:8000"

ROOM = "bench"
M = 200
INTERVAL_MS = 20
WAIT_TIMEOUT_S = 30
TEXT_LEN = 100
CONNECT_BATCH = 50
SAMPLE_INTERVAL_S = 0.5
READY_TIMEOUT_S = 60
WORKERS = 4

DEFAULT_RUNS = 3

RESULTS_DIR = Path(__file__).parent / "results"

CSV_FIELDS = [
    "transport", "n", "m", "run", "avg_ms", "p95_ms", "deliveries_per_s",
    "lost", "send_errors",
    "kicked", "cpu_avg", "cpu_max", "rss_start_mb", "rss_max_mb",
    "client_cpu_avg", "client_cpu_max",
]

WORDS = (
    "the a to and of in is it for on with at this that we you they can will "
    "meeting room slides today tomorrow please bring printed report team "
    "project server client message latency window update check build test "
    "review deploy coffee lunch friday morning evening question answer idea "
    "plan task bug fix release hello thanks sorry later soon maybe sure great "
    "work home office call email chat link file page code data result"
).split()


def make_texts():
    rng = random.Random(42)
    texts = []
    for _ in range(M):
        text = ""
        while len(text) < TEXT_LEN:
            text += rng.choice(WORDS) + " "
        texts.append(text[:TEXT_LEN])
    return texts


TEXTS = make_texts()


def now_ms():
    return time.perf_counter() * 1000


def latency_stats(latencies):
    if not latencies:
        return None, None
    ordered = sorted(latencies)
    avg = sum(ordered) / len(ordered)
    p95 = ordered[math.ceil(0.95 * len(ordered)) - 1]
    return avg, p95


async def get_stats(http):
    response = await http.get(f"{HTTP_URL}/api/stats")
    response.raise_for_status()
    return response.json()


async def wait_stats(http, condition, what):
    deadline = time.perf_counter() + READY_TIMEOUT_S
    while True:
        stats = await get_stats(http)
        if condition(stats):
            return stats
        if time.perf_counter() > deadline:
            raise RuntimeError(f"не дождались: {what}; /api/stats = {stats}")
        await asyncio.sleep(0.1)


class Delivery:
    def __init__(self, expected):
        self.expected = expected
        self.latencies = []
        self.last_time = None
        self.all_received = asyncio.Event()

    def add(self, message):
        if message.get("type") != "message" or "cts" not in message:
            return
        self.latencies.append(now_ms() - message["cts"])
        self.last_time = time.perf_counter()
        if len(self.latencies) == self.expected:
            self.all_received.set()


async def wait_delivery(delivery):
    try:
        await asyncio.wait_for(delivery.all_received.wait(), WAIT_TIMEOUT_S)
    except asyncio.TimeoutError:
        pass


async def read_messages(client, delivery):
    async for message in client.messages():
        delivery.add(message)


async def connect_clients(make_client, first, count, delivery):
    clients = [make_client() for _ in range(count)]
    readers = []

    async def join(client, nick):
        await client.connect(ROOM, nick)
        readers.append(asyncio.create_task(read_messages(client, delivery)))

    for start in range(0, count, CONNECT_BATCH):
        batch = clients[start:start + CONNECT_BATCH]
        await asyncio.gather(*(
            join(client, f"u{first + start + i}") for i, client in enumerate(batch)
        ))
    return clients, readers


async def close_clients(clients, readers):
    await asyncio.gather(*(client.close() for client in clients))
    for reader in readers:
        reader.cancel()
    results = await asyncio.gather(*readers, return_exceptions=True)
    return [repr(result) for result in results if isinstance(result, Exception)]


async def sample_usage(server, client_processes, samples):
    while True:
        await asyncio.sleep(SAMPLE_INTERVAL_S)
        cpu = server.cpu_percent(None)
        rss_mb = server.memory_info().rss / 1024 / 1024
        client_cpu = max(process.cpu_percent(None) for process in client_processes)
        samples.append((time.perf_counter(), cpu, rss_mb, client_cpu))


def usage_in_window(samples, window_end):
    inside = [s for s in samples if s[0] - SAMPLE_INTERVAL_S < window_end]
    if not inside:
        return None, None, None, None, None
    cpus = [s[1] for s in inside]
    client_cpus = [s[3] for s in inside]
    return (
        sum(cpus) / len(cpus), max(cpus),
        max(s[2] for s in inside),
        sum(client_cpus) / len(client_cpus), max(client_cpus),
    )


def save_csv(row):
    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / f"{row['transport']}_n{row['n']}_run{row['run']}.csv"
    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerow(row)
    return path


def rounded(value, digits):
    return None if value is None else round(value, digits)


def send_to_parent(data):
    sys.stdout.write(json.dumps(data) + "\n")
    sys.stdout.flush()


async def read_from_parent(expected_cmd):
    line = await asyncio.to_thread(sys.stdin.readline)
    if not line:
        raise RuntimeError("главный процесс закрыл stdin")
    command = json.loads(line)
    if command["cmd"] != expected_cmd:
        raise RuntimeError(f"ждали команду {expected_cmd}, пришла {command}")
    return command


async def run_worker(make_client):
    command = await read_from_parent("connect")
    count = command["count"]
    delivery = Delivery(expected=count * M)
    clients, readers = await connect_clients(make_client, command["first"], count, delivery)
    send_to_parent({"pid": os.getpid()})

    await read_from_parent("sent")
    await wait_delivery(delivery)
    send_to_parent({"done": True})

    await read_from_parent("close")
    read_errors = await close_clients(clients, readers)
    send_to_parent({
        "latencies": delivery.latencies,
        "last_time": delivery.last_time,
        "send_errors": sum(client.send_errors for client in clients),
        "read_errors": read_errors,
    })


STDOUT_LIMIT = 16 * 1024 * 1024


class Worker:
    def __init__(self, first, count):
        self.first = first
        self.count = count
        self.proc = None
        self.pid = None

    async def start(self):
        self.proc = await asyncio.create_subprocess_exec(
            sys.executable, sys.argv[0], *sys.argv[1:], "--worker",
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            limit=STDOUT_LIMIT,
        )

    async def send(self, data):
        self.proc.stdin.write((json.dumps(data) + "\n").encode())
        await self.proc.stdin.drain()

    async def receive(self):
        line = await self.proc.stdout.readline()
        if not line:
            raise RuntimeError(
                f"дочерний процесс с клиентами u{self.first}.."
                f"u{self.first + self.count - 1} завершился, причина выше в stderr"
            )
        return json.loads(line)

    def stop(self):
        if self.proc is not None and self.proc.returncode is None:
            self.proc.kill()


def split_receivers(n):
    base, extra = divmod(n - 1, WORKERS)
    parts = []
    first = 1
    for i in range(WORKERS):
        count = base + (1 if i < extra else 0)
        if count > 0:
            parts.append((first, count))
        first += count
    return parts


async def run_scenario(transport, make_client, n, run):
    workers = [Worker(first, count) for first, count in split_receivers(n)]
    try:
        async with httpx.AsyncClient() as http:
            await wait_stats(http, lambda s: s["connections"] == 0, "0 подключений")

            await asyncio.gather(*(worker.start() for worker in workers))
            delivery = Delivery(expected=M)
            clients, readers = await connect_clients(make_client, 0, 1, delivery)
            sender = clients[0]
            for worker in workers:
                await worker.send({"cmd": "connect", "first": worker.first, "count": worker.count})
                worker.pid = (await worker.receive())["pid"]

            stats = await wait_stats(
                http,
                lambda s: s["connections"] == n and s["queue_total"] == 0,
                f"{n} подключений и пустые очереди",
            )
            server = psutil.Process(stats["pid"])
            rss_start = server.memory_info().rss / 1024 / 1024
            client_processes = [psutil.Process()]
            client_processes += [psutil.Process(worker.pid) for worker in workers]

            server.cpu_percent(None)
            for process in client_processes:
                process.cpu_percent(None)
            samples = []
            sampler = asyncio.create_task(sample_usage(server, client_processes, samples))
            window_start = time.perf_counter()

            for i in range(M):
                delay = window_start + i * INTERVAL_MS / 1000 - time.perf_counter()
                if delay > 0:
                    await asyncio.sleep(delay)
                await sender.send(TEXTS[i])

            for worker in workers:
                await worker.send({"cmd": "sent"})
            await asyncio.gather(
                wait_delivery(delivery), *(worker.receive() for worker in workers)
            )
            sampler.cancel()

            stats = await get_stats(http)
            kicked = n - stats["connections"]

            for worker in workers:
                await worker.send({"cmd": "close"})
            read_errors = await close_clients(clients, readers)
            latencies = list(delivery.latencies)
            last_times = [delivery.last_time]
            send_errors = sender.send_errors
            for worker in workers:
                result = await worker.receive()
                latencies += result["latencies"]
                last_times.append(result["last_time"])
                send_errors += result["send_errors"]
                read_errors += result["read_errors"]
                await worker.proc.wait()
    finally:
        for worker in workers:
            worker.stop()

    for error in read_errors:
        print(f"ошибка при чтении сообщений: {error}", file=sys.stderr)

    received_times = [t for t in last_times if t is not None]
    window_end = max(received_times) if received_times else window_start

    window_s = window_end - window_start
    deliveries_per_s = len(latencies) / window_s if window_s > 0 else None

    avg, p95 = latency_stats(latencies)
    cpu_avg, cpu_max, rss_max, client_cpu_avg, client_cpu_max = usage_in_window(samples, window_end)
    row = {
        "transport": transport,
        "n": n,
        "m": M,
        "run": run,
        "avg_ms": rounded(avg, 3),
        "p95_ms": rounded(p95, 3),
        "deliveries_per_s": rounded(deliveries_per_s, 0),
        "lost": n * M - len(latencies),
        "send_errors": send_errors,
        "kicked": kicked,
        "cpu_avg": rounded(cpu_avg, 1),
        "cpu_max": rounded(cpu_max, 1),
        "rss_start_mb": rounded(rss_start, 1),
        "rss_max_mb": rounded(rss_max, 1),
        "client_cpu_avg": rounded(client_cpu_avg, 1),
        "client_cpu_max": rounded(client_cpu_max, 1),
    }
    save_csv(row)
    return row


def make_parser():
    parser = argparse.ArgumentParser(description="Нагрузочный тест чата")
    parser.add_argument("--n", type=int, required=True,
                        help="число клиентов; перед каждым N перезапустите сервер")
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS,
                        help="сколько прогонов подряд")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    return parser


async def run_all(transport, make_client, n, runs):
    if "--worker" in sys.argv[1:]:
        await run_worker(make_client)
        return
    for run in range(1, runs + 1):
        row = await run_scenario(transport, make_client, n, run)
        print(row)
