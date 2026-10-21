"""Общая часть бенчмарка: параметры, замеры и сам сценарий.

Сценарий одинаковый для WebSocket и SSE, поэтому он описан здесь один раз.
Файлы bench_ws.py и bench_sse.py содержат только класс клиента — то, чем
транспорты отличаются. Это тот же подход, что с Hub на сервере.

Клиент, которого принимает run_scenario, должен уметь:
    await client.connect(room, nick)   войти в комнату
    await client.send(text)            отправить сообщение, cts ставит сам
    async for msg in client.messages() получать сообщения (словари);
                                       цикл заканчивается, когда сервер
                                       закрыл соединение
    await client.close()               закрыть; у SSE ещё и дождаться
                                       недоотправленных POST
    client.send_errors                 сколько отправок не удалось

Сценарий одного прогона:
1. N клиентов входят в комнату пачками по CONNECT_BATCH.
2. Ждём, пока /api/stats покажет N подключений и пустые очереди.
3. Первый клиент отправляет M сообщений с интервалом INTERVAL_MS,
   получают все N, включая отправителя. Ожидается N × M доставок.
4. Ждём недошедшие сообщения не дольше WAIT_TIMEOUT_S секунд.
5. Считаем задержку, потери, CPU и память сервера, пишем CSV.

Запуск: сервер без --reload, например
    uvicorn app.main:app --no-access-log
и затем python bench/bench_ws.py (или bench_sse.py).
"""

import argparse
import asyncio
import csv
import math
import time
from pathlib import Path

import httpx
import psutil

# --- Параметры, одинаковые для обоих транспортов ---

# 127.0.0.1, а не localhost: на Windows localhost сначала пробует IPv6
# (::1), а uvicorn по умолчанию слушает только IPv4.
HTTP_URL = "http://127.0.0.1:8000"
WS_URL = "ws://127.0.0.1:8000"

ROOM = "bench"
M = 200                # сколько сообщений отправляет отправитель
INTERVAL_MS = 20       # интервал между отправками
WAIT_TIMEOUT_S = 5     # сколько ждать недошедшие после последней отправки
TEXT = "x" * 100       # текст сообщения: 100 символов, только латиница
CONNECT_BATCH = 50     # сколько клиентов подключаем одновременно
SAMPLE_INTERVAL_S = 0.5  # как часто замерять CPU и память сервера
READY_TIMEOUT_S = 60   # сколько ждать, пока сервер примет всех клиентов

DEFAULT_NS = [10, 100, 500]
DEFAULT_RUNS = 3

RESULTS_DIR = Path(__file__).parent / "results"

CSV_FIELDS = [
    "transport", "n", "m", "run", "avg_ms", "p95_ms", "lost", "send_errors",
    "kicked", "cpu_avg", "cpu_max", "rss_max_mb",
]


def now_ms():
    """Время в миллисекундах для cts и подсчёта задержки.

    perf_counter, а не time.time: на одной машине задержка около 1 мс,
    нужны дробные миллисекунды. perf_counter можно сравнивать только
    внутри одного процесса, поэтому отправитель и получатели — в одном.
    """
    return time.perf_counter() * 1000


# --- Помощники ---

def latency_stats(latencies):
    """Среднее и p95 по всем задержкам вместе, в миллисекундах.

    p95 — значение, меньше или равно которому 95 % задержек: сортируем
    список и берём элемент с номером ceil(0,95 × длина).
    """
    if not latencies:
        return None, None
    ordered = sorted(latencies)
    avg = sum(ordered) / len(ordered)
    p95 = ordered[math.ceil(0.95 * len(ordered)) - 1]
    return avg, p95


async def get_stats(http):
    """Ответ /api/stats сервера в виде словаря."""
    response = await http.get(f"{HTTP_URL}/api/stats")
    response.raise_for_status()
    return response.json()


async def wait_stats(http, condition, what):
    """Опрашивать /api/stats, пока condition(stats) не станет True."""
    deadline = time.perf_counter() + READY_TIMEOUT_S
    while True:
        stats = await get_stats(http)
        if condition(stats):
            return stats
        if time.perf_counter() > deadline:
            raise RuntimeError(f"не дождались: {what}; /api/stats = {stats}")
        await asyncio.sleep(0.1)


class Delivery:
    """Что получили клиенты за прогон: задержки и время последней доставки."""

    def __init__(self, expected):
        self.expected = expected
        self.latencies = []
        self.last_time = None  # perf_counter последней доставки
        # Событие «пришли все N × M»: тогда не ждём тайм-аут зря.
        self.all_received = asyncio.Event()

    def add(self, message):
        # Считаем только сообщения бенчмарка; join, leave, presence пропускаем.
        if message.get("type") != "message" or "cts" not in message:
            return
        self.latencies.append(now_ms() - message["cts"])
        self.last_time = time.perf_counter()
        if len(self.latencies) == self.expected:
            self.all_received.set()


async def read_messages(client, delivery):
    """Задача одного клиента: читать всё, что присылает сервер.

    Запускается сразу после connect: если не читать, пока входят остальные,
    в очереди клиента накопятся сотни join и Hub его выкинет до замера.
    """
    async for message in client.messages():
        delivery.add(message)


async def sample_server(process, samples):
    """Раз в SAMPLE_INTERVAL_S записывать (время, CPU %, память МБ) сервера.

    cpu_percent(None) считает загрузку с прошлого вызова, поэтому каждое
    значение описывает промежуток длиной SAMPLE_INTERVAL_S до замера.
    """
    while True:
        await asyncio.sleep(SAMPLE_INTERVAL_S)
        cpu = process.cpu_percent(None)
        rss_mb = process.memory_info().rss / 1024 / 1024
        samples.append((time.perf_counter(), cpu, rss_mb))


def server_usage(samples, window_end):
    """CPU (среднее и максимум) и наибольшая память за окно замера.

    Берём замеры, чей промежуток начался до конца окна: замер в момент t
    описывает время от t - SAMPLE_INTERVAL_S до t.
    """
    inside = [s for s in samples if s[0] - SAMPLE_INTERVAL_S < window_end]
    if not inside:
        return None, None, None
    cpus = [s[1] for s in inside]
    return sum(cpus) / len(cpus), max(cpus), max(s[2] for s in inside)


def save_csv(row):
    """Записать результат прогона в bench/results/<transport>_n<N>_run<K>.csv."""
    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / f"{row['transport']}_n{row['n']}_run{row['run']}.csv"
    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerow(row)
    return path


def rounded(value, digits):
    return None if value is None else round(value, digits)


# --- Сценарий ---

async def run_scenario(transport, make_client, n, run):
    """Один прогон: n клиентов, M сообщений. Возвращает строку для CSV."""
    async with httpx.AsyncClient() as http:
        # Клиенты прошлого прогона могли ещё не выйти из комнаты.
        await wait_stats(http, lambda s: s["connections"] == 0, "0 подключений")

        delivery = Delivery(expected=n * M)
        clients = [make_client() for _ in range(n)]
        readers = []

        async def join(client, nick):
            await client.connect(ROOM, nick)
            readers.append(asyncio.create_task(read_messages(client, delivery)))

        # 1. Вход пачками: 500 подключений сразу сервер принял бы
        # медленнее и с ошибками по тайм-ауту.
        for start in range(0, n, CONNECT_BATCH):
            batch = clients[start:start + CONNECT_BATCH]
            await asyncio.gather(*(
                join(client, f"u{start + i}") for i, client in enumerate(batch)
            ))

        # 2. Все вошли и разобрали свои join и presence.
        stats = await wait_stats(
            http,
            lambda s: s["connections"] == n and s["queue_total"] == 0,
            f"{n} подключений и пустые очереди",
        )
        process = psutil.Process(stats["pid"])

        # 3. Окно замера. Первый cpu_percent всегда 0.0, его отбрасываем:
        # он только запоминает отправную точку.
        process.cpu_percent(None)
        samples = []
        sampler = asyncio.create_task(sample_server(process, samples))
        window_start = time.perf_counter()

        # Отправляем по расписанию start + i × интервал, а не «sleep после
        # каждой отправки»: так задержки отправки не накапливаются.
        sender = clients[0]
        for i in range(M):
            delay = window_start + i * INTERVAL_MS / 1000 - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            await sender.send(TEXT)

        # 4. Ждём все доставки, но не дольше тайм-аута.
        try:
            await asyncio.wait_for(delivery.all_received.wait(), WAIT_TIMEOUT_S)
        except asyncio.TimeoutError:
            pass
        sampler.cancel()

        # Окно — до последней доставки, без ожидания по тайм-ауту.
        window_end = delivery.last_time or window_start

        # Выкинутые как медленные: сразу после ожидания, пока клиенты
        # ещё не закрыты (иначе все подключения уже будут 0).
        stats = await get_stats(http)
        kicked = n - stats["connections"]

        # 5. Закрываем клиентов. close() ждём: SSE-клиент в нём дожидается
        # своих POST, и только после этого send_errors окончательный.
        await asyncio.gather(*(client.close() for client in clients))
        for reader in readers:
            reader.cancel()
        await asyncio.gather(*readers, return_exceptions=True)

    avg, p95 = latency_stats(delivery.latencies)
    cpu_avg, cpu_max, rss_max = server_usage(samples, window_end)
    row = {
        "transport": transport,
        "n": n,
        "m": M,
        "run": run,
        "avg_ms": rounded(avg, 3),
        "p95_ms": rounded(p95, 3),
        "lost": n * M - len(delivery.latencies),
        "send_errors": sum(client.send_errors for client in clients),
        "kicked": kicked,
        "cpu_avg": rounded(cpu_avg, 1),
        "cpu_max": rounded(cpu_max, 1),
        "rss_max_mb": rounded(rss_max, 1),
    }
    save_csv(row)
    return row


def parse_args():
    """Параметры командной строки: какие N и сколько прогонов."""
    parser = argparse.ArgumentParser(description="Нагрузочный тест чата")
    parser.add_argument("--n", type=int, nargs="+", default=DEFAULT_NS,
                        help="число клиентов, можно несколько: --n 10 100")
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS,
                        help="сколько прогонов на каждое N")
    args = parser.parse_args()
    return args.n, args.runs


async def run_all(transport, make_client, ns, runs):
    """Все прогоны подряд: для каждого N — runs раз."""
    for n in ns:
        for run in range(1, runs + 1):
            row = await run_scenario(transport, make_client, n, run)
            print(row)
