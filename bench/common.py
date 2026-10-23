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

Клиенты работают в нескольких процессах. Один процесс Python с 500
клиентами сам загружает ядро процессора, и часть задержки принадлежала бы
тестовому скрипту, а не серверу. Поэтому главный процесс держит только
отправителя u0, а остальные N - 1 клиентов (получатели) делятся между
WORKERS дочерними процессами. Дочерний процесс — тот же bench_*.py с теми
же аргументами и флагом --worker. Главный пишет ему команды в stdin и
читает ответы из его stdout, по одной строке JSON:

    главный -> дочерний         дочерний -> главный
    {"cmd": "connect", ...}     {"pid": ...}         все его клиенты вошли
    {"cmd": "sent"}             {"done": true}       получили всё или
                                                     прошло WAIT_TIMEOUT_S
    {"cmd": "close"}            {"latencies": ...}   итоги, процесс выходит

stdout дочернего процесса занят этим обменом, поэтому вся диагностика
печатается в stderr.

Сценарий одного прогона:
1. Запускаем дочерние процессы. Сначала входит u0, потом по очереди
   клиенты каждого процесса, внутри процесса пачками по CONNECT_BATCH.
2. Ждём, пока /api/stats покажет N подключений и пустые очереди.
3. u0 отправляет M сообщений с интервалом INTERVAL_MS, получают все N,
   включая отправителя. Ожидается N × M доставок.
4. Главный говорит всем «sent». Каждый процесс ждёт свою долю сообщений
   не дольше WAIT_TIMEOUT_S секунд и отвечает «done».
5. Считаем выкинутых, закрываем клиентов, собираем задержки со всех
   процессов, считаем потери, CPU и память, пишем CSV.

Запуск: одно N за раз, и перед каждым N сервер перезапускается. Python
не возвращает память системе, поэтому после прогона с N = 500 следующий
прогон с меньшим N показал бы завышенную память. Сервер без --reload:
    uvicorn app.main:app --no-access-log
    python bench/bench_ws.py --n 100       (или bench_sse.py)
"""

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

# --- Параметры, одинаковые для обоих транспортов ---

# 127.0.0.1, а не localhost: на Windows localhost сначала пробует IPv6
# (::1), а uvicorn по умолчанию слушает только IPv4.
HTTP_URL = "http://127.0.0.1:8000"
WS_URL = "ws://127.0.0.1:8000"

ROOM = "bench"
M = 200                # сколько сообщений отправляет отправитель
INTERVAL_MS = 20       # интервал между отправками
WAIT_TIMEOUT_S = 5     # сколько ждать недошедшие после последней отправки
TEXT_LEN = 100         # длина текста сообщения, только латиница
CONNECT_BATCH = 50     # сколько клиентов подключаем одновременно
SAMPLE_INTERVAL_S = 0.5  # как часто замерять CPU и память
READY_TIMEOUT_S = 60   # сколько ждать, пока сервер примет всех клиентов
WORKERS = 4            # сколько дочерних процессов с получателями

DEFAULT_RUNS = 3

RESULTS_DIR = Path(__file__).parent / "results"

CSV_FIELDS = [
    "transport", "n", "m", "run", "avg_ms", "p95_ms", "lost", "send_errors",
    "kicked", "cpu_avg", "cpu_max", "rss_start_mb", "rss_max_mb",
    "client_cpu_avg", "client_cpu_max",
]

# Слова для текстов сообщений.
WORDS = (
    "the a to and of in is it for on with at this that we you they can will "
    "meeting room slides today tomorrow please bring printed report team "
    "project server client message latency window update check build test "
    "review deploy coffee lunch friday morning evening question answer idea "
    "plan task bug fix release hello thanks sorry later soon maybe sure great "
    "work home office call email chat link file page code data result"
).split()


def make_texts():
    """M разных текстов по TEXT_LEN символов из случайных слов.

    Тексты разные, потому что WebSocket сжимает кадры (permessage-deflate)
    и помнит прошлые сообщения: одинаковый текст со второго раза сжимается
    почти в ноль, и WS получил бы нереально хорошие условия. Зерно
    случайности фиксированное, поэтому тексты одинаковые при каждом
    запуске и у обоих транспортов.
    """
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
    """Время в миллисекундах для cts и подсчёта задержки.

    perf_counter, а не time.time: на одной машине задержка около 1 мс,
    нужны дробные миллисекунды. cts ставит главный процесс, а задержку
    считают дочерние, поэтому значения perf_counter сравниваются между
    процессами. Так можно, потому что perf_counter читает один общий
    счётчик системы (на Windows QueryPerformanceCounter, на Linux
    CLOCK_MONOTONIC). Документация Python этого не обещает, поэтому
    проверили замером (см. docs/comparison.md).
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
    """Что получили клиенты процесса: задержки и время последней доставки."""

    def __init__(self, expected):
        self.expected = expected
        self.latencies = []
        self.last_time = None  # perf_counter последней доставки
        # Событие «пришло всё ожидаемое»: тогда не ждём тайм-аут зря.
        self.all_received = asyncio.Event()

    def add(self, message):
        # Считаем только сообщения бенчмарка; join, leave, presence пропускаем.
        if message.get("type") != "message" or "cts" not in message:
            return
        self.latencies.append(now_ms() - message["cts"])
        self.last_time = time.perf_counter()
        if len(self.latencies) == self.expected:
            self.all_received.set()


async def wait_delivery(delivery):
    """Ждать всё ожидаемое, но не дольше WAIT_TIMEOUT_S."""
    try:
        await asyncio.wait_for(delivery.all_received.wait(), WAIT_TIMEOUT_S)
    except asyncio.TimeoutError:
        pass


async def read_messages(client, delivery):
    """Задача одного клиента: читать всё, что присылает сервер."""
    async for message in client.messages():
        delivery.add(message)


async def connect_clients(make_client, first, count, delivery):
    """Создать count клиентов с никами u<first>, u<first+1>, ... и войти.

    Задача чтения запускается сразу после connect: если не читать, пока
    входят остальные, в очереди клиента накопятся сотни join и Hub его
    выкинет до замера. Возвращает клиентов и их задачи чтения.
    """
    clients = [make_client() for _ in range(count)]
    readers = []

    async def join(client, nick):
        await client.connect(ROOM, nick)
        readers.append(asyncio.create_task(read_messages(client, delivery)))

    # Пачками: 500 подключений сразу сервер принял бы медленнее и с
    # ошибками по тайм-ауту.
    for start in range(0, count, CONNECT_BATCH):
        batch = clients[start:start + CONNECT_BATCH]
        await asyncio.gather(*(
            join(client, f"u{first + start + i}") for i, client in enumerate(batch)
        ))
    return clients, readers


async def close_clients(clients, readers):
    """Закрыть клиентов и остановить чтение. Возвращает ошибки чтения.

    close() ждём: SSE-клиент в нём дожидается своих POST, и только после
    этого send_errors окончательный.
    """
    await asyncio.gather(*(client.close() for client in clients))
    for reader in readers:
        reader.cancel()
    # Ошибки задач чтения возвращаем, чтобы их напечатать: иначе упавший
    # клиент выглядел бы просто как потери. Отмена (CancelledError) — не
    # ошибка, это мы сами остановили задачу строкой выше.
    results = await asyncio.gather(*readers, return_exceptions=True)
    return [repr(result) for result in results if isinstance(result, Exception)]


async def sample_usage(server, client_processes, samples):
    """Раз в SAMPLE_INTERVAL_S записывать замер в samples:
    (время, CPU сервера %, память сервера МБ, CPU клиентов %).

    cpu_percent(None) считает загрузку с прошлого вызова, поэтому каждое
    значение описывает промежуток длиной SAMPLE_INTERVAL_S до замера.
    CPU клиентов — самого загруженного из их процессов: если хотя бы один
    упирается в ядро (100 %), задержки уже искажены тестом.
    """
    while True:
        await asyncio.sleep(SAMPLE_INTERVAL_S)
        cpu = server.cpu_percent(None)
        rss_mb = server.memory_info().rss / 1024 / 1024
        client_cpu = max(process.cpu_percent(None) for process in client_processes)
        samples.append((time.perf_counter(), cpu, rss_mb, client_cpu))


def usage_in_window(samples, window_end):
    """Среднее и максимум CPU сервера, наибольшая память сервера, среднее
    и максимум CPU клиентов за окно замера.

    Берём замеры, чей промежуток начался до конца окна: замер в момент t
    описывает время от t - SAMPLE_INTERVAL_S до t.
    """
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


# --- Дочерний процесс ---

def send_to_parent(data):
    """Отправить главному процессу строку JSON через stdout."""
    sys.stdout.write(json.dumps(data) + "\n")
    sys.stdout.flush()


async def read_from_parent(expected_cmd):
    """Дождаться команды главного процесса из stdin.

    sys.stdin.readline — обычный блокирующий вызов: прямо в цикле событий
    он остановил бы всё, и клиенты перестали бы читать сообщения.
    asyncio.to_thread выполняет его в отдельном потоке, а цикл событий
    тем временем работает дальше.
    """
    line = await asyncio.to_thread(sys.stdin.readline)
    if not line:
        raise RuntimeError("главный процесс закрыл stdin")
    command = json.loads(line)
    if command["cmd"] != expected_cmd:
        raise RuntimeError(f"ждали команду {expected_cmd}, пришла {command}")
    return command


async def run_worker(make_client):
    """Дочерний процесс: держит свою часть получателей и отвечает главному."""
    command = await read_from_parent("connect")
    count = command["count"]
    delivery = Delivery(expected=count * M)
    clients, readers = await connect_clients(make_client, command["first"], count, delivery)
    # PID сообщаем сами: на Windows python.exe из venv — только запускалка,
    # настоящий Python — её дочерний процесс, и PID, который видит главный
    # процесс, не тот, что нужен psutil.
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


# --- Главный процесс ---

# Самая длинная строка, которую главный читает из stdout дочернего. По
# умолчанию asyncio разрешает 64 КБ, а строка с задержками при N = 500 —
# около 0,5 МБ (125 клиентов × 200 сообщений).
STDOUT_LIMIT = 16 * 1024 * 1024


class Worker:
    """Дочерний процесс с получателями, как его видит главный процесс."""

    def __init__(self, first, count):
        self.first = first  # номер ника первого клиента
        self.count = count  # сколько клиентов
        self.proc = None
        self.pid = None     # PID настоящего Python, его присылает сам процесс

    async def start(self):
        # Тот же скрипт с теми же аргументами плюс --worker. stderr не
        # перехватываем: диагностика дочернего видна в той же консоли.
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
        """Убить процесс, если он ещё работает (прогон прервался ошибкой)."""
        if self.proc is not None and self.proc.returncode is None:
            self.proc.kill()


def split_receivers(n):
    """Поделить получателей u1..u(n-1) между WORKERS процессами поровну.

    Возвращает пары (номер первого ника, сколько клиентов). Процессов без
    клиентов не бывает: при n - 1 < WORKERS их просто меньше.
    """
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
    """Один прогон: n клиентов, M сообщений. Возвращает строку для CSV."""
    workers = [Worker(first, count) for first, count in split_receivers(n)]
    try:
        async with httpx.AsyncClient() as http:
            # Клиенты прошлого прогона могли ещё не выйти из комнаты.
            await wait_stats(http, lambda s: s["connections"] == 0, "0 подключений")

            # 1. Процессы запускаем все сразу: старт Python занимает время.
            # Входят по очереди: сначала u0, потом клиенты каждого процесса.
            await asyncio.gather(*(worker.start() for worker in workers))
            delivery = Delivery(expected=M)  # то, что получает сам u0
            clients, readers = await connect_clients(make_client, 0, 1, delivery)
            sender = clients[0]
            for worker in workers:
                await worker.send({"cmd": "connect", "first": worker.first, "count": worker.count})
                worker.pid = (await worker.receive())["pid"]

            # 2. Все вошли и разобрали свои join и presence.
            stats = await wait_stats(
                http,
                lambda s: s["connections"] == n and s["queue_total"] == 0,
                f"{n} подключений и пустые очереди",
            )
            server = psutil.Process(stats["pid"])
            # Память перед окном: от неё видно, сколько добавила сама рассылка.
            rss_start = server.memory_info().rss / 1024 / 1024
            # Процессы клиентов: главный (psutil.Process() без PID — свой) и
            # все дочерние.
            client_processes = [psutil.Process()]
            client_processes += [psutil.Process(worker.pid) for worker in workers]

            # 3. Окно замера. Первый cpu_percent всегда 0.0, его отбрасываем:
            # он только запоминает отправную точку.
            server.cpu_percent(None)
            for process in client_processes:
                process.cpu_percent(None)
            samples = []
            sampler = asyncio.create_task(sample_usage(server, client_processes, samples))
            window_start = time.perf_counter()

            # Отправляем по расписанию start + i × интервал, а не «sleep после
            # каждой отправки»: так задержки отправки не накапливаются.
            for i in range(M):
                delay = window_start + i * INTERVAL_MS / 1000 - time.perf_counter()
                if delay > 0:
                    await asyncio.sleep(delay)
                await sender.send(TEXTS[i])

            # 4. Каждый процесс ждёт свою долю не дольше тайм-аута; главный
            # тем временем ждёт сообщения для u0.
            for worker in workers:
                await worker.send({"cmd": "sent"})
            await asyncio.gather(
                wait_delivery(delivery), *(worker.receive() for worker in workers)
            )
            sampler.cancel()

            # Выкинутые как медленные: сразу после ожидания, пока клиенты
            # ещё не закрыты (иначе все подключения уже будут 0).
            stats = await get_stats(http)
            kicked = n - stats["connections"]

            # 5. Закрываем клиентов и собираем итоги со всех процессов.
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

    # Окно — до последней доставки в любом процессе, без ожидания по
    # тайм-ауту. Если не дошло ничего, окно пустое.
    received_times = [t for t in last_times if t is not None]
    window_end = max(received_times) if received_times else window_start

    avg, p95 = latency_stats(latencies)
    cpu_avg, cpu_max, rss_max, client_cpu_avg, client_cpu_max = usage_in_window(samples, window_end)
    row = {
        "transport": transport,
        "n": n,
        "m": M,
        "run": run,
        "avg_ms": rounded(avg, 3),
        "p95_ms": rounded(p95, 3),
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
    """Параметры командной строки. bench_*.py может добавить свои."""
    parser = argparse.ArgumentParser(description="Нагрузочный тест чата")
    parser.add_argument("--n", type=int, required=True,
                        help="число клиентов; перед каждым N перезапустите сервер")
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS,
                        help="сколько прогонов подряд")
    # Служебный флаг: с ним run_scenario запускает дочерние процессы.
    # Руками его не указывают, поэтому в --help он скрыт.
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    return parser


async def run_all(transport, make_client, n, runs):
    """runs прогонов подряд с одним и тем же N.

    С флагом --worker скрипт — дочерний процесс одного прогона: сценарий
    он не ведёт, а держит своих клиентов и выполняет команды главного.
    Флаг смотрим прямо в sys.argv, чтобы не менять bench_ws.py и
    bench_sse.py: они передают сюда только n и runs.
    """
    if "--worker" in sys.argv[1:]:
        await run_worker(make_client)
        return
    for run in range(1, runs + 1):
        row = await run_scenario(transport, make_client, n, run)
        print(row)
