"""Hub — комнаты, подписчики и рассылка сообщений.

Это единственное место в программе, где хранится состояние чата.
Транспорты (WebSocket и SSE) про комнаты ничего не знают: они вызывают
hub.join, забирают сообщения из очереди подписчика и отправляют их
клиенту своим способом.

Блокировки здесь не нужны. Сервер работает в одном потоке, а в методах
Hub нет ни одного await, поэтому каждый метод выполняется целиком и
никто другой в это время словарь комнат не меняет.
"""

import asyncio
import logging

from app.core import models

# Сколько сообщений может ждать отправки одному клиенту.
# Если очередь заполнилась, клиент считается медленным и отключается.
QUEUE_SIZE = 100

log = logging.getLogger("chat.hub")


class NickTaken(Exception):
    """Ник в этой комнате уже занят."""


class Subscriber:
    """Один подключённый клиент."""

    def __init__(self, room, nick):
        self.room = room
        self.nick = nick
        # Сюда Hub кладёт сообщения, отсюда их забирает транспорт.
        self.queue = asyncio.Queue(maxsize=QUEUE_SIZE)
        # True, если Hub отключил клиента как медленного.
        # Транспорт проверяет этот флаг и закрывает соединение.
        self.kicked = False


class Hub:
    def __init__(self):
        # Название комнаты -> словарь {ник: Subscriber}.
        self.rooms = {}
        # Номер последнего сообщения (поле id), общий на весь сервер.
        self.last_id = 0

    def join(self, room, nick):
        """Добавить клиента в комнату и вернуть его Subscriber.

        Выбрасывает ValueError, если ник или комната некорректны,
        и NickTaken, если такой ник в комнате уже есть.
        """
        room = models.check_room(room)
        nick = models.check_nick(nick)

        if self.has(room, nick):
            raise NickTaken(nick)

        # Сначала сообщаем о входе тем, кто уже в комнате,
        # и только потом добавляем нового: ему самому join не нужен.
        self.publish(room, models.make_join(room, nick))

        subscriber = Subscriber(room, nick)
        if room not in self.rooms:
            self.rooms[room] = {}
        self.rooms[room][nick] = subscriber

        # Полный список онлайн получает только вошедший.
        subscriber.queue.put_nowait(models.make_presence(room, self.online(room)))
        return subscriber

    def leave(self, subscriber):
        """Убрать клиента из комнаты и сообщить об этом остальным.

        Повторный вызов безопасен: если клиента уже убрали
        (например, как медленного), ничего не происходит.
        """
        members = self.rooms.get(subscriber.room, {})
        if members.get(subscriber.nick) is not subscriber:
            return

        del members[subscriber.nick]
        if not members:
            # Пустые комнаты не храним.
            del self.rooms[subscriber.room]

        self.publish(subscriber.room, models.make_leave(subscriber.room, subscriber.nick))

    def publish(self, room, message):
        """Положить сообщение в очереди всех подписчиков комнаты.

        Всем кладётся один и тот же словарь, поэтому транспорты
        не должны его изменять.
        """
        if message["type"] == "message":
            self.last_id += 1
            message["id"] = self.last_id

        slow = []
        for subscriber in self.rooms.get(room, {}).values():
            try:
                # put_nowait не ждёт: если места нет, сразу бросает QueueFull.
                subscriber.queue.put_nowait(message)
            except asyncio.QueueFull:
                slow.append(subscriber)

        # Медленных убираем после цикла: менять словарь во время обхода нельзя.
        for subscriber in slow:
            if subscriber.kicked:
                # Уже выгнан во вложенной рассылке: leave другого медленного
                # клиента разослал событие leave и при этом выгнал и этого.
                continue
            log.warning(
                "Очередь переполнена, отключаем клиента: комната=%s ник=%s",
                subscriber.room,
                subscriber.nick,
            )
            subscriber.kicked = True
            self.leave(subscriber)

    def online(self, room):
        """Список ников в комнате по алфавиту."""
        return sorted(self.rooms.get(room, {}))

    def has(self, room, nick):
        """Есть ли такой ник в комнате."""
        return nick in self.rooms.get(room, {})

    def stats(self):
        """Числа для /api/stats: сколько подключений и насколько заняты очереди."""
        connections = 0
        queue_max = 0
        queue_total = 0
        rooms = {}
        for room, members in self.rooms.items():
            rooms[room] = len(members)
            for subscriber in members.values():
                size = subscriber.queue.qsize()
                connections += 1
                queue_total += size
                queue_max = max(queue_max, size)
        return {
            "connections": connections,
            "rooms": rooms,
            "queue_max": queue_max,
            "queue_total": queue_total,
            "queue_limit": QUEUE_SIZE,
        }


# Один Hub на всё приложение. main.py и оба транспорта импортируют
# именно эту переменную, поэтому клиенты WS и SSE видят друг друга.
hub = Hub()
