import asyncio
import logging

from app.core import models

QUEUE_SIZE = 100

log = logging.getLogger("chat.hub")


class NickTaken(Exception):
    pass


class Subscriber:
    def __init__(self, room, nick):
        self.room = room
        self.nick = nick
        self.queue = asyncio.Queue(maxsize=QUEUE_SIZE)
        self.kicked = False


class Hub:
    def __init__(self):
        self.rooms = {}
        self.last_id = 0

    def join(self, room, nick):
        room = models.check_room(room)
        nick = models.check_nick(nick)

        if self.has(room, nick):
            raise NickTaken(nick)

        self.publish(room, models.make_join(room, nick))

        subscriber = Subscriber(room, nick)
        if room not in self.rooms:
            self.rooms[room] = {}
        self.rooms[room][nick] = subscriber

        subscriber.queue.put_nowait(models.make_presence(room, self.online(room)))
        return subscriber

    def leave(self, subscriber):
        members = self.rooms.get(subscriber.room, {})
        if members.get(subscriber.nick) is not subscriber:
            return

        del members[subscriber.nick]
        if not members:
            del self.rooms[subscriber.room]

        self.publish(subscriber.room, models.make_leave(subscriber.room, subscriber.nick))

    def publish(self, room, message):
        if message["type"] == "message":
            self.last_id += 1
            message["id"] = self.last_id

        slow = []
        for subscriber in self.rooms.get(room, {}).values():
            try:
                subscriber.queue.put_nowait(message)
            except asyncio.QueueFull:
                slow.append(subscriber)

        for subscriber in slow:
            if subscriber.kicked:
                continue
            log.warning(
                "Очередь переполнена, отключаем клиента: комната=%s ник=%s",
                subscriber.room,
                subscriber.nick,
            )
            subscriber.kicked = True
            self.leave(subscriber)

    def online(self, room):
        return sorted(self.rooms.get(room, {}))

    def has(self, room, nick):
        return nick in self.rooms.get(room, {})

    def stats(self):
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


hub = Hub()
