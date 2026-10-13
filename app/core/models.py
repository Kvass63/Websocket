"""Формат сообщений чата (общий для WebSocket и SSE).

Сообщения — обычные словари. Функции ниже собирают их в одном месте,
чтобы оба транспорта отправляли клиентам одинаковый JSON.
"""

import time

# Ограничения на длину строк. Проверяются в функциях check_*.
MAX_NICK_LEN = 20
MAX_ROOM_LEN = 30
MAX_TEXT_LEN = 2000


def now_ms():
    """Текущее время сервера в миллисекундах."""
    return int(time.time() * 1000)


def check_string(value, max_len):
    """Проверить строку, пришедшую от клиента.

    Возвращает строку без пробелов по краям. Если это не строка,
    она пустая или слишком длинная, выбрасывает ValueError.
    """
    if not isinstance(value, str):
        raise ValueError("ожидалась строка")
    value = value.strip()
    if not value:
        raise ValueError("пустая строка")
    if len(value) > max_len:
        raise ValueError(f"строка длиннее {max_len} символов")
    return value


def check_nick(nick):
    return check_string(nick, MAX_NICK_LEN)


def check_room(room):
    return check_string(room, MAX_ROOM_LEN)


def check_text(text):
    return check_string(text, MAX_TEXT_LEN)


def make_message(room, nick, text, cts=None):
    """Обычное сообщение чата.

    Поле id здесь равно 0: настоящий номер проставит Hub в момент рассылки.
    cts — время клиента, сервер возвращает его как есть (нужно бенчмарку).
    """
    message = {
        "type": "message",
        "id": 0,
        "room": room,
        "nick": nick,
        "text": text,
        "ts": now_ms(),
    }
    if cts is not None:
        message["cts"] = cts
    return message


def make_join(room, nick):
    """Событие: пользователь вошёл в комнату."""
    return {"type": "join", "room": room, "nick": nick, "ts": now_ms()}


def make_leave(room, nick):
    """Событие: пользователь вышел из комнаты."""
    return {"type": "leave", "room": room, "nick": nick, "ts": now_ms()}


def make_presence(room, online):
    """Список тех, кто сейчас в комнате. Отправляется только вошедшему."""
    return {"type": "presence", "room": room, "online": online, "ts": now_ms()}
