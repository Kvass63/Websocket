import time

MAX_NICK_LEN = 20
MAX_ROOM_LEN = 30
MAX_TEXT_LEN = 2000


def now_ms():
    return int(time.time() * 1000)


def check_string(value, max_len):
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
    return {"type": "join", "room": room, "nick": nick, "ts": now_ms()}


def make_leave(room, nick):
    return {"type": "leave", "room": room, "nick": nick, "ts": now_ms()}


def make_presence(room, online):
    return {"type": "presence", "room": room, "online": online, "ts": now_ms()}
