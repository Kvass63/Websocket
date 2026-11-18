export class SseTransport {
  constructor() {
    this.messageCallback = null;
    this.statusCallback = null;
    this.source = null;
    this.stopped = false;
    this.room = "";
    this.nick = "";
  }

  onMessage(callback) {
    this.messageCallback = callback;
  }

  onStatus(callback) {
    this.statusCallback = callback;
  }

  query() {
    return `?room=${encodeURIComponent(this.room)}&nick=${encodeURIComponent(this.nick)}`;
  }

  connect({ room, nick }) {
    this.room = room;
    this.nick = nick;
    this.statusCallback("connecting");

    this.source = new EventSource("/sse/events" + this.query());

    this.source.onopen = () => this.statusCallback("open");

    this.source.onmessage = (event) => {
      this.messageCallback(JSON.parse(event.data));
    };

    this.source.addEventListener("kicked", () => {
      this.statusCallback("reconnecting", "отключён как медленный клиент");
    });

    this.source.onerror = () => {
      if (this.source.readyState === EventSource.CONNECTING) {
        this.statusCallback("reconnecting");
      } else {
        this.source = null;
        this.stopped = true;
        this.reportFailure();
      }
    };
  }

  async reportFailure() {
    let reason = "не удалось подключиться к серверу";
    try {
      const response = await fetch("/sse/check" + this.query());
      if (response.status === 409) {
        reason = "ник уже занят в этой комнате";
      } else if (response.status === 422) {
        reason = "некорректный ник или название комнаты";
      }
    } catch {}
    this.statusCallback("closed", reason);
  }

  async send(text) {
    try {
      const response = await fetch("/sse/send", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ room: this.room, nick: this.nick, text }),
      });
      if (!response.ok) {
        console.warn("POST /sse/send: ответ", response.status);
      }
    } catch (error) {
      console.warn("POST /sse/send: сервер не ответил", error);
    }
  }

  close() {
    if (this.stopped) {
      return;
    }
    this.stopped = true;
    if (this.source) {
      this.source.close();
      this.source = null;
    }
    this.statusCallback("closed");
  }
}
