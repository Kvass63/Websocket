const FIRST_DELAY_MS = 1000;
const MAX_DELAY_MS = 10000;

const NO_RETRY_CODES = [4000, 4001];

export class WsTransport {
  constructor() {
    this.messageCallback = null;
    this.statusCallback = null;
    this.socket = null;
    this.url = "";
    this.delay = FIRST_DELAY_MS;
    this.timer = null;
    this.closedByUser = false;
  }

  onMessage(callback) {
    this.messageCallback = callback;
  }

  onStatus(callback) {
    this.statusCallback = callback;
  }

  connect({ room, nick }) {
    const scheme = location.protocol === "https:" ? "wss" : "ws";
    this.url =
      `${scheme}://${location.host}/ws` +
      `?room=${encodeURIComponent(room)}&nick=${encodeURIComponent(nick)}`;

    this.statusCallback("connecting");
    this.openSocket();
  }

  openSocket() {
    this.socket = new WebSocket(this.url);

    this.socket.onmessage = (event) => {
      const msg = JSON.parse(event.data);
      if (msg.type === "presence") {
        this.delay = FIRST_DELAY_MS;
        this.statusCallback("open");
      }
      this.messageCallback(msg);
    };

    this.socket.onclose = (event) => {
      console.log(`WebSocket закрыт: код ${event.code}, причина "${event.reason}"`);

      if (this.closedByUser || NO_RETRY_CODES.includes(event.code)) {
        this.statusCallback("closed", event.reason);
        return;
      }

      this.statusCallback("reconnecting", event.reason);
      console.log(`Переподключение через ${this.delay} мс`);
      this.timer = setTimeout(() => {
        this.timer = null;
        this.openSocket();
      }, this.delay);
      this.delay = Math.min(this.delay * 2, MAX_DELAY_MS);
    };
  }

  send(text) {
    this.socket.send(JSON.stringify({ text }));
  }

  close() {
    this.closedByUser = true;
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
      this.statusCallback("closed");
    } else {
      this.socket.close(1000);
    }
  }
}
