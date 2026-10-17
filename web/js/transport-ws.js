// Транспорт WebSocket (участник A).
//
// Одно соединение new WebSocket(url) работает в обе стороны:
// сервер присылает сообщения комнаты (событие message),
// а мы отправляем свои через socket.send().
//
// Переподключение у WebSocket не встроено, поэтому пишем его сами:
// если соединение оборвалось, ждём и пробуем снова, каждый раз
// увеличивая паузу (1 с, 2 с, 4 с, 8 с, дальше 10 с). Растущая пауза
// нужна, чтобы после падения сервера клиенты не засыпали его попытками.

const FIRST_DELAY_MS = 1000;
const MAX_DELAY_MS = 10000;

// Коды закрытия, после которых переподключаться бессмысленно:
// с теми же ником и комнатой сервер откажет снова.
// 4000 — некорректный ник или комната, 4001 — ник занят.
const NO_RETRY_CODES = [4000, 4001];

export class WsTransport {
  constructor() {
    this.messageCallback = null;
    this.statusCallback = null;
    this.socket = null;
    this.url = "";
    this.delay = FIRST_DELAY_MS; // пауза перед следующей попыткой
    this.timer = null;           // таймер ожидания попытки или null
    this.closedByUser = false;   // true после close(): больше не переподключаемся
  }

  onMessage(callback) {
    this.messageCallback = callback;
  }

  onStatus(callback) {
    this.statusCallback = callback;
  }

  connect({ room, nick }) {
    // ws:// для обычной страницы, wss:// (шифрованный) для https://.
    // location.host — адрес и порт, с которых открыта страница.
    const scheme = location.protocol === "https:" ? "wss" : "ws";
    this.url =
      `${scheme}://${location.host}/ws` +
      `?room=${encodeURIComponent(room)}&nick=${encodeURIComponent(nick)}`;

    this.statusCallback("connecting");
    this.openSocket();
  }

  // Открыть новое соединение. Вызывается при входе и при каждой
  // попытке переподключения.
  openSocket() {
    this.socket = new WebSocket(this.url);

    // onopen не используем. Сервер принимает соединение даже тогда, когда
    // сразу закроет его с кодом 4001, и статус «на связи» мигнул бы зря.
    // Настоящий вход в комнату — первое сообщение presence: сервер
    // присылает его сразу после hub.join.
    //
    // Обработчики — стрелочные функции: внутри них this остаётся
    // нашим объектом WsTransport, а не объектом WebSocket.
    this.socket.onmessage = (event) => {
      // event.data — строка JSON, которую сервер отправил через send_json.
      const msg = JSON.parse(event.data);
      if (msg.type === "presence") {
        this.delay = FIRST_DELAY_MS; // вход удался, паузу сбрасываем
        this.statusCallback("open");
      }
      this.messageCallback(msg);
    };

    // onclose вызывается при любом закрытии: нашем, серверном или из-за
    // обрыва сети. Код и причину пишем в консоль браузера (F12):
    // 4000, 4001 — см. NO_RETRY_CODES, 4002 — медленный клиент,
    // 1000 — обычное закрытие, 1006 — обрыв, 1012 — сервер перезапускается.
    this.socket.onclose = (event) => {
      console.log(`WebSocket закрыт: код ${event.code}, причина "${event.reason}"`);

      if (this.closedByUser || NO_RETRY_CODES.includes(event.code)) {
        this.statusCallback("closed");
        return;
      }

      this.statusCallback("reconnecting");
      console.log(`Переподключение через ${this.delay} мс`);
      this.timer = setTimeout(() => {
        this.timer = null;
        this.openSocket();
      }, this.delay);
      this.delay = Math.min(this.delay * 2, MAX_DELAY_MS);
    };
  }

  send(text) {
    // По сети уходит строка JSON, сервер читает её через receive_text.
    this.socket.send(JSON.stringify({ text }));
  }

  close() {
    this.closedByUser = true;
    if (this.timer !== null) {
      // Сейчас ждём следующей попытки: соединения нет, закрывать нечего.
      clearTimeout(this.timer);
      this.timer = null;
      this.statusCallback("closed");
    } else {
      // Обычное закрытие (код 1000). После него сработает onclose
      // и сообщит интерфейсу статус 'closed'.
      this.socket.close();
    }
  }
}
