// Транспорт SSE (участник B).
//
// ЗАГЛУШКА шага 1: к серверу не подключается. Отправленное сообщение
// сразу возвращается в эту же вкладку, чтобы можно было проверить интерфейс.
//
// Что нужно сделать на шаге 2:
// 1. В connect собрать адрес:
//      const url = `/sse/events?room=${encodeURIComponent(room)}&nick=${encodeURIComponent(nick)}`;
//    и создать this.source = new EventSource(url).
// 2. Повесить обработчики (стрелочные функции, чтобы внутри работал this):
//      this.source.onopen    = () => this.statusCallback("open");
//      this.source.onmessage = (event) => this.messageCallback(JSON.parse(event.data));
//      this.source.onerror   = () => { ... };
//    В onerror смотреть this.source.readyState:
//      EventSource.CONNECTING — браузер сам переподключается, статус "reconnecting";
//      EventSource.CLOSED     — сервер отказал (статус не 200), статус "closed".
// 3. В send отправить запрос:
//      fetch("/sse/send", {
//        method: "POST",
//        headers: { "Content-Type": "application/json" },
//        body: JSON.stringify({ room: this.room, nick: this.nick, text }),
//      });
// 4. В close вызвать this.source.close() и сообщить статус "closed":
//    сам EventSource после close() никаких событий не присылает.

export class SseTransport {
  constructor() {
    this.messageCallback = null;
    this.statusCallback = null;
    this.room = "";
    this.nick = "";
  }

  onMessage(callback) {
    this.messageCallback = callback;
  }

  onStatus(callback) {
    this.statusCallback = callback;
  }

  connect({ room, nick }) {
    this.room = room;
    this.nick = nick;
    this.statusCallback("connecting");
    this.statusCallback("open");
    this.messageCallback({ type: "presence", room, online: [nick], ts: Date.now() });
    this.messageCallback({
      type: "message",
      id: 0,
      room,
      nick: "заглушка",
      text: "Транспорт SSE ещё не реализован: сообщения видны только в этой вкладке.",
      ts: Date.now(),
    });
  }

  send(text) {
    this.messageCallback({
      type: "message",
      id: 0,
      room: this.room,
      nick: this.nick,
      text,
      ts: Date.now(),
    });
  }

  close() {
    this.statusCallback("closed");
  }
}
