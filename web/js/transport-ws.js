// Транспорт WebSocket (участник A).
//
// ЗАГЛУШКА шага 1: к серверу не подключается. Отправленное сообщение
// сразу возвращается в эту же вкладку, чтобы можно было проверить интерфейс.
//
// Что нужно сделать на шаге 2:
// 1. В connect собрать адрес:
//      const scheme = location.protocol === "https:" ? "wss" : "ws";
//      const url = `${scheme}://${location.host}/ws?room=${encodeURIComponent(room)}&nick=${encodeURIComponent(nick)}`;
//    и создать this.socket = new WebSocket(url).
// 2. Повесить обработчики (стрелочные функции, чтобы внутри работал this):
//      this.socket.onopen    = () => this.statusCallback("open");
//      this.socket.onmessage = (event) => this.messageCallback(JSON.parse(event.data));
//      this.socket.onclose   = (event) => { ...переподключение или "closed"... };
// 3. В send вызвать this.socket.send(JSON.stringify({ text })).
// 4. В close запомнить, что закрытие было по нашей воле, и вызвать this.socket.close().
// 5. Переподключение (шаг 3): таймер setTimeout с растущей задержкой.
//    После кодов 4000 и 4001 и после close() переподключаться не надо.

export class WsTransport {
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
      text: "Транспорт WebSocket ещё не реализован: сообщения видны только в этой вкладке.",
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
