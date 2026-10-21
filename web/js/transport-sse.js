// Транспорт SSE (участник B).
//
// Сервер -> клиент: EventSource, встроенный в браузер объект. Он открывает
// долгий GET /sse/events, сам разбирает строки "data: ..." и вызывает
// onmessage на каждое событие. При обрыве сети переподключается сам.
//
// Клиент -> сервер: обычный fetch(POST /sse/send). Своё сообщение мы
// увидим, когда сервер пришлёт его обратно через стрим, как всем.

export class SseTransport {
  constructor() {
    this.messageCallback = null;
    this.statusCallback = null;
    this.source = null;   // объект EventSource
    this.stopped = false; // true после close() или окончательной ошибки
    this.room = "";
    this.nick = "";
  }

  onMessage(callback) {
    this.messageCallback = callback;
  }

  onStatus(callback) {
    this.statusCallback = callback;
  }

  // Часть адреса "?room=...&nick=...".
  // encodeURIComponent экранирует пробелы, &, ? и русские буквы.
  query() {
    return `?room=${encodeURIComponent(this.room)}&nick=${encodeURIComponent(this.nick)}`;
  }

  connect({ room, nick }) {
    this.room = room;
    this.nick = nick;
    this.statusCallback("connecting");

    // EventSource умеет только GET и не позволяет задать свои заголовки,
    // поэтому комната и ник передаются в адресе.
    this.source = new EventSource("/sse/events" + this.query());

    // Стрелочные функции, чтобы внутри this указывал на наш объект.
    this.source.onopen = () => this.statusCallback("open");

    this.source.onmessage = (event) => {
      // event.data — текст после "data: ", в нём JSON.
      this.messageCallback(JSON.parse(event.data));
    };

    // Событие "event: kicked" не попадает в onmessage: события со своим
    // именем ловятся только через addEventListener. Сервер шлёт его, когда
    // выкинул нас как медленного клиента, и сразу закрывает стрим. После
    // этого EventSource переподключится сам, и мы войдём в комнату заново.
    this.source.addEventListener("kicked", () => {
      this.statusCallback("reconnecting", "отключён как медленный клиент");
    });

    this.source.onerror = () => {
      if (this.source.readyState === EventSource.CONNECTING) {
        // Сеть оборвалась или сервер недоступен: браузер сам
        // попробует подключиться снова через несколько секунд.
        this.statusCallback("reconnecting");
      } else {
        // CLOSED: сервер ответил, но не статусом 200 (409, 422, 500...).
        // EventSource больше не переподключается и код ответа не сообщает.
        this.source = null;
        this.stopped = true;
        this.reportFailure();
      }
    };
  }

  // Узнать у сервера причину отказа и сообщить статус "closed".
  async reportFailure() {
    let reason = "не удалось подключиться к серверу";
    try {
      // /sse/check проверяет то же, что вход, но в комнату не добавляет.
      // Поэтому повторный запрос безопасен, даже если ник уже освободился.
      const response = await fetch("/sse/check" + this.query());
      if (response.status === 409) {
        reason = "ник уже занят в этой комнате";
      } else if (response.status === 422) {
        reason = "некорректный ник или название комнаты";
      }
      // 200: за время проверки ник освободился, причину уже не узнать,
      // остаётся общее сообщение.
    } catch {
      // Сервер не ответил на проверку: тоже общее сообщение.
    }
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
      // Если сервер недоступен, fetch не возвращает ответ, а бросает
      // исключение. Без catch в консоли было бы «Uncaught (in promise)».
      console.warn("POST /sse/send: сервер не ответил", error);
    }
  }

  close() {
    if (this.stopped) {
      // Уже закрыто (или идёт проверка причины, статус сообщит она).
      return;
    }
    this.stopped = true;
    if (this.source) {
      this.source.close();
      this.source = null;
    }
    // После close() EventSource никаких событий не присылает,
    // поэтому статус сообщаем сами.
    this.statusCallback("closed");
  }
}
