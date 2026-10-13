// Логика интерфейса чата.
//
// Этот файл ничего не знает про WebSocket или EventSource. Он работает
// с объектом-транспортом, у которого есть пять методов:
//   connect({ room, nick })  открыть соединение
//   send(text)               отправить сообщение
//   close()                  закрыть соединение
//   onMessage(callback)      callback(msg) вызывается на каждое сообщение сервера
//   onStatus(callback)       callback(status), где status — одна из строк
//                            'connecting' | 'open' | 'reconnecting' | 'closed'
//
// Договорённость: когда соединение закрыто окончательно (по кнопке «Выйти»
// или из-за ошибки, после которой переподключаться нельзя), транспорт
// обязательно сообщает статус 'closed'.

import { WsTransport } from "./transport-ws.js";
import { SseTransport } from "./transport-sse.js";

// Значение из выпадающего списка -> класс транспорта.
const TRANSPORTS = {
  ws: WsTransport,
  sse: SseTransport,
};

const STATUS_TEXT = {
  connecting: "подключение…",
  open: "на связи",
  reconnecting: "переподключение…",
  closed: "отключено",
};

// Элементы страницы.
const nickInput = document.getElementById("nick");
const roomInput = document.getElementById("room");
const transportSelect = document.getElementById("transport");
const connectButton = document.getElementById("connect");
const statusLabel = document.getElementById("status");
const feed = document.getElementById("feed");
const onlineCount = document.getElementById("online-count");
const onlineList = document.getElementById("online-list");
const sendForm = document.getElementById("send-form");
const textInput = document.getElementById("text");
const sendButton = document.getElementById("send");

let transport = null; // текущий транспорт или null, если мы не в чате
let online = [];      // ники тех, кто сейчас в комнате

// Поля можно заполнить через адрес страницы:
// http://localhost:8000/?transport=sse&room=general&nick=alice
const params = new URLSearchParams(location.search);
if (params.get("transport") in TRANSPORTS) {
  transportSelect.value = params.get("transport");
}
if (params.get("room")) {
  roomInput.value = params.get("room");
}
if (params.get("nick")) {
  nickInput.value = params.get("nick");
}

// ---------- Отрисовка ----------

function formatTime(ts) {
  return new Date(ts).toLocaleTimeString();
}

// Добавить строку в ленту и прокрутить ленту вниз.
function appendToFeed(line) {
  feed.append(line);
  feed.scrollTop = feed.scrollHeight;
}

// Текст от пользователей вставляем только через textContent:
// так HTML-теги в сообщении останутся обычным текстом и не выполнятся.
function addMessage(msg) {
  const time = document.createElement("span");
  time.className = "time";
  time.textContent = formatTime(msg.ts);

  const nick = document.createElement("span");
  nick.className = "nick";
  nick.textContent = msg.nick + ":";

  const text = document.createElement("span");
  text.textContent = msg.text;

  const line = document.createElement("div");
  line.className = "line";
  line.append(time, nick, text);
  appendToFeed(line);
}

// Служебная строка: «alice вошёл в комнату» и подобные.
function addSystemLine(text) {
  const line = document.createElement("div");
  line.className = "line system";
  line.textContent = text;
  appendToFeed(line);
}

function renderOnline() {
  onlineList.replaceChildren(); // убрать старые пункты списка
  for (const nick of online) {
    const item = document.createElement("li");
    item.textContent = nick;
    onlineList.append(item);
  }
  onlineCount.textContent = online.length;
}

// ---------- Обработчики событий транспорта ----------

function handleMessage(msg) {
  if (msg.type === "message") {
    addMessage(msg);
  } else if (msg.type === "presence") {
    // Полный список приходит один раз при входе (и при переподключении).
    online = msg.online;
    renderOnline();
  } else if (msg.type === "join") {
    if (!online.includes(msg.nick)) {
      online.push(msg.nick);
      online.sort();
    }
    renderOnline();
    addSystemLine(msg.nick + " вошёл в комнату");
  } else if (msg.type === "leave") {
    online = online.filter((nick) => nick !== msg.nick);
    renderOnline();
    addSystemLine(msg.nick + " вышел из комнаты");
  }
}

function handleStatus(status) {
  statusLabel.textContent = STATUS_TEXT[status];
  statusLabel.className = "status " + status;

  // Писать можно только при открытом соединении.
  const isOpen = status === "open";
  textInput.disabled = !isOpen;
  sendButton.disabled = !isOpen;

  // Пока мы в чате, ник, комнату и транспорт менять нельзя.
  const isClosed = status === "closed";
  nickInput.disabled = !isClosed;
  roomInput.disabled = !isClosed;
  transportSelect.disabled = !isClosed;
  connectButton.textContent = isClosed ? "Войти" : "Выйти";

  if (isClosed) {
    transport = null;
    online = [];
    renderOnline();
  } else if (isOpen) {
    textInput.focus();
  }
}

// ---------- Действия пользователя ----------

function connect() {
  const nick = nickInput.value.trim();
  const room = roomInput.value.trim();
  if (!nick || !room) {
    addSystemLine("Введите ник и название комнаты.");
    return;
  }

  feed.replaceChildren(); // очистить ленту от прошлого сеанса

  const TransportClass = TRANSPORTS[transportSelect.value];
  transport = new TransportClass();
  transport.onMessage(handleMessage);
  transport.onStatus(handleStatus);
  transport.connect({ room, nick });
}

// Одна кнопка работает и как «Войти», и как «Выйти».
connectButton.addEventListener("click", () => {
  if (transport) {
    transport.close();
  } else {
    connect();
  }
});

sendForm.addEventListener("submit", (event) => {
  event.preventDefault(); // иначе браузер перезагрузит страницу
  const text = textInput.value.trim();
  if (!text || !transport) {
    return;
  }
  transport.send(text);
  textInput.value = "";
  textInput.focus();
});
