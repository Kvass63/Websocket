import { WsTransport } from "./transport-ws.js";
import { SseTransport } from "./transport-sse.js";

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

let transport = null;
let online = [];

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

function formatTime(ts) {
  return new Date(ts).toLocaleTimeString();
}

function appendToFeed(line) {
  feed.append(line);
  feed.scrollTop = feed.scrollHeight;
}

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

function addSystemLine(text) {
  const line = document.createElement("div");
  line.className = "line system";
  line.textContent = text;
  appendToFeed(line);
}

function renderOnline() {
  onlineList.replaceChildren();
  for (const nick of online) {
    const item = document.createElement("li");
    item.textContent = nick;
    onlineList.append(item);
  }
  onlineCount.textContent = online.length;
}

function handleMessage(msg) {
  if (msg.type === "message") {
    addMessage(msg);
  } else if (msg.type === "presence") {
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

function handleStatus(status, reason) {
  statusLabel.textContent = STATUS_TEXT[status];
  statusLabel.className = "status " + status;

  if (reason) {
    addSystemLine(reason);
  }

  const isOpen = status === "open";
  textInput.disabled = !isOpen;
  sendButton.disabled = !isOpen;

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

function connect() {
  const nick = nickInput.value.trim();
  const room = roomInput.value.trim();
  if (!nick || !room) {
    addSystemLine("Введите ник и название комнаты.");
    return;
  }

  feed.replaceChildren();

  const TransportClass = TRANSPORTS[transportSelect.value];
  transport = new TransportClass();
  transport.onMessage(handleMessage);
  transport.onStatus(handleStatus);
  transport.connect({ room, nick });
}

connectButton.addEventListener("click", () => {
  if (transport) {
    transport.close();
  } else {
    connect();
  }
});

sendForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const text = textInput.value.trim();
  if (!text || !transport) {
    return;
  }
  transport.send(text);
  textInput.value = "";
  textInput.focus();
});
