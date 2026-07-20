const toggleButton = document.getElementById("chat-toggle");
const chatStateEl = document.getElementById("chat-state");
const chatBox = document.getElementById("chat-box");
const chatForm = document.getElementById("chat-form");
const chatInput = document.getElementById("chat-input");
const chatSend = document.getElementById("chat-send");

const PLAYBACK_SAMPLE_RATE = 24000; // must match cartesia_realtime.TTS_SAMPLE_RATE on the backend

let ws = null;
let audioContext = null;
let nextStartTime = 0;
let playbackDoneTimer = null;
let activeSources = [];
let currentBotEl = null;

function addMessage(text, className) {
  const el = document.createElement("div");
  el.className = `msg ${className}`;
  el.textContent = text;
  chatBox.appendChild(el);
  chatBox.scrollTop = chatBox.scrollHeight;
  return el;
}

function scrollChat() {
  chatBox.scrollTop = chatBox.scrollHeight;
}

function updateState(text) {
  chatStateEl.textContent = text || "";
}

function setButtonState(state, label) {
  toggleButton.dataset.state = state;
  toggleButton.textContent = label;
}

function setInputEnabled(enabled) {
  chatInput.disabled = !enabled;
  chatSend.disabled = !enabled;
  if (enabled) chatInput.focus();
}

function resetToIdle() {
  setButtonState("idle", "💬 Начать чат");
  setInputEnabled(false);
  updateState("");
  currentBotEl = null;
}

function startChat() {
  setButtonState("connecting", "💬 Соединяюсь…");

  // Created synchronously inside the click handler, so it starts unlocked
  // (not suspended) by the browser's autoplay policy — same trick app.js
  // uses for the voice call.
  audioContext = new AudioContext();
  nextStartTime = 0;

  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws/chat`);
  ws.binaryType = "arraybuffer";

  ws.onopen = () => {
    setButtonState("active", "💬 Завершить чат");
    updateState("Соединяюсь с ботом…");
  };

  ws.onmessage = (event) => {
    if (typeof event.data === "string") {
      handleServerEvent(JSON.parse(event.data));
    } else {
      playAudioChunk(event.data);
    }
  };

  ws.onerror = () => {
    addMessage("Ошибка соединения с сервером", "msg-error");
  };

  ws.onclose = () => {
    if (toggleButton.dataset.state !== "idle") {
      addMessage("Чат завершён", "msg-status");
    }
    cleanupChatResources();
    resetToIdle();
  };
}

function handleServerEvent(data) {
  switch (data.type) {
    case "status":
      addMessage(data.text, "msg-status");
      if (data.text.includes("Gemini")) updateState("Бот думает…");
      break;
    case "user_final":
      // Already shown optimistically when the message was sent.
      break;
    case "bot_text_delta":
      if (!currentBotEl) currentBotEl = addMessage("", "msg-bot");
      currentBotEl.textContent += data.text;
      scrollChat();
      break;
    case "bot_text_done":
      currentBotEl = null;
      break;
    case "bot_done":
      scheduleBotPlaybackDone();
      break;
    case "error":
      addMessage(data.text, "msg-error");
      setInputEnabled(true);
      updateState("");
      break;
  }
}

function scheduleBotPlaybackDone() {
  // "bot_done" only means Cartesia finished *generating* audio for this turn —
  // playback of already-queued chunks can still be running for a bit longer.
  // Only re-enable typing once real playback catches up to nextStartTime.
  if (playbackDoneTimer) clearTimeout(playbackDoneTimer);
  const remainingMs = Math.max(0, (nextStartTime - audioContext.currentTime) * 1000) + 150;
  playbackDoneTimer = setTimeout(() => {
    playbackDoneTimer = null;
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: "bot_playback_done" }));
    }
    updateState("Ваш ход");
    setInputEnabled(true);
  }, remainingMs);
}

function playAudioChunk(arrayBuffer) {
  const int16 = new Int16Array(arrayBuffer);
  const float32 = new Float32Array(int16.length);
  for (let i = 0; i < int16.length; i++) float32[i] = int16[i] / 32768;

  const buffer = audioContext.createBuffer(1, float32.length, PLAYBACK_SAMPLE_RATE);
  buffer.copyToChannel(float32, 0);

  const source = audioContext.createBufferSource();
  source.buffer = buffer;
  source.connect(audioContext.destination);
  activeSources.push(source);
  source.onended = () => {
    activeSources = activeSources.filter((s) => s !== source);
  };

  const startAt = Math.max(audioContext.currentTime + 0.05, nextStartTime);
  source.start(startAt);
  nextStartTime = startAt + buffer.duration;

  updateState("Бот говорит…");
}

function cleanupChatResources() {
  if (playbackDoneTimer) {
    clearTimeout(playbackDoneTimer);
    playbackDoneTimer = null;
  }
  activeSources = [];
  if (audioContext) {
    audioContext.close();
    audioContext = null;
  }
  ws = null;
}

function endChat() {
  if (ws) {
    ws.onclose = null;
    ws.close();
  }
  cleanupChatResources();
  addMessage("Чат завершён", "msg-status");
  resetToIdle();
}

toggleButton.addEventListener("click", () => {
  const state = toggleButton.dataset.state;
  if (state === "idle") startChat();
  else if (state === "active") endChat();
});

chatForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const text = chatInput.value.trim();
  if (!text || !ws || ws.readyState !== WebSocket.OPEN) return;
  addMessage(text, "msg-user");
  ws.send(JSON.stringify({ type: "user_text", text }));
  chatInput.value = "";
  setInputEnabled(false);
  updateState("Обрабатываю…");
});
