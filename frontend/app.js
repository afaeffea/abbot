const callButton = document.getElementById("call-button");
const callStateEl = document.getElementById("call-state");
const chatBox = document.getElementById("chat-box");

const PLAYBACK_SAMPLE_RATE = 24000; // must match cartesia_realtime.TTS_SAMPLE_RATE on the backend

let ws = null;
let audioContext = null;
let mediaStream = null;
let workletNode = null;
let nextStartTime = 0;
let playbackDoneTimer = null;
let activeSources = [];
let botIsSpeaking = false;

let currentPartialEl = null;
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

function updateCallState(text, live) {
  callStateEl.textContent = text || "";
  if (live) callStateEl.dataset.live = live;
  else delete callStateEl.dataset.live;
}

function setButtonState(state, label) {
  callButton.dataset.state = state;
  callButton.textContent = label;
}

function resetToIdle() {
  setButtonState("idle", "📞 Начать звонок");
  updateCallState("");
  currentPartialEl = null;
  currentBotEl = null;
}

async function startCall() {
  setButtonState("connecting", "📞 Соединяюсь…");

  try {
    mediaStream = await navigator.mediaDevices.getUserMedia({
      audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true },
    });
  } catch (err) {
    addMessage(`Ошибка доступа к микрофону: ${err.message}`, "msg-error");
    resetToIdle();
    return;
  }

  // Created synchronously inside the click handler's call chain, so it starts
  // unlocked (not suspended) by the browser's autoplay policy.
  audioContext = new AudioContext();
  nextStartTime = 0;

  try {
    await audioContext.audioWorklet.addModule("mic-worklet.js");
  } catch (err) {
    addMessage(`Не удалось загрузить аудио-обработчик: ${err.message}`, "msg-error");
    resetToIdle();
    return;
  }

  const source = audioContext.createMediaStreamSource(mediaStream);
  workletNode = new AudioWorkletNode(audioContext, "mic-processor", {
    processorOptions: {
      targetSampleRate: 16000,
      energyThreshold: 0.02,
      speechStartMs: 150,
      silenceHangoverMs: 1300,
      bargeInEnergyThreshold: 0.09,
      bargeInSpeechStartMs: 500,
    },
  });
  workletNode.port.onmessage = handleWorkletMessage;

  // Keep the audio graph alive without producing audible mic feedback.
  const silentGain = audioContext.createGain();
  silentGain.gain.value = 0;
  source.connect(workletNode);
  workletNode.connect(silentGain);
  silentGain.connect(audioContext.destination);

  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws/call`);
  ws.binaryType = "arraybuffer";

  ws.onopen = () => {
    setButtonState("active", "📴 Завершить звонок");
    updateCallState("Соединяюсь с ботом…");
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
    if (callButton.dataset.state !== "idle") {
      addMessage("Звонок завершён", "msg-status");
    }
    cleanupCallResources();
    resetToIdle();
  };
}

function handleWorkletMessage(event) {
  const msg = event.data;
  if (msg.type === "audio") {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(msg.buffer);
    }
  } else if (msg.type === "vad") {
    if (msg.event === "speech_start") {
      updateCallState("Вы говорите…", "user");
      stopBotPlayback();
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "interrupt" }));
      }
    } else if (msg.event === "speech_end") {
      updateCallState("Обрабатываю…");
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "end_of_utterance" }));
      }
    }
  }
}

function handleServerEvent(data) {
  switch (data.type) {
    case "status":
      addMessage(data.text, "msg-status");
      if (data.text.includes("Gemini")) updateCallState("Бот думает…");
      break;
    case "user_partial":
      if (!currentPartialEl) currentPartialEl = addMessage(data.text, "msg-partial");
      else {
        currentPartialEl.textContent = data.text;
        scrollChat();
      }
      break;
    case "user_final":
      if (currentPartialEl) {
        currentPartialEl.remove();
        currentPartialEl = null;
      }
      addMessage(data.text, "msg-user");
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
      break;
  }
}

function setBotSpeaking(value) {
  if (botIsSpeaking === value) return;
  botIsSpeaking = value;
  if (workletNode) {
    workletNode.port.postMessage({ type: "bot_speaking", value });
  }
}

function stopBotPlayback() {
  // Called the instant local VAD detects the caller talking, whether or not
  // the bot actually has audio queued right now — cheap no-op if it doesn't.
  activeSources.forEach((source) => {
    try {
      source.stop();
    } catch (err) {
      /* already stopped/ended */
    }
  });
  activeSources = [];
  if (audioContext) nextStartTime = audioContext.currentTime;
  if (playbackDoneTimer) {
    clearTimeout(playbackDoneTimer);
    playbackDoneTimer = null;
  }
  setBotSpeaking(false);
}

function scheduleBotPlaybackDone() {
  // "bot_done" only means Cartesia finished *generating* audio for this turn —
  // the browser is usually still playing back already-queued chunks for a few
  // more seconds. Tell the server once actual playback catches up to
  // nextStartTime, so it only unmutes the mic after the bot is truly silent
  // (otherwise the mic picks up the bot's own trailing audio and re-triggers).
  if (playbackDoneTimer) clearTimeout(playbackDoneTimer);
  const remainingMs = Math.max(0, (nextStartTime - audioContext.currentTime) * 1000) + 150;
  playbackDoneTimer = setTimeout(() => {
    playbackDoneTimer = null;
    setBotSpeaking(false);
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: "bot_playback_done" }));
    }
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

  setBotSpeaking(true);
  updateCallState("Бот говорит…", "bot");
}

function cleanupCallResources() {
  if (playbackDoneTimer) {
    clearTimeout(playbackDoneTimer);
    playbackDoneTimer = null;
  }
  activeSources = [];
  botIsSpeaking = false;
  if (mediaStream) {
    mediaStream.getTracks().forEach((track) => track.stop());
    mediaStream = null;
  }
  if (workletNode) {
    workletNode.disconnect();
    workletNode = null;
  }
  if (audioContext) {
    audioContext.close();
    audioContext = null;
  }
  ws = null;
}

function endCall() {
  if (ws) {
    ws.onclose = null;
    ws.close();
  }
  cleanupCallResources();
  addMessage("Звонок завершён", "msg-status");
  resetToIdle();
}

callButton.addEventListener("click", () => {
  const state = callButton.dataset.state;
  if (state === "idle") {
    startCall();
  } else if (state === "active") {
    endCall();
  }
});
