const callButton = document.getElementById("call-button");
const callStateEl = document.getElementById("call-state");
const chatBox = document.getElementById("chat-box");
const chatForm = document.getElementById("chat-form");
const chatInput = document.getElementById("chat-input");
const chatSend = document.getElementById("chat-send");

const PLAYBACK_SAMPLE_RATE = 24000; // must match cartesia_realtime.TTS_SAMPLE_RATE on the backend

// Mirrors settings_store.DEFAULT_SETTINGS["vad"] on the backend — used only
// if /api/settings can't be reached, so the call can still proceed.
const DEFAULT_VAD = {
  energyThreshold: 0.02,
  speechStartMs: 150,
  silenceHangoverMs: 700,
  bargeInEnergyThreshold: 0.035,
  bargeInSpeechStartMs: 250,
};

async function fetchVadSettings() {
  // Fetched fresh per call (not cached across page views): an operator could
  // save new values on /prompt.html in another tab while this page stays
  // open, and a stale cached fetch would silently keep serving old
  // sensitivity. Never throws — any failure just falls back to defaults.
  try {
    const res = await fetch("/api/settings");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    const vad = data.settings.vad;
    return {
      energyThreshold: vad.energy_threshold,
      speechStartMs: vad.speech_start_ms,
      silenceHangoverMs: vad.silence_hangover_ms,
      bargeInEnergyThreshold: vad.barge_in_energy_threshold,
      bargeInSpeechStartMs: vad.barge_in_speech_start_ms,
    };
  } catch (err) {
    return DEFAULT_VAD;
  }
}

let ws = null;
let audioContext = null;
let mediaStream = null;
let workletNode = null;
let micSourceNode = null;
let micSilentGain = null;
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

function setInputEnabled(enabled) {
  chatInput.disabled = !enabled;
  chatSend.disabled = !enabled;
  if (enabled) chatInput.focus();
}

function resetToIdle() {
  setButtonState("idle", "📞 Начать");
  setInputEnabled(false);
  updateCallState("");
  currentPartialEl = null;
  currentBotEl = null;
}

async function startSession() {
  setButtonState("connecting", "📞 Соединяюсь…");

  // Created synchronously inside the click handler's call chain, so it starts
  // unlocked (not suspended) by the browser's autoplay policy. Also used by
  // the mic worklet below, on the same context.
  audioContext = new AudioContext();
  nextStartTime = 0;

  // Mic listens for the whole call, like an actual phone call — no manual
  // toggle. Fired without awaiting so the permission prompt/worklet setup
  // runs concurrently with the WebSocket connecting below rather than
  // delaying it; handleWorkletMessage already guards on ws being open
  // before sending anything, and if mic access fails/is denied the call
  // still proceeds text-only (enableMic reports the error and just returns).
  enableMic();

  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws/call`);
  ws.binaryType = "arraybuffer";

  ws.onopen = () => {
    setButtonState("active", "📴 Завершить");
    // Safe to enable immediately: the server treats a typed message that
    // arrives while the bot is still talking (e.g. mid-greeting) as a
    // barge-in and cancels the in-flight turn, rather than dropping it.
    setInputEnabled(true);
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
      addMessage("Сессия завершена", "msg-status");
    }
    cleanupSessionResources();
    resetToIdle();
  };
}

async function enableMic() {
  // Fired immediately, concurrently with the getUserMedia permission prompt
  // below (which is usually much slower) — resolves to DEFAULT_VAD on any
  // error, never throws, so it can't block/break mic setup.
  const vadPromise = fetchVadSettings();

  try {
    mediaStream = await navigator.mediaDevices.getUserMedia({
      audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true },
    });
  } catch (err) {
    addMessage(`Ошибка доступа к микрофону: ${err.message}`, "msg-error");
    return;
  }

  try {
    await audioContext.audioWorklet.addModule("mic-worklet.js");
  } catch (err) {
    addMessage(`Не удалось загрузить аудио-обработчик: ${err.message}`, "msg-error");
    mediaStream.getTracks().forEach((track) => track.stop());
    mediaStream = null;
    return;
  }

  // Meaning of each value (also editable on /prompt.html — "Чувствительность
  // микрофона"): energyThreshold/speechStartMs gate normal speech detection;
  // silenceHangoverMs is how long the caller must be silent before we
  // consider them done talking — the single biggest lever on perceived
  // response latency, since nothing (STT finalize, LLM, TTS) starts until
  // this elapses. bargeInEnergyThreshold/bargeInSpeechStartMs are the
  // stricter thresholds used only while the bot is speaking, guarding
  // against its own voice leaking back in through speakers and
  // self-triggering a "barge-in" (getUserMedia's own echoCancellation does
  // most of that suppression already, this is just a light extra margin).
  const vad = await vadPromise; // already in flight since before getUserMedia — usually a no-op await by now
  workletNode = new AudioWorkletNode(audioContext, "mic-processor", {
    processorOptions: { targetSampleRate: 16000, ...vad },
  });
  // The bot may already be mid-reply when the mic gets turned on — sync the
  // fresh worklet's anti-echo threshold immediately instead of waiting for
  // the next setBotSpeaking() *change* (which won't fire if the JS-side flag
  // was already true, leaving the new node on the sensitive default).
  workletNode.port.postMessage({ type: "bot_speaking", value: botIsSpeaking });
  workletNode.port.onmessage = handleWorkletMessage;

  // Keep the audio graph alive without producing audible mic feedback.
  micSourceNode = audioContext.createMediaStreamSource(mediaStream);
  micSilentGain = audioContext.createGain();
  micSilentGain.gain.value = 0;
  micSourceNode.connect(workletNode);
  workletNode.connect(micSilentGain);
  micSilentGain.connect(audioContext.destination);
}

function disableMic() {
  if (mediaStream) {
    mediaStream.getTracks().forEach((track) => track.stop());
    mediaStream = null;
  }
  if (workletNode) {
    workletNode.port.onmessage = null;
    workletNode.disconnect();
    workletNode = null;
  }
  if (micSourceNode) {
    micSourceNode.disconnect();
    micSourceNode = null;
  }
  if (micSilentGain) {
    micSilentGain.disconnect();
    micSilentGain = null;
  }
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
      if (data.text.includes("модели")) updateCallState("Бот думает…");
      break;
    case "user_partial":
      if (!currentPartialEl) currentPartialEl = addMessage(data.text, "msg-partial");
      else {
        currentPartialEl.textContent = data.text;
        scrollChat();
      }
      break;
    case "user_final":
      // Single render path for both voice (STT) and typed replies.
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
      // The server-side error path doesn't send bot_done, so the usual
      // scheduleBotPlaybackDone() unmute round-trip never fires here —
      // resync the mic's anti-echo threshold directly instead of leaving it
      // stuck on the bot-speaking-strict setting.
      setBotSpeaking(false);
      updateCallState("");
      break;
  }
}

function setBotSpeaking(value) {
  // Deliberately doesn't touch the text input's enabled state — typing (and
  // hitting send) is allowed at any time, including while the bot is
  // talking, and doubles as a barge-in just like voice does.
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

function cleanupSessionResources() {
  if (playbackDoneTimer) {
    clearTimeout(playbackDoneTimer);
    playbackDoneTimer = null;
  }
  activeSources = [];
  botIsSpeaking = false;
  disableMic();
  if (audioContext) {
    audioContext.close();
    audioContext = null;
  }
  ws = null;
}

function endSession() {
  if (ws) {
    ws.onclose = null;
    ws.close();
  }
  cleanupSessionResources();
  addMessage("Сессия завершена", "msg-status");
  resetToIdle();
}

callButton.addEventListener("click", () => {
  const state = callButton.dataset.state;
  if (state === "idle") {
    startSession();
  } else if (state === "active") {
    endSession();
  }
});

chatForm.addEventListener("submit", (event) => {
  event.preventDefault();
  const text = chatInput.value.trim();
  if (!text || !ws || ws.readyState !== WebSocket.OPEN) return;
  // Sending while the bot is still talking is a barge-in, same as voice:
  // stop whatever audio is already queued so it doesn't keep playing over
  // (and outlasting) the reply to this new message.
  if (botIsSpeaking) stopBotPlayback();
  ws.send(JSON.stringify({ type: "user_text", text }));
  chatInput.value = "";
  updateCallState("Обрабатываю…");
});
