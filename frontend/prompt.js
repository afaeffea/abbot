const statusEl = document.getElementById("settings-status");
const greetingEl = document.getElementById("greeting-textarea");
const personaEl = document.getElementById("persona-textarea");
const factsEl = document.getElementById("facts-textarea");
const reasoningEl = document.getElementById("reasoning-select");
const temperatureEl = document.getElementById("temperature-input");
const voiceEl = document.getElementById("voice-select");
const voiceHintEl = document.getElementById("voice-hint");
const maxTokensEl = document.getElementById("max-tokens-input");
const vadEnergyEl = document.getElementById("vad-energy");
const vadSpeechStartEl = document.getElementById("vad-speech-start");
const vadSilenceHangoverEl = document.getElementById("vad-silence-hangover");
const vadBargeEnergyEl = document.getElementById("vad-barge-energy");
const vadBargeSpeechStartEl = document.getElementById("vad-barge-speech-start");
const saveBtn = document.getElementById("settings-save");
const resetBtn = document.getElementById("settings-reset");

const allFields = [
  greetingEl, personaEl, factsEl, reasoningEl, temperatureEl, voiceEl, maxTokensEl,
  vadEnergyEl, vadSpeechStartEl, vadSilenceHangoverEl, vadBargeEnergyEl, vadBargeSpeechStartEl,
];

const testChatBox = document.getElementById("test-chat-box");
const testChatForm = document.getElementById("test-chat-form");
const testChatInput = document.getElementById("test-chat-input");
const testChatSend = document.getElementById("test-chat-send");
const testChatClear = document.getElementById("test-chat-clear");

let testHistory = [];

function setFieldsEnabled(enabled) {
  allFields.forEach((el) => { el.disabled = !enabled; });
  saveBtn.disabled = !enabled;
  resetBtn.disabled = !enabled;
}

function applyState(data) {
  const s = data.settings;
  greetingEl.value = s.greeting;
  personaEl.value = s.persona_prompt;
  factsEl.value = s.business_facts;
  reasoningEl.value = s.reasoning_effort;
  temperatureEl.value = s.temperature === null || s.temperature === undefined ? "" : s.temperature;
  voiceEl.value = s.voice_id || "";
  maxTokensEl.value = s.max_reply_tokens;
  vadEnergyEl.value = s.vad.energy_threshold;
  vadSpeechStartEl.value = s.vad.speech_start_ms;
  vadSilenceHangoverEl.value = s.vad.silence_hangover_ms;
  vadBargeEnergyEl.value = s.vad.barge_in_energy_threshold;
  vadBargeSpeechStartEl.value = s.vad.barge_in_speech_start_ms;

  statusEl.textContent = data.is_customized
    ? "Изменено (отличается от настроек по умолчанию). Изменения применяются к новым звонкам сразу после сохранения."
    : "Настройки по умолчанию (не изменены).";
  statusEl.className = "prompt-status" + (data.is_customized ? " prompt-status-custom" : "");
}

async function loadVoices(selectedVoiceId) {
  try {
    const res = await fetch("/api/voices");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    for (const voice of data.voices) {
      const option = document.createElement("option");
      option.value = voice.id;
      option.textContent = voice.name;
      voiceEl.appendChild(option);
    }
    voiceEl.value = selectedVoiceId || "";
    voiceHintEl.textContent = "";
  } catch (err) {
    // Degrades gracefully: "Авто" stays selectable, rest of the page still works.
    voiceHintEl.textContent = `Не удалось загрузить список голосов: ${err.message}. Можно оставить «Авто».`;
  }
}

async function loadSettings() {
  statusEl.textContent = "Загрузка…";
  try {
    const res = await fetch("/api/settings");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    applyState(data);
    await loadVoices(data.settings.voice_id);
    setFieldsEnabled(true);
    seedTestChatWithGreeting();
  } catch (err) {
    statusEl.textContent = `Ошибка загрузки: ${err.message}`;
    statusEl.className = "prompt-status prompt-status-error";
  }
}

function readFormSettings() {
  return {
    greeting: greetingEl.value,
    persona_prompt: personaEl.value,
    business_facts: factsEl.value,
    reasoning_effort: reasoningEl.value,
    temperature: temperatureEl.value === "" ? null : Number(temperatureEl.value),
    voice_id: voiceEl.value || null,
    max_reply_tokens: Number(maxTokensEl.value),
    vad: {
      energy_threshold: Number(vadEnergyEl.value),
      speech_start_ms: Number(vadSpeechStartEl.value),
      silence_hangover_ms: Number(vadSilenceHangoverEl.value),
      barge_in_energy_threshold: Number(vadBargeEnergyEl.value),
      barge_in_speech_start_ms: Number(vadBargeSpeechStartEl.value),
    },
  };
}

async function saveSettings() {
  saveBtn.disabled = true;
  statusEl.textContent = "Сохраняю…";
  try {
    const res = await fetch("/api/settings", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(readFormSettings()),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
    applyState(data);
  } catch (err) {
    statusEl.textContent = `Ошибка сохранения: ${err.message}`;
    statusEl.className = "prompt-status prompt-status-error";
  } finally {
    saveBtn.disabled = false;
  }
}

async function resetSettings() {
  if (!confirm("Восстановить настройки по умолчанию? Текущие изменения будут потеряны.")) return;
  resetBtn.disabled = true;
  statusEl.textContent = "Восстанавливаю…";
  try {
    const res = await fetch("/api/settings/reset", { method: "POST" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    applyState(data);
    voiceEl.value = data.settings.voice_id || "";
  } catch (err) {
    statusEl.textContent = `Ошибка восстановления: ${err.message}`;
    statusEl.className = "prompt-status prompt-status-error";
  } finally {
    resetBtn.disabled = false;
  }
}

function addTestMessage(text, className) {
  const el = document.createElement("div");
  el.className = `msg ${className}`;
  el.textContent = text;
  testChatBox.appendChild(el);
  testChatBox.scrollTop = testChatBox.scrollHeight;
  return el;
}

async function sendTestMessage(event) {
  event.preventDefault();
  const message = testChatInput.value.trim();
  if (!message) return;

  addTestMessage(message, "msg-user");
  testChatInput.value = "";
  testChatSend.disabled = true;
  const statusMsg = addTestMessage("Обращаюсь к модели…", "msg-status");

  try {
    const form = readFormSettings();
    const res = await fetch("/api/settings/test-chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        persona_prompt: form.persona_prompt,
        business_facts: form.business_facts,
        reasoning_effort: form.reasoning_effort,
        temperature: form.temperature,
        max_reply_tokens: form.max_reply_tokens,
        history: testHistory,
        message,
      }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);

    statusMsg.remove();
    addTestMessage(data.reply, "msg-bot");
    if (data.tool_calls && data.tool_calls.length > 0) {
      const summary = data.tool_calls.map((tc) => `${tc.name}(${JSON.stringify(tc.arguments)})`).join(", ");
      addTestMessage(`Вызвана функция: ${summary}`, "msg-status");
    }
    if (data.truncated) {
      addTestMessage("Внимание: ответ модели содержал сырой JSON/технический текст и был обрезан — это тот самый сбой, от которого защищает боевой конвейер.", "msg-error");
    }

    testHistory.push({ role: "user", content: message });
    testHistory.push({ role: "assistant", content: data.reply });
  } catch (err) {
    statusMsg.remove();
    addTestMessage(`Ошибка: ${err.message}`, "msg-error");
  } finally {
    testChatSend.disabled = false;
  }
}

function seedTestChatWithGreeting() {
  // Mirrors the real call: the greeting is sent straight to TTS and appended
  // to history as the assistant's first turn (see session_base.py's
  // _send_fixed_greeting) — reflecting that here means the test chat's
  // conversation context matches what a real caller's session would have,
  // e.g. correctly testing that the bot doesn't try to greet a second time.
  testHistory = [{ role: "assistant", content: greetingEl.value }];
  testChatBox.innerHTML = "";
  addTestMessage(greetingEl.value, "msg-bot");
}

function clearTestChat() {
  seedTestChatWithGreeting();
}

saveBtn.addEventListener("click", saveSettings);
resetBtn.addEventListener("click", resetSettings);
testChatForm.addEventListener("submit", sendTestMessage);
testChatClear.addEventListener("click", clearTestChat);

loadSettings();
