const statusEl = document.getElementById("prompt-status");
const textareaEl = document.getElementById("prompt-textarea");
const saveBtn = document.getElementById("prompt-save");
const resetBtn = document.getElementById("prompt-reset");

function applyState(data) {
  textareaEl.value = data.prompt;
  statusEl.textContent = data.is_customized
    ? "Изменён (отличается от промпта по умолчанию). Изменения применяются к новым звонкам сразу после сохранения."
    : "Промпт по умолчанию (не изменён).";
  statusEl.className = "prompt-status" + (data.is_customized ? " prompt-status-custom" : "");
}

async function loadPrompt() {
  statusEl.textContent = "Загрузка…";
  try {
    const res = await fetch("/api/prompt");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    applyState(await res.json());
    textareaEl.disabled = false;
    saveBtn.disabled = false;
    resetBtn.disabled = false;
  } catch (err) {
    statusEl.textContent = `Ошибка загрузки: ${err.message}`;
    statusEl.className = "prompt-status prompt-status-error";
  }
}

async function savePrompt() {
  saveBtn.disabled = true;
  statusEl.textContent = "Сохраняю…";
  try {
    const res = await fetch("/api/prompt", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ prompt: textareaEl.value }),
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

async function resetPrompt() {
  if (!confirm("Восстановить промпт по умолчанию? Текущие изменения будут потеряны.")) return;
  resetBtn.disabled = true;
  statusEl.textContent = "Восстанавливаю…";
  try {
    const res = await fetch("/api/prompt/reset", { method: "POST" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    applyState(await res.json());
  } catch (err) {
    statusEl.textContent = `Ошибка восстановления: ${err.message}`;
    statusEl.className = "prompt-status prompt-status-error";
  } finally {
    resetBtn.disabled = false;
  }
}

saveBtn.addEventListener("click", savePrompt);
resetBtn.addEventListener("click", resetPrompt);

loadPrompt();
