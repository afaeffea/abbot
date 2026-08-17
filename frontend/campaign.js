const tokenEl = document.getElementById("campaign-token");
const numbersEl = document.getElementById("campaign-numbers");
const delayEl = document.getElementById("campaign-delay");
const startBtn = document.getElementById("campaign-start");
const statusEl = document.getElementById("campaign-status");
const progressEl = document.getElementById("campaign-progress");

const TOKEN_STORAGE_KEY = "warpoint_telephony_token";

let pollTimer = null;

function escapeHtml(str) {
  const div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}

const savedToken = localStorage.getItem(TOKEN_STORAGE_KEY);
if (savedToken) tokenEl.value = savedToken;

function statusLabel(status) {
  if (status === "started") return "запущен";
  if (status === "error") return "ошибка";
  return "в очереди";
}

function renderState(state) {
  if (!state.results || !state.results.length) {
    progressEl.textContent = "";
    return;
  }
  const done = state.results.filter((r) => r.status !== "pending").length;
  statusEl.textContent =
    state.status === "running"
      ? `Идёт обзвон: ${done} / ${state.total}…`
      : `Обзвон завершён: ${done} / ${state.total}.`;

  const table = document.createElement("table");
  table.className = "calls-table";
  table.innerHTML = "<thead><tr><th>Номер</th><th>Статус</th><th>Подробности</th></tr></thead><tbody></tbody>";
  const tbody = table.querySelector("tbody");
  for (const r of state.results) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(r.phone_number)}</td>
      <td>${statusLabel(r.status)}</td>
      <td>${r.detail ? escapeHtml(r.detail) : "—"}</td>
    `;
    tbody.appendChild(tr);
  }
  progressEl.innerHTML = "";
  progressEl.appendChild(table);
}

async function pollState(token) {
  try {
    const res = await fetch("/api/telephony/campaign", { headers: { "X-Telephony-Token": token } });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const state = await res.json();
    renderState(state);
    if (state.status !== "running" && pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
      startBtn.disabled = false;
    }
    return state;
  } catch (err) {
    statusEl.textContent = `Ошибка получения статуса: ${err.message}`;
    return null;
  }
}

startBtn.addEventListener("click", async () => {
  const token = tokenEl.value.trim();
  if (!token) {
    statusEl.textContent = "Введите токен.";
    return;
  }
  localStorage.setItem(TOKEN_STORAGE_KEY, token);

  const phoneNumbers = numbersEl.value
    .split("\n")
    .map((s) => s.trim())
    .filter(Boolean);
  if (!phoneNumbers.length) {
    statusEl.textContent = "Добавьте хотя бы один номер.";
    return;
  }
  const delaySeconds = Number(delayEl.value) || 5;

  startBtn.disabled = true;
  statusEl.textContent = "Запускаю обзвон…";
  try {
    const res = await fetch("/api/telephony/campaign", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Telephony-Token": token },
      body: JSON.stringify({ phone_numbers: phoneNumbers, delay_seconds: delaySeconds }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);

    statusEl.textContent = `Обзвон запущен: ${data.total} номер(ов).`;
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(() => pollState(token), 2000);
    pollState(token);
  } catch (err) {
    statusEl.textContent = `Ошибка: ${err.message}`;
    startBtn.disabled = false;
  }
});

// If a campaign from an earlier page load is still running, pick up its
// progress instead of showing a blank page.
if (savedToken) {
  pollState(savedToken).then((state) => {
    if (state && state.status === "running") {
      startBtn.disabled = true;
      pollTimer = setInterval(() => pollState(savedToken), 2000);
    }
  });
}
