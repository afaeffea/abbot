const callsListEl = document.getElementById("calls-list");
const callDetailEl = document.getElementById("call-detail");

function formatDuration(seconds) {
  if (seconds == null) return "—";
  const m = Math.floor(seconds / 60);
  const s = Math.round(seconds % 60);
  return `${m}:${String(s).padStart(2, "0")}`;
}

function formatOutcome(outcome) {
  if (!outcome || !outcome.booked) return "без брони";
  const parts = [outcome.service, outcome.date, outcome.time].filter(Boolean).join(" · ");
  const people = outcome.people_count != null ? `, ${outcome.people_count} чел.` : "";
  return `бронь: ${parts}${people}`;
}

function formatLatency(ms) {
  return ms == null ? "—" : `${ms} мс`;
}

function escapeHtml(str) {
  const div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}

async function loadCalls() {
  callsListEl.textContent = "Загрузка…";
  try {
    const res = await fetch("/api/calls");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const calls = await res.json();
    if (!calls.length) {
      callsListEl.textContent = "Пока нет ни одного звонка.";
      return;
    }

    const table = document.createElement("table");
    table.className = "calls-table";
    table.innerHTML =
      "<thead><tr><th>Время</th><th>Длительность</th><th>Итог</th><th>Ходов</th></tr></thead><tbody></tbody>";
    const tbody = table.querySelector("tbody");

    for (const call of calls) {
      const tr = document.createElement("tr");
      tr.className = "calls-row";
      tr.innerHTML = `
        <td>${call.started_at ? new Date(call.started_at).toLocaleString("ru-RU") : "—"}</td>
        <td>${formatDuration(call.duration_seconds)}</td>
        <td>${escapeHtml(formatOutcome(call.outcome))}</td>
        <td>${call.turn_count}</td>
      `;
      tr.addEventListener("click", () => loadCallDetail(call.call_id));
      tbody.appendChild(tr);
    }

    callsListEl.innerHTML = "";
    callsListEl.appendChild(table);
  } catch (err) {
    callsListEl.textContent = `Ошибка загрузки: ${err.message}`;
  }
}

async function loadCallDetail(callId) {
  callDetailEl.textContent = "Загрузка…";
  try {
    const res = await fetch(`/api/calls/${callId}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    const wrapper = document.createElement("div");
    wrapper.className = "call-detail-inner";

    const heading = document.createElement("h2");
    heading.textContent = `Звонок ${data.call_id}`;
    wrapper.appendChild(heading);

    const audio = document.createElement("audio");
    audio.controls = true;
    audio.src = `/api/calls/${callId}/audio`;
    wrapper.appendChild(audio);

    const outcomeEl = document.createElement("p");
    outcomeEl.className = "call-outcome";
    outcomeEl.textContent = `Итог: ${formatOutcome(data.outcome)}`;
    wrapper.appendChild(outcomeEl);

    const table = document.createElement("table");
    table.className = "turns-table";
    table.innerHTML = `
      <thead><tr>
        <th>#</th><th>Пользователь</th><th>Бот</th>
        <th>До первого токена</th><th>До первого звука</th><th>Функции</th><th>Статус</th>
      </tr></thead>
      <tbody></tbody>
    `;
    const tbody = table.querySelector("tbody");
    for (const turn of data.turns) {
      const toolCalls = (turn.tool_calls || []).map((tc) => tc.name).join(", ") || "—";
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${turn.index}</td>
        <td>${turn.user_text ? escapeHtml(turn.user_text) : "<i>(приветствие)</i>"}</td>
        <td>${escapeHtml(turn.bot_text || "")}</td>
        <td>${formatLatency(turn.llm_first_token_latency_ms)}</td>
        <td>${formatLatency(turn.tts_first_audio_latency_ms)}</td>
        <td>${escapeHtml(toolCalls)}</td>
        <td>${turn.cancelled ? "прервано" : "ок"}</td>
      `;
      tbody.appendChild(tr);
    }
    wrapper.appendChild(table);

    callDetailEl.innerHTML = "";
    callDetailEl.appendChild(wrapper);
  } catch (err) {
    callDetailEl.textContent = `Ошибка загрузки: ${err.message}`;
  }
}

loadCalls();
