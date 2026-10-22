// potatoq dashboard: polls api/overview and api/dead, renders with textContent only
// (task arguments and failure reasons are user data, never HTML).
"use strict";

const REFRESH_MS = 3000;
const $ = (id) => document.getElementById(id);
let paused = false;
let lastUpdate = null;
let state = { read_only: true, now: Date.now() / 1000 };
let openTaskId = null;

function el(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props)) {
    if (key === "class") node.className = value;
    else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else node[key] = value;
  }
  for (const child of children) {
    if (child !== null && child !== undefined) node.append(child instanceof Node ? child : String(child));
  }
  return node;
}

function coarse(seconds) {
  seconds = Math.floor(Math.abs(seconds));
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  if (seconds < 86400) {
    const h = Math.floor(seconds / 3600), m = Math.floor((seconds % 3600) / 60);
    return m ? `${h}h${String(m).padStart(2, "0")}m` : `${h}h`;
  }
  const d = Math.floor(seconds / 86400), h = Math.floor((seconds % 86400) / 3600);
  return h ? `${d}d${h}h` : `${d}d`;
}
const ago = (ts) => (ts == null ? "—" : state.now - ts < 1 ? "just now" : `${coarse(state.now - ts)} ago`);
const until = (ts) => (ts == null ? "—" : ts - state.now < 1 ? "due now" : `in ${coarse(ts - state.now)}`);
const when = (ts) =>
  ts == null ? "—" : `${new Date(ts * 1000).toLocaleString()} (${ts > state.now + 1 ? until(ts) : ago(ts)})`;
const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;
const lastLine = (text) => (text || "").trim().split("\n").pop() || "";
const show = (value) => (typeof value === "string" ? value : JSON.stringify(value));

function fill(tbody, rows, columns, emptyText) {
  tbody.replaceChildren(
    ...(rows.length ? rows : [el("tr", {}, el("td", { class: "empty", colSpan: columns }, emptyText))]),
  );
}

function taskLink(id) {
  return el("button", { type: "button", class: "link mono", onclick: () => openTask(id) }, id);
}

async function api(path, options = {}) {
  const response = await fetch(`api/${path}`, { cache: "no-store", ...options });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.error || `${response.status} ${response.statusText}`);
  return body;
}

function action(path) {
  return api(path, { method: "POST", headers: { "X-Potatoq-Dashboard": "1" } });
}

function renderOverview(data) {
  state = data;
  $("app-name").textContent = data.app ? `· ${data.app}` : "";
  $("broker").textContent = data.results && data.results !== data.broker
    ? `${data.broker} · results: ${data.results}` : data.broker;
  $("read-only").hidden = !data.read_only;

  const running = data.workers.reduce((n, w) => n + w.running.length, 0);
  const waiting = Object.values(data.queues).reduce((n, c) => n + c, 0);
  $("t-workers").textContent = data.workers.length;
  $("t-running").textContent = running;
  $("t-waiting").textContent = waiting;
  $("t-dead").textContent = data.dead.count + (data.dead.more ? "+" : "");
  $("tile-dead").classList.toggle("alert", data.dead.count > 0);

  fill($("workers"), data.workers.map((w) => el("tr", {},
    el("td", { class: "nowrap" }, el("span", { class: `dot${w.stale ? " stale" : ""}`, title: w.stale ? "Heartbeat is late" : "Live" }), w.id),
    el("td", {}, w.queues.join(", ")),
    el("td", { class: "num" }, w.threads > 1 ? `${w.concurrency * w.threads} (${w.concurrency}×${w.threads})` : w.concurrency),
    el("td", {}, ...(w.running.length
      ? w.running.flatMap((id, i) => (i ? [el("br"), taskLink(id)] : [taskLink(id)]))
      : [el("span", { class: "muted" }, "idle")])),
    el("td", { class: "num" }, w.heartbeat_age < 1 ? "just now" : `${coarse(w.heartbeat_age)} ago`),
  )), 5, "No live workers. Start one with: potatoq worker");

  fill($("queues"), Object.entries(data.queues).map(([name, count]) => el("tr", {},
    el("td", {}, name), el("td", { class: "num" }, count),
  )), 2, "All queues are empty ✨");

  fill($("schedule"), data.schedule.map((e) => el("tr", {},
    el("td", {}, el("strong", {}, e.name), el("br"), el("span", { class: "muted small mono" }, e.task)),
    el("td", { class: "mono" }, e.schedule),
    el("td", { class: "num" }, until(e.next_run)),
    el("td", { class: "num" }, ago(e.last_sent)),
  )), 4, "No periodic tasks (beat_schedule is empty)");
}

function renderDead(entries) {
  $("dead-note").textContent = entries.length ? "newest first" : "";
  fill($("dead"), entries.map((e) => el("tr", {},
    el("td", {}, el("strong", {}, e.task)),
    el("td", {}, taskLink(e.id)),
    el("td", {}, e.queue),
    el("td", { class: "num" }, ago(e.died_at)),
    el("td", { class: "reason", title: e.reason }, lastLine(e.reason)),
    el("td", {}, state.read_only ? null : el("button", {
      type: "button", class: "small", onclick: (event) => retry(e.id, event.currentTarget),
    }, "Retry")),
  )), 6, "No dead-lettered tasks 🎉");
}

async function refresh() {
  if (paused) return;
  try {
    const overview = await api("overview");
    renderOverview(overview);
    renderDead(overview.dead.count ? await api("dead") : []);
    lastUpdate = Date.now();
    $("error").hidden = true;
  } catch (error) {
    $("error").textContent = `Couldn't refresh: ${error.message}`;
    $("error").hidden = false;
  }
  tick();
}

function tick() {
  if (paused) $("updated").textContent = "Paused";
  else if (lastUpdate) $("updated").textContent = `Updated ${coarse((Date.now() - lastUpdate) / 1000)} ago`;
}

async function retry(id, button) {
  button.disabled = true;
  try {
    await action(`dead/${encodeURIComponent(id)}/retry`);
  } catch (error) {
    alert(`Couldn't retry ${id}: ${error.message}`);
  }
  refresh();
}

function field(name, value, extra = {}) {
  if (value === null || value === undefined || value === "") return [];
  return [el("dt", {}, name), el("dd", extra, value)];
}

async function openTask(id) {
  openTaskId = id;
  const dialog = $("task");
  $("task-title").textContent = id;
  $("task-fields").replaceChildren(el("dd", { class: "muted" }, "Loading…"));
  $("task-traceback").hidden = true;
  $("revoke").hidden = true;
  if (!dialog.open) dialog.showModal();
  try {
    renderTask(await api(`tasks/${encodeURIComponent(id)}`));
  } catch (error) {
    $("task-fields").replaceChildren(el("dd", {}, `Couldn't load the task: ${error.message}`));
  }
}

function renderTask(t) {
  const waiting = t.waiting;
  const dead = t.dead;
  const stateLabel = t.state || (dead ? "DEAD-LETTERED" : waiting ? waiting.state.toUpperCase() : null);
  const at = (ts) => (ts ? when(ts) : null);
  $("task-fields").replaceChildren(
    ...field("State", stateLabel || "Unknown: no result stored and not in a queue",
      { class: stateLabel ? `state-${stateLabel}` : "muted" }),
    ...field("Task", t.task || waiting?.task || dead?.task),
    ...field("Queue", waiting?.queue || dead?.queue),
    ...field("Arguments", show(t.args ?? waiting?.args ?? dead?.args), { class: "mono" }),
    ...field("Keyword args", show(t.kwargs ?? waiting?.kwargs ?? dead?.kwargs), { class: "mono" }),
    ...field("Runs at", at(waiting?.eta)),
    ...field("Enqueued", at(t.enqueued_at ?? waiting?.enqueued_at)),
    ...field("Started", at(t.started_at)),
    ...field("Finished", at(t.done_at)),
    ...field("Dead-lettered", at(dead?.died_at)),
    ...field("Worker", t.worker),
    ...field("Retries", t.retries || dead?.retries || null),
    ...field("Result", t.state === "SUCCESS" ? show(t.result) : null, { class: "mono" }),
    ...field("Error", t.state === "FAILURE" ? t.error || show(t.result) : null, { class: "mono" }),
  );
  const details = t.traceback || dead?.reason || "";
  $("task-traceback").textContent = details;
  $("task-traceback").hidden = !details;
  $("retry").hidden = !dead || state.read_only;
  $("revoke").hidden = !(waiting && waiting.state !== "running" && !state.read_only);
}

$("retry").addEventListener("click", async (event) => {
  const id = openTaskId;
  const button = event.currentTarget;
  button.disabled = true;
  try {
    await action(`dead/${encodeURIComponent(id)}/retry`);
    await openTask(id);
  } catch (error) {
    alert(`Couldn't retry ${id}: ${error.message}`);
  }
  button.disabled = false;
  refresh();
});

$("revoke").addEventListener("click", async (event) => {
  const id = openTaskId;
  const button = event.currentTarget;
  if (!id || !confirm(`Revoke ${id}? It won't run.`)) return;
  button.disabled = true;
  try {
    await action(`tasks/${encodeURIComponent(id)}/revoke`);
    await openTask(id);
  } catch (error) {
    alert(`Couldn't revoke ${id}: ${error.message}`);
  }
  button.disabled = false;
  refresh();
});

$("lookup").addEventListener("submit", (event) => {
  event.preventDefault();
  const id = $("lookup-id").value.trim();
  if (id) openTask(id);
});

$("pause").addEventListener("click", () => {
  paused = !paused;
  $("pause").textContent = paused ? "Resume" : "Pause";
  if (!paused) refresh();
  tick();
});

refresh();
setInterval(refresh, REFRESH_MS);
setInterval(tick, 1000);
