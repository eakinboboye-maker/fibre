const BASE = "";
let token = localStorage.getItem("token") || "";
let me = null;

let workers = [];        // active workers for daily operations
let workersAll = [];     // admin-only list including inactive
let taskTypes = [];
let workstations = [];
let factories = [];
let teams = [];
let appUsers = [];

let currentWorkDayId = null;
let currentWorkerId = null;
let currentDayClosed = false;

let approvalsSelection = new Set();

function $(id) { return document.getElementById(id); }
function show(el, yes) { if (el) el.classList.toggle("hidden", !yes); }
function todayISO() { return new Date().toISOString().slice(0, 10); }

function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;");
}

function moneyNGN(n) {
  try { return new Intl.NumberFormat("en-NG",{style:"currency",currency:"NGN"}).format(Number(n)); }
  catch { return `₦${Number(n).toFixed(2)}`; }
}

function qs(params = {}) {
  const clean = Object.fromEntries(Object.entries(params).filter(([_, v]) => v !== null && v !== undefined && v !== ""));
  const s = new URLSearchParams(clean).toString();
  return s ? `?${s}` : "";
}

function uuid() {
  return (crypto && crypto.randomUUID) ? crypto.randomUUID() : String(Math.random()).slice(2) + Date.now();
}

// ---------------- IndexedDB queue ----------------
const Q_DB = "fibre_ops_queue_db";
const Q_STORE = "queue";

function idbOpen() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(Q_DB, 1);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(Q_STORE)) {
        const store = db.createObjectStore(Q_STORE, { keyPath: "id" });
        store.createIndex("ts", "ts");
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function qAdd(item) {
  const db = await idbOpen();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(Q_STORE, "readwrite");
    tx.objectStore(Q_STORE).put(item);
    tx.oncomplete = () => resolve(true);
    tx.onerror = () => reject(tx.error);
  });
}

async function qAll() {
  const db = await idbOpen();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(Q_STORE, "readonly");
    const req = tx.objectStore(Q_STORE).getAll();
    req.onsuccess = () => resolve(req.result || []);
    req.onerror = () => reject(req.error);
  });
}

async function qDel(id) {
  const db = await idbOpen();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(Q_STORE, "readwrite");
    tx.objectStore(Q_STORE).delete(id);
    tx.oncomplete = () => resolve(true);
    tx.onerror = () => reject(tx.error);
  });
}

async function updateSyncBadge() {
  const items = await qAll();
  const btn = $("syncBtn");
  if (!btn) return;
  btn.textContent = `Sync (${items.length})`;
  show(btn, !!token);
}

function setNetBadge() {
  const online = navigator.onLine;
  const b = $("netBadge");
  if (!b) return;
  b.textContent = online ? "Online" : "Offline";
  b.classList.toggle("badge-ok", online);
  b.classList.toggle("badge-warn", !online);
}

// ---------------- request wrapper ----------------
async function req(path, { method="GET", body, offlineQueue=true } = {}) {
  const headers = { "Content-Type": "application/json" };
  if (token) headers["Authorization"] = `Bearer ${token}`;

  const isWrite = ["POST","PATCH","PUT","DELETE"].includes(method.toUpperCase());

  try {
    const res = await fetch(BASE + path, {
      method,
      headers,
      body: body ? JSON.stringify(body) : undefined
    });

    const txt = await res.text();
    let data = null;
    try { data = txt ? JSON.parse(txt) : null; } catch { data = txt; }

    if (!res.ok) throw new Error(data?.detail || `Request failed (${res.status})`);
    return data;

  } catch (err) {
    if (isWrite && offlineQueue) {
      const item = { id: uuid(), ts: Date.now(), method, path, body: body || null };
      await qAdd(item);
      await updateSyncBadge();
      return { queued: true, queued_id: item.id };
    }
    throw err;
  }
}

// ---------------- API ----------------
const API = {
  login: (body) => req("/api/auth/login", { method:"POST", body, offlineQueue:false }),
  me: () => req("/api/me", { offlineQueue:false }),

  listWorkers: () => req("/api/workers", { offlineQueue:false }),
  listAdminWorkers: () => req("/api/admin/workers", { offlineQueue:false }),
  createWorker: (body) => req("/api/workers", { method:"POST", body }),
  updateWorker: (id, body) => req(`/api/workers/${id}`, { method:"PATCH", body }),

  listTaskTypes: () => req("/api/task-types", { offlineQueue:false }),
  listWorkstations: (factory_id=null) => req(`/api/workstations${qs({ factory_id })}`, { offlineQueue:false }),
  createWorkstation: (body) => req("/api/workstations", { method:"POST", body }),

  listFactories: () => req("/api/factories", { offlineQueue:false }),
  createFactory: (body) => req("/api/factories", { method:"POST", body }),

  listTeams: (factory_id=null) => req(`/api/teams${qs({ factory_id })}`, { offlineQueue:false }),
  createTeam: (body) => req("/api/teams", { method:"POST", body }),

  // app users (admin)
  createAppUser: (body) => req("/api/admin/app-users", { method:"POST", body }),
  listAppUsers: () => req("/api/admin/app-users", { offlineQueue:false }),
  updateAppUser: (id, body) => req(`/api/admin/app-users/${id}`, { method:"PATCH", body }),

  createOrLoadWorkDay: (body) => req("/api/work-days", { method:"POST", body }),
  getWorkerDays: (workerId, params={}) => req(`/api/work-days/${workerId}${qs(params)}`, { offlineQueue:false }),

  addWorkTask: (body) => req("/api/work-tasks", { method:"POST", body }),
  updateTask: (taskId, body) => req(`/api/work-tasks/${taskId}`, { method:"PATCH", body }),
  deleteTask: (taskId) => req(`/api/work-tasks/${taskId}`, { method:"DELETE" }),
  decideTask: (taskId, body) => req(`/api/work-tasks/${taskId}/decide`, { method:"POST", body }),

  pendingApprovals: (params={}) => req(`/api/approvals/pending${qs(params)}`, { offlineQueue:false }),
  bulkDecide: (body) => req("/api/work-tasks/bulk-decide", { method:"POST", body }),

  closeDay: (work_day_id) => req(`/api/work-days/${work_day_id}/close`, { method:"POST", body:{} }),
  reopenDay: (work_day_id) => req(`/api/work-days/${work_day_id}/reopen`, { method:"POST", body:{} }),

  payroll: (workerId, as_of) => req(`/api/payroll/${workerId}${qs({ as_of })}`, { offlineQueue:false }),

  createRun: (as_of, note) => req("/api/payroll-runs", { method:"POST", body:{ as_of, note } }),
  listRuns: () => req("/api/payroll-runs", { offlineQueue:false }),

  reportTaskTotals: (start, end) => req(`/api/reports/task-totals${qs({ start, end })}`, { offlineQueue:false }),
  reportByWorkstation: (start, end) => req(`/api/reports/by-workstation${qs({ start, end })}`, { offlineQueue:false }),
  reportBySupervisor: (start, end) => req(`/api/reports/by-supervisor${qs({ start, end })}`, { offlineQueue:false }),

  audit: (entity_type, limit) => req(`/api/audit${qs({ entity_type, limit })}`, { offlineQueue:false }),
  
  payrollDue: (as_of) => req(`/api/payroll/due${qs({ as_of })}`, { offlineQueue:false }),

};

// ---------------- UI helpers ----------------
function setWhoami() { const el = $("whoami"); if (el) el.textContent = me ? `${me.email} (${me.role})` : ""; }

function setActiveTab(tabName) {
  document.querySelectorAll(".tab").forEach(b => b.classList.toggle("active", b.dataset.tab === tabName));
  document.querySelectorAll(".tabPanel").forEach(p => show(p, p.id === `tab-${tabName}`));
}

function renderOptions(selectEl, list, labelFn, placeholder="Select...", includeNone=false) {
  if (!selectEl) return;
  selectEl.innerHTML = "";

  const opt0 = document.createElement("option");
  opt0.value = "";
  opt0.textContent = placeholder;
  selectEl.appendChild(opt0);

  if (includeNone) {
    const optN = document.createElement("option");
    optN.value = "__NONE__";
    optN.textContent = "— None —";
    selectEl.appendChild(optN);
  }

  for (const item of list) {
    const opt = document.createElement("option");
    opt.value = item.id;
    opt.textContent = labelFn(item);
    selectEl.appendChild(opt);
  }
}

function pill(status) {
  const cls = status || "pending";
  return `<span class="pill ${cls}">${escapeHtml(cls.toUpperCase())}</span>`;
}

function setDayStatusUI() {
  const badge = $("dayStatus");
  if (!badge) return;

  if (!currentWorkDayId) {
    badge.textContent = "No day loaded";
    badge.className = "badge";
    show($("closeDayBtn"), false);
    show($("reopenDayBtn"), false);
    return;
  }
  badge.textContent = currentDayClosed ? "Closed" : "Open";
  badge.className = "badge " + (currentDayClosed ? "badge-warn" : "badge-ok");

  show($("closeDayBtn"), !currentDayClosed);
  show($("reopenDayBtn"), currentDayClosed && me?.role === "admin");
}

// ---------------- Daily ----------------
function renderRubricBox(dayObj) {
  const rL = dayObj.rubric_logged;
  const rA = dayObj.rubric_approved;
  $("rubricBox").innerHTML = `
    <div><b>Daily Target Guidance</b> (1kg combing ≡ 60m weaving/twisting)</div>
    <div class="muted">Logged totals:</div>
    <ul>
      <li>Target met: <b>${rL.target_met}</b></li>
      <li>If combing low → weaving needed: <b>${Number(rL.weaving_needed_m).toFixed(2)}</b> m</li>
      <li>If weaving low → combing needed: <b>${Number(rL.combing_needed_kg).toFixed(4)}</b> kg</li>
    </ul>
    <div class="muted">Approved totals:</div>
    <ul>
      <li>Target met: <b>${rA.target_met}</b></li>
      <li>If combing low → weaving needed: <b>${Number(rA.weaving_needed_m).toFixed(2)}</b> m</li>
      <li>If weaving low → combing needed: <b>${Number(rA.combing_needed_kg).toFixed(4)}</b> kg</li>
    </ul>
  `;
  show($("rubricBox"), true);
}

function renderTasksTable(dayObj) {
  const tasks = dayObj.tasks || [];
  if (!tasks.length) {
    $("tasksWrap").innerHTML = `<p class="muted">No tasks yet for this day.</p>`;
    return;
  }

  $("tasksWrap").innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Task</th><th>Qty</th><th>Status</th><th>Approved Pay</th><th>Notes</th><th>Actions</th>
        </tr>
      </thead>
      <tbody>
        ${tasks.map(t => `
          <tr>
            <td><b>${escapeHtml(t.name)}</b><div class="small">${escapeHtml(t.code)} • ${escapeHtml(t.unit)}</div></td>
            <td>${Number(t.quantity).toFixed(3)} ${escapeHtml(t.unit)}</td>
            <td>${pill(t.status)}</td>
            <td>${moneyNGN(t.approved_pay_ngn || 0)}</td>
            <td class="small">${escapeHtml(t.note || "")}</td>
            <td>
              <div class="inlineBtns">
                <button class="ok" data-approve="${t.id}" ${t.status==="approved" || currentDayClosed ? "disabled":""}>Approve</button>
                <button class="danger" data-reject="${t.id}" ${t.status==="rejected" || currentDayClosed ? "disabled":""}>Reject</button>
                <button class="secondary" data-edit="${t.id}" ${t.status!=="pending" || currentDayClosed ? "disabled":""}>Edit</button>
                <button class="secondary" data-del="${t.id}" ${t.status!=="pending" || currentDayClosed ? "disabled":""}>Delete</button>
              </div>
            </td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;

  document.querySelectorAll("button[data-approve]").forEach(btn => btn.onclick = () => decide(btn.dataset.approve, "approved"));
  document.querySelectorAll("button[data-reject]").forEach(btn => btn.onclick = () => decide(btn.dataset.reject, "rejected"));
  document.querySelectorAll("button[data-edit]").forEach(btn => btn.onclick = () => editTask(btn.dataset.edit, tasks));
  document.querySelectorAll("button[data-del]").forEach(btn => btn.onclick = () => deleteTask(btn.dataset.del));
}

async function loadSelectedDay() {
  if (!currentWorkerId) return alert("Select a worker first");
  const d = $("workDate").value;
  if (!d) return alert("Select a date");

  const wsId = $("workstationSelect").value || null;

  const out = await API.createOrLoadWorkDay({
    worker_id: currentWorkerId,
    work_date: d,
    workstation_id: wsId,
    day_note: $("dayNote").value || null
  });

  currentWorkDayId = out.work_day_id;
  $("dayMeta").innerHTML = `<div><b>Work Day loaded</b></div><div class="muted">ID: <code>${escapeHtml(currentWorkDayId)}</code></div>`;
  show($("dayMeta"), true);

  await refreshDayDetails();
}

async function refreshDayDetails() {
  if (!currentWorkerId) return;
  const d = $("workDate").value;
  const days = await API.getWorkerDays(currentWorkerId, { start: d, end: d });
  if (!days.length) {
    $("tasksWrap").innerHTML = `<p class="muted">No work day found. Click "Create / Load Day".</p>`;
    show($("rubricBox"), false);
    currentWorkDayId = null;
    currentDayClosed = false;
    setDayStatusUI();
    return;
  }
  const dayObj = days[0];
  currentWorkDayId = dayObj.work_day_id;
  currentDayClosed = !!dayObj.is_closed;
  $("dayNote").value = dayObj.day_note || "";
  setDayStatusUI();

  renderRubricBox(dayObj);
  renderTasksTable(dayObj);
}

async function addTask(e) {
  e.preventDefault();
  if (!currentWorkDayId) return alert("Load the work day first.");
  if (currentDayClosed) return alert("Day is closed.");

  const qty = Number($("taskQty").value || 0);
  if (qty <= 0) return alert("Quantity must be > 0");

  const result = await API.addWorkTask({
    id: uuid(),
    work_day_id: currentWorkDayId,
    task_type_id: $("taskTypeSelect").value,
    quantity: qty,
    note: $("taskNote").value || null
  });

  $("taskQty").value = "0";
  $("taskNote").value = "";

  if (result?.queued) alert("Saved offline (queued). It will sync when online.");
  else { await refreshDayDetails(); await refreshApprovals(); }
}

async function decide(taskId, status) {
  if (currentDayClosed) return alert("Day is closed.");
  const reason = prompt(`${status === "approved" ? "Approve" : "Reject"} task?\nOptional reason:`) || null;
  const result = await API.decideTask(taskId, { status, decision_reason: reason });
  if (result?.queued) alert("Decision queued offline.");
  else { await refreshDayDetails(); await refreshApprovals(); }
}

async function editTask(taskId, tasks) {
  if (currentDayClosed) return alert("Day is closed.");
  const t = tasks.find(x => x.id === taskId);
  if (!t) return;

  const newQtyStr = prompt(`Edit quantity (${t.unit})`, String(t.quantity));
  if (newQtyStr === null) return;
  const newQty = Number(newQtyStr);
  if (!(newQty >= 0)) return alert("Quantity must be >= 0");

  const newNote = prompt("Edit note (optional)", t.note || "");
  const result = await API.updateTask(taskId, { quantity: newQty, note: newNote ?? null });
  if (result?.queued) alert("Edit queued offline.");
  else await refreshDayDetails();
}

async function deleteTask(taskId) {
  if (currentDayClosed) return alert("Day is closed.");
  const ok = confirm("Delete this pending task? This cannot be undone.");
  if (!ok) return;
  const result = await API.deleteTask(taskId);
  if (result?.queued) alert("Delete queued offline.");
  else { await refreshDayDetails(); await refreshApprovals(); }
}

async function saveFastLog() {
  if (!currentWorkDayId) return alert("Load the work day first.");
  if (currentDayClosed) return alert("Day is closed.");

  const combing = Number($("fastCombing").value || 0);
  const weaving = Number($("fastWeaving").value || 0);
  const note = $("fastNote").value || null;

  const combType = taskTypes.find(t => t.code === "COMBING");
  const weavType = taskTypes.find(t => t.code === "WEAVING");
  if (!combType || !weavType) return alert("Missing COMBING/WEAVING task types in DB.");

  const queuedAny = [];
  if (combing > 0) queuedAny.push(!!(await API.addWorkTask({ id: uuid(), work_day_id: currentWorkDayId, task_type_id: combType.id, quantity: combing, note }))?.queued);
  if (weaving > 0) queuedAny.push(!!(await API.addWorkTask({ id: uuid(), work_day_id: currentWorkDayId, task_type_id: weavType.id, quantity: weaving, note }))?.queued);

  $("fastCombing").value = "0";
  $("fastWeaving").value = "0";
  $("fastNote").value = "";

  if (queuedAny.some(Boolean)) alert("Saved offline (queued).");
  else { await refreshDayDetails(); await refreshApprovals(); }
}

async function closeDay() {
  if (!currentWorkDayId) return alert("Load a day first.");
  const r = await API.closeDay(currentWorkDayId);
  if (r?.queued) alert("Close day queued offline.");
  await refreshDayDetails();
}

async function reopenDay() {
  if (!currentWorkDayId) return alert("Load a day first.");
  const r = await API.reopenDay(currentWorkDayId);
  if (r?.queued) alert("Reopen queued offline.");
  await refreshDayDetails();
}

// ---------------- Approvals ----------------
function renderApprovals(rows) {
  if (!rows.length) {
    $("approvalsWrap").innerHTML = `<p class="muted">No pending tasks found.</p>`;
    return;
  }

  $("approvalsWrap").innerHTML = `
    <table>
      <thead>
        <tr>
          <th><input type="checkbox" id="selAll"/></th>
          <th>Date</th><th>Worker</th><th>Task</th><th>Qty</th><th>Note</th><th>Actions</th>
        </tr>
      </thead>
      <tbody>
        ${rows.map(r => `
          <tr>
            <td><input type="checkbox" class="selOne" data-id="${r.task_id}" ${approvalsSelection.has(r.task_id) ? "checked":""}/></td>
            <td>${escapeHtml(r.work_date)}</td>
            <td><b>${escapeHtml(r.worker_name)}</b></td>
            <td>${escapeHtml(r.task_name)} <span class="small">(${escapeHtml(r.task_code)})</span></td>
            <td>${Number(r.quantity).toFixed(3)} ${escapeHtml(r.unit)}</td>
            <td class="small">${escapeHtml(r.note || "")}</td>
            <td>
              <div class="inlineBtns">
                <button class="ok" data-ap="${r.task_id}">Approve</button>
                <button class="danger" data-rj="${r.task_id}">Reject</button>
              </div>
            </td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;

  $("selAll").onchange = (e) => {
    const checked = e.target.checked;
    document.querySelectorAll(".selOne").forEach(cb => {
      cb.checked = checked;
      const id = cb.dataset.id;
      if (checked) approvalsSelection.add(id); else approvalsSelection.delete(id);
    });
  };

  document.querySelectorAll(".selOne").forEach(cb => {
    cb.onchange = () => {
      const id = cb.dataset.id;
      if (cb.checked) approvalsSelection.add(id); else approvalsSelection.delete(id);
    };
  });

  document.querySelectorAll("button[data-ap]").forEach(b => b.onclick = async () => {
    const r = await API.decideTask(b.dataset.ap, { status:"approved", decision_reason: null });
    if (r?.queued) alert("Queued offline.");
    await refreshApprovals();
  });

  document.querySelectorAll("button[data-rj]").forEach(b => b.onclick = async () => {
    const reason = prompt("Reject reason (optional):") || null;
    const r = await API.decideTask(b.dataset.rj, { status:"rejected", decision_reason: reason });
    if (r?.queued) alert("Queued offline.");
    await refreshApprovals();
  });
}

async function refreshApprovals() {
  const worker_id = $("approvalsWorkerSelect").value || null;
  const start = $("approvalsStart").value || null;
  const end = $("approvalsEnd").value || null;

  const rows = await API.pendingApprovals({ worker_id, start, end });
  const ids = new Set(rows.map(r => r.task_id));
  approvalsSelection = new Set([...approvalsSelection].filter(id => ids.has(id)));
  renderApprovals(rows);
}

async function bulkDecision(status) {
  const ids = [...approvalsSelection];
  if (!ids.length) return alert("Select at least one task.");
  const reason = prompt(`${status === "approved" ? "Approve" : "Reject"} selected tasks.\nOptional reason:`) || null;
  const out = await API.bulkDecide({ task_ids: ids, status, decision_reason: reason });
  if (out?.queued) alert("Bulk decision queued offline.");
  approvalsSelection.clear();
  await refreshApprovals();
}

// ---------------- Payroll + Runs ----------------
async function refreshPayroll() {
  const workerId = $("payrollWorkerSelect").value;
  if (!workerId) return alert("Select a worker");
  const asOf = $("payrollAsOf").value || null;
  const out = await API.payroll(workerId, asOf);

  $("payrollSummary").innerHTML = `
    <div><b>Worker:</b> ${escapeHtml(out.full_name)}</div>
    <div><b>Frequency:</b> ${escapeHtml(out.payout)}</div>
    <div><b>Period:</b> ${escapeHtml(out.period_start)} → ${escapeHtml(out.period_end)}</div>
    <hr/>
    <div><b>Approved combed:</b> ${Number(out.approved_combed_kg).toFixed(3)} kg</div>
    <div><b>Approved woven:</b> ${Number(out.approved_woven_m).toFixed(3)} m</div>
    <div><b>Total approved pay:</b> ${moneyNGN(out.approved_total_pay_ngn)}</div>
  `;

  $("downloadPayrollCsvBtn").onclick = () => {
    window.location.href = `/api/payroll/${workerId}/export.csv${qs({ as_of: asOf })}`;
  };
}

async function refreshRuns() {
  const runs = await API.listRuns();
  if (!runs.length) {
    $("runsWrap").innerHTML = `<p class="muted">No payroll runs yet.</p>`;
    return;
  }
  $("runsWrap").innerHTML = `
    <table>
      <thead><tr><th>Created</th><th>As Of</th><th>Note</th><th>Export</th></tr></thead>
      <tbody>
        ${runs.map(r => `
          <tr>
            <td>${escapeHtml(r.created_at)}</td>
            <td><b>${escapeHtml(r.as_of)}</b></td>
            <td class="small">${escapeHtml(r.note || "")}</td>
            <td><button class="secondary" data-exp="${r.id}">CSV</button></td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;
  document.querySelectorAll("button[data-exp]").forEach(b => {
    b.onclick = () => window.location.href = `/api/payroll-runs/${b.dataset.exp}/export.csv`;
  });
}

async function createRun() {
  const as_of = $("runAsOf").value;
  if (!as_of) return alert("Choose an as-of date");
  const note = $("runNote").value || null;
  const r = await API.createRun(as_of, note);
  if (r?.queued) alert("Run creation queued offline (will compute when back online).");
  else alert("Payroll run created.");
  await refreshRuns();
}

function renderDue(rows) {
  if (!rows.length) {
    $("dueWrap").innerHTML = `<p class="muted">No due payments.</p>`;
    return;
  }

  $("dueWrap").innerHTML = `
    <div class="callout badge-warn">
      <b>${rows.length}</b> worker(s) have due unpaid approved work.
    </div>
    <table>
      <thead>
        <tr>
          <th>Worker</th><th>Frequency</th><th>Period</th><th>Combed</th><th>Woven</th><th>Pay</th>
        </tr>
      </thead>
      <tbody>
        ${rows.map(r => `
          <tr>
            <td><b>${escapeHtml(r.full_name)}</b></td>
            <td>${escapeHtml(r.payout)}</td>
            <td>${escapeHtml(r.period_start)} → ${escapeHtml(r.period_end)}</td>
            <td>${Number(r.approved_combed_kg).toFixed(3)} kg</td>
            <td>${Number(r.approved_woven_m).toFixed(3)} m</td>
            <td><b>${moneyNGN(r.approved_total_pay_ngn)}</b></td>
          </tr>
        `).join("")}
      </tbody>
    </table>
    <p class="muted">Tip: Create a Payroll Run with an “As of” date on/after the period end to pay these.</p>
  `;
}

async function refreshDue() {
  const asOf = $("runAsOf").value || $("payrollAsOf").value || null;
  const rows = await API.payrollDue(asOf);
  renderDue(rows);

  // Optional: a simple “notify” in-app
  if (rows.length) {
    // show an alert once per load, not too annoying
    console.log("Due payments detected:", rows.length);
  }
}


// ---------------- Reports ----------------
function renderTable(rows, columns) {
  if (!rows.length) return `<p class="muted">No data.</p>`;
  return `
    <table>
      <thead><tr>${columns.map(c => `<th>${escapeHtml(c.label)}</th>`).join("")}</tr></thead>
      <tbody>
        ${rows.map(r => `<tr>${columns.map(c => `<td>${escapeHtml(c.fmt ? c.fmt(r[c.key]) : r[c.key])}</td>`).join("")}</tr>`).join("")}
      </tbody>
    </table>
  `;
}

async function refreshReports() {
  const start = $("reportStart").value;
  const end = $("reportEnd").value;
  if (!start || !end) return alert("Set report start/end");

  const a = await API.reportTaskTotals(start, end);
  const b = await API.reportByWorkstation(start, end);
  const c = await API.reportBySupervisor(start, end);

  $("reportTaskTotals").innerHTML = renderTable(a, [
    { key:"task_name", label:"Task" },
    { key:"task_code", label:"Code" },
    { key:"unit", label:"Unit" },
    { key:"total_quantity", label:"Total Qty", fmt: (x)=>Number(x).toFixed(3) },
    { key:"total_pay_ngn", label:"Total Pay", fmt: (x)=>moneyNGN(x) },
  ]);

  $("reportWorkstations").innerHTML = renderTable(b, [
    { key:"workstation", label:"Workstation" },
    { key:"total_pay_ngn", label:"Total Pay", fmt: (x)=>moneyNGN(x) },
  ]);

  $("reportSupervisors").innerHTML = renderTable(c, [
    { key:"supervisor_email", label:"Supervisor" },
    { key:"days_logged", label:"Days Logged" },
    { key:"tasks_approved", label:"Tasks Approved" },
    { key:"approved_pay_ngn", label:"Approved Pay", fmt: (x)=>moneyNGN(x) },
  ]);
}

// ---------------- Audit (admin) ----------------
async function refreshAudit() {
  const entity_type = $("auditEntityType").value || null;
  const limit = Number($("auditLimit").value || 100);
  const rows = await API.audit(entity_type, limit);

  $("auditWrap").innerHTML = rows.length ? `
    <table>
      <thead><tr><th>Time</th><th>Role</th><th>Action</th><th>Entity</th><th>Metadata</th></tr></thead>
      <tbody>
        ${rows.map(r => `
          <tr>
            <td>${escapeHtml(r.created_at)}</td>
            <td>${escapeHtml(r.actor_role || "")}</td>
            <td><b>${escapeHtml(r.action)}</b></td>
            <td>${escapeHtml(r.entity_type)}<div class="small">${escapeHtml(r.entity_id || "")}</div></td>
            <td class="small"><pre style="margin:0; white-space:pre-wrap;">${escapeHtml(JSON.stringify(r.metadata || {}, null, 2))}</pre></td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  ` : `<p class="muted">No audit records.</p>`;
}

// ---------------- Sync engine ----------------
async function syncQueue() {
  if (!navigator.onLine) return alert("You are offline.");
  const items = await qAll();
  if (!items.length) return alert("Nothing to sync.");

  items.sort((a,b) => a.ts - b.ts);

  let ok = 0;
  for (const it of items) {
    try {
      await req(it.path, { method: it.method, body: it.body, offlineQueue:false });
      await qDel(it.id);
      ok++;
      await updateSyncBadge();
    } catch (e) {
      alert(`Sync stopped: ${e.message}`);
      break;
    }
  }
  if (ok) alert(`Synced ${ok} item(s).`);
}

// ---------------- Admin UI helpers ----------------
function renderAdminLists() {
  const f = $("factoryList");
  const t = $("teamList");
  const w = $("workstationList");

  if (f) f.innerHTML = `
    <h3>Factories</h3>
    ${factories.length ? `<ul>${factories.map(x => `<li><b>${escapeHtml(x.name)}</b> <span class="small">${escapeHtml(x.id)}</span></li>`).join("")}</ul>` : `<p class="muted">No factories.</p>`}
  `;

  if (t) t.innerHTML = `
    <h3>Teams</h3>
    ${teams.length ? `<ul>${teams.map(x => `<li><b>${escapeHtml(x.name)}</b> <span class="small">factory: ${escapeHtml(x.factory_id)}</span></li>`).join("")}</ul>` : `<p class="muted">No teams.</p>`}
  `;

  if (w) w.innerHTML = `
    <h3>Workstations</h3>
    ${workstations.length ? `<ul>${workstations.map(x => `<li><b>${escapeHtml(x.name)}</b> <span class="small">factory: ${escapeHtml(x.factory_id)}</span></li>`).join("")}</ul>` : `<p class="muted">No workstations.</p>`}
  `;
}

async function refreshAdminData() {
  factories = await API.listFactories();
  teams = await API.listTeams(null);
  workstations = await API.listWorkstations(null);

  // Daily operations should only show active workers
  workers = await API.listWorkers();

  // Admin needs ALL workers including inactive
  workersAll = await API.listAdminWorkers();


  // app users only for admin
  if (me?.role === "admin") {
    appUsers = await API.listAppUsers();
  } else {
    appUsers = [];
  }

  // factories dropdowns
  const factoryLabel = x => x.name;
  renderOptions($("teamFactorySelect"), factories, factoryLabel, "Select factory...");
  renderOptions($("wsFactorySelect"), factories, factoryLabel, "Select factory...");
  renderOptions($("newUserFactorySelect"), factories, factoryLabel, "No factory scope");
  renderOptions($("appUserFactorySelect"), factories, factoryLabel, "No factory scope", true);
  renderOptions($("newWorkerFactorySelect"), factories, factoryLabel, "No factory");
  renderOptions($("workerFactorySelect"), factories, factoryLabel, "No factory", true);

  // teams dropdowns (unfiltered initially)
  renderOptions($("newWorkerTeamSelect"), teams, x => x.name, "No team");
  renderOptions($("workerTeamSelect"), teams, x => x.name, "No team", true);

  // app users select
  renderOptions($("appUserSelect"), appUsers, u => `${u.email} (${u.role})`, "Select user...");

  // workers for admin assignment
  renderOptions(
  $("adminWorkerSelect"),
  workersAll,
  w => `${w.full_name}${w.is_active ? "" : " (inactive)"}`,
  "Select worker..."
  );


  // daily selects should update too (workers list changes by scope)
  renderOptions($("workerSelect"), workers, w => `${w.full_name}${w.worker_code ? ` (${w.worker_code})` : ""}`, "Select worker...");
  renderOptions($("approvalsWorkerSelect"), workers, w => w.full_name, "All workers");
  renderOptions($("payrollWorkerSelect"), workers, w => w.full_name, "Select worker...");

  // workstations dropdown (all, or could filter by factory later)
  renderOptions($("workstationSelect"), workstations, ws => ws.name, "No workstation");

  renderAdminLists();
}

function filterTeamsByFactory(factoryId) {
  if (!factoryId || factoryId === "__NONE__") return teams;
  return teams.filter(t => String(t.factory_id) === String(factoryId));
}

// ---------------- Bootstrapping non-admin data ----------------
async function loadBootstrapData() {
  workers = await API.listWorkers();
  taskTypes = await API.listTaskTypes();
  workstations = await API.listWorkstations(null);

  renderOptions($("workerSelect"), workers, w => `${w.full_name}${w.worker_code ? ` (${w.worker_code})` : ""}`, "Select worker...");
  renderOptions($("approvalsWorkerSelect"), workers, w => w.full_name, "All workers");
  renderOptions($("payrollWorkerSelect"), workers, w => w.full_name, "Select worker...");
  renderOptions($("taskTypeSelect"), taskTypes, t => `${t.name} [${t.code}] — unit: ${t.unit}`, "Task type...");
  renderOptions($("workstationSelect"), workstations, w => w.name, "No workstation");

  if (workers.length) {
    currentWorkerId = workers[0].id;
    $("workerSelect").value = workers[0].id;
    $("payrollWorkerSelect").value = workers[0].id;
  }
}

async function loadMeAndShowApp() {
  me = await API.me();
  setWhoami();

  show($("loginView"), false);
  show($("appView"), true);
  show($("logoutBtn"), true);

  document.querySelectorAll(".adminOnly").forEach(el => show(el, me.role === "admin"));

  $("workDate").value = todayISO();
  $("payrollAsOf").value = todayISO();
  $("runAsOf").value = todayISO();

  const end = new Date();
  const start = new Date(Date.now() - 6*24*3600*1000);
  $("reportStart").value = start.toISOString().slice(0,10);
  $("reportEnd").value = end.toISOString().slice(0,10);
}

// ---------------- Main boot ----------------
async function boot() {
  // Tabs
  document.querySelectorAll(".tab").forEach(btn => btn.onclick = () => setActiveTab(btn.dataset.tab));

  // Net
  setNetBadge();
  window.addEventListener("online", async () => { setNetBadge(); await syncQueue().catch(()=>{}); });
  window.addEventListener("offline", () => setNetBadge());

  // Sync
  $("syncBtn").onclick = () => syncQueue();
  await updateSyncBadge();

  // Logout
  $("logoutBtn").onclick = () => { token=""; localStorage.removeItem("token"); location.reload(); };

  // Login
  $("loginForm").onsubmit = async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const out = await API.login({ email: fd.get("email"), password: fd.get("password") });
    token = out.access_token;
    localStorage.setItem("token", token);

    await loadMeAndShowApp();
    await loadBootstrapData();
    await updateSyncBadge();
    setActiveTab("daily");
    await refreshApprovals();
    await refreshRuns();

    // admin-only preload
    if (me.role === "admin") await refreshAdminData();
  };

  // Daily selection
  $("workerSelect").onchange = () => {
    currentWorkerId = $("workerSelect").value || null;
    currentWorkDayId = null;
    currentDayClosed = false;
    setDayStatusUI();
    show($("dayMeta"), false);
    $("tasksWrap").innerHTML = "";
    show($("rubricBox"), false);
  };

  $("loadDayBtn").onclick = () => loadSelectedDay().catch(e => alert(e.message));
  $("saveDayNoteBtn").onclick = () => loadSelectedDay().catch(e => alert(e.message));
  $("refreshDayBtn").onclick = () => refreshDayDetails().catch(e => alert(e.message));

  $("closeDayBtn").onclick = () => closeDay().catch(e => alert(e.message));
  $("reopenDayBtn").onclick = () => reopenDay().catch(e => alert(e.message));

  $("taskForm").onsubmit = (e) => addTask(e).catch(err => alert(err.message));
  $("fastModeToggle").onchange = () => {
    const on = $("fastModeToggle").checked;
    show($("normalTaskBox"), !on);
    show($("fastTaskBox"), on);
  };
  $("fastSaveBtn").onclick = () => saveFastLog().catch(err => alert(err.message));

  // Approvals
  $("refreshApprovalsBtn").onclick = () => refreshApprovals().catch(e => alert(e.message));
  $("filterApprovalsBtn").onclick = () => refreshApprovals().catch(e => alert(e.message));
  $("bulkApproveBtn").onclick = () => bulkDecision("approved").catch(e => alert(e.message));
  $("bulkRejectBtn").onclick = () => bulkDecision("rejected").catch(e => alert(e.message));

  // Payroll
  $("payrollRefreshBtn").onclick = () => refreshPayroll().catch(e => alert(e.message));
  $("createRunBtn").onclick = () => createRun().catch(e => alert(e.message));
  $("refreshRunsBtn").onclick = () => refreshRuns().catch(e => alert(e.message));
  
  if ($("refreshDueBtn")) $("refreshDueBtn").onclick = () => refreshDue().catch(e => alert(e.message));


  // Reports
  $("refreshReportsBtn").onclick = () => refreshReports().catch(e => alert(e.message));
  $("exportTaskTotalsBtn").onclick = () => {
    const s = $("reportStart").value, e = $("reportEnd").value;
    window.location.href = `/api/reports/task-totals/export.csv${qs({ start:s, end:e })}`;
  };
  $("exportWorkstationsBtn").onclick = () => {
    const s = $("reportStart").value, e = $("reportEnd").value;
    window.location.href = `/api/reports/by-workstation/export.csv${qs({ start:s, end:e })}`;
  };
  $("exportSupervisorsBtn").onclick = () => {
    const s = $("reportStart").value, e = $("reportEnd").value;
    window.location.href = `/api/reports/by-supervisor/export.csv${qs({ start:s, end:e })}`;
  };

  // Audit (admin)
  if ($("refreshAuditBtn")) $("refreshAuditBtn").onclick = () => refreshAudit().catch(e => alert(e.message));

  // -------- ADMIN tab wiring --------
  const refreshAdminListsBtn = $("refreshAdminListsBtn");
  if (refreshAdminListsBtn) refreshAdminListsBtn.onclick = () => refreshAdminData().catch(e => alert(e.message));

  const createFactoryForm = $("createFactoryForm");
  if (createFactoryForm) createFactoryForm.onsubmit = async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    await API.createFactory({ name: fd.get("name") });
    e.target.reset();
    await refreshAdminData();
    alert("Factory created.");
  };

  const createTeamForm = $("createTeamForm");
  if (createTeamForm) createTeamForm.onsubmit = async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    await API.createTeam({ factory_id: fd.get("factory_id"), name: fd.get("name") });
    e.target.reset();
    await refreshAdminData();
    alert("Team created.");
  };

  const createWorkstationForm = $("createWorkstationForm");
  if (createWorkstationForm) createWorkstationForm.onsubmit = async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    await API.createWorkstation({ factory_id: fd.get("factory_id"), name: fd.get("name") });
    e.target.reset();
    await refreshAdminData();
    alert("Workstation created.");
  };

  const createAppUserForm = $("createAppUserForm");
  if (createAppUserForm) createAppUserForm.onsubmit = async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const factory_id = fd.get("factory_id") || null;
    await API.createAppUser({
      email: fd.get("email"),
      password: fd.get("password"),
      role: fd.get("role"),
      factory_id: factory_id || null
    });
    e.target.reset();
    await refreshAdminData();
    alert("App user created.");
  };

  // assign supervisor -> factory
  const appUserSelect = $("appUserSelect");
  if (appUserSelect) appUserSelect.onchange = () => {
    const id = appUserSelect.value;
    const u = appUsers.find(x => x.id === id);
    if (!u) return;

    const sel = $("appUserFactorySelect");
    if (!sel) return;

    // Set factory selection
    if (u.factory_id) sel.value = u.factory_id;
    else sel.value = "__NONE__";

    $("appUserMeta").innerHTML = `
      <div><b>${escapeHtml(u.email)}</b> (${escapeHtml(u.role)})</div>
      <div class="small">Active: ${u.is_active}</div>
      <div class="small">Factory scope: ${escapeHtml(u.factory_id || "None")}</div>
    `;
    show($("appUserMeta"), true);
  };

  const saveUserScopeBtn = $("saveUserScopeBtn");
  if (saveUserScopeBtn) saveUserScopeBtn.onclick = async () => {
    const userId = $("appUserSelect").value;
    if (!userId) return alert("Select a user");
    const v = $("appUserFactorySelect").value;
    const factory_id = (v === "__NONE__" || v === "") ? null : v;
    await API.updateAppUser(userId, { factory_id });
    await refreshAdminData();
    alert("User scope saved.");
  };

  const toggleUserActiveBtn = $("toggleUserActiveBtn");
  if (toggleUserActiveBtn) toggleUserActiveBtn.onclick = async () => {
    const userId = $("appUserSelect").value;
    if (!userId) return alert("Select a user");
    const u = appUsers.find(x => x.id === userId);
    if (!u) return;
    await API.updateAppUser(userId, { is_active: !u.is_active });
    await refreshAdminData();
    alert("User active status toggled.");
  };

  // Create worker
  const createWorkerForm = $("createWorkerForm");
  if (createWorkerForm) createWorkerForm.onsubmit = async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    const factory_id = fd.get("factory_id") || null;
    const team_id = fd.get("team_id") || null;

    await API.createWorker({
      worker_code: fd.get("worker_code") || null,
      full_name: fd.get("full_name"),
      payout: fd.get("payout"),
      payout_anchor_date: fd.get("payout_anchor_date"),
      factory_id: factory_id || null,
      team_id: team_id || null
    });
    e.target.reset();
    await refreshAdminData();
    await loadBootstrapData();
    alert("Worker created.");
  };

  // Assign worker -> factory/team/payout
  const adminWorkerSelect = $("adminWorkerSelect");
  if (adminWorkerSelect) adminWorkerSelect.onchange = () => {
    const id = adminWorkerSelect.value;
    const w = workersAll.find(x => x.id === id);
    if (!w) return;

    const factorySel = $("workerFactorySelect");
    const teamSel = $("workerTeamSelect");

    if (w.factory_id) factorySel.value = w.factory_id;
    else factorySel.value = "__NONE__";

    // filter teams by factory
    const fId = w.factory_id || null;
    const tFiltered = filterTeamsByFactory(fId);
    renderOptions(teamSel, tFiltered, x => x.name, "No team", true);
    if (w.team_id) teamSel.value = w.team_id;
    else teamSel.value = "__NONE__";

    $("workerPayoutSelect").value = w.payout;
    $("workerAnchorDate").value = w.payout_anchor_date;

    $("workerMeta").innerHTML = `
    <div><b>${escapeHtml(w.full_name)}</b> ${w.worker_code ? `<span class="small">(${escapeHtml(w.worker_code)})</span>` : ""}</div>
    <div class="small">Factory: ${escapeHtml(w.factory_id || "None")}</div>
    <div class="small">Team: ${escapeHtml(w.team_id || "None")}</div>
    <div class="small">Active: <b>${w.is_active}</b></div>
`   ;
    show($("workerMeta"), true);

  };

  // when workerFactorySelect changes, refilter workerTeamSelect
  const workerFactorySelect = $("workerFactorySelect");
  if (workerFactorySelect) workerFactorySelect.onchange = () => {
    const v = workerFactorySelect.value;
    const factory_id = (v === "__NONE__" || v === "") ? null : v;
    const tFiltered = filterTeamsByFactory(factory_id);
    renderOptions($("workerTeamSelect"), tFiltered, x => x.name, "No team", true);
    $("workerTeamSelect").value = "__NONE__";
  };

  const saveWorkerAssignBtn = $("saveWorkerAssignBtn");
  if (saveWorkerAssignBtn) saveWorkerAssignBtn.onclick = async () => {
    const workerId = $("adminWorkerSelect").value;
    if (!workerId) return alert("Select a worker");

    const fv = $("workerFactorySelect").value;
    const tv = $("workerTeamSelect").value;

    const factory_id = (fv === "__NONE__" || fv === "") ? null : fv;
    const team_id = (tv === "__NONE__" || tv === "") ? null : tv;

    const payout = $("workerPayoutSelect").value;
    const anchor = $("workerAnchorDate").value;
    if (!anchor) return alert("Anchor date is required");

    await API.updateWorker(workerId, {
      factory_id,
      team_id,
      payout,
      payout_anchor_date: anchor
    });

    await refreshAdminData();
    await loadBootstrapData();
    alert("Worker updated.");
  };

  const toggleWorkerActiveBtn = $("toggleWorkerActiveBtn");
  if (toggleWorkerActiveBtn) toggleWorkerActiveBtn.onclick = async () => {
  const workerId = $("adminWorkerSelect").value;
  if (!workerId) return alert("Select a worker");

  const w = workersAll.find(x => x.id === workerId);
  if (!w) return alert("Worker not found");

  const next = !w.is_active;
  const ok = confirm(`Set worker "${w.full_name}" to ${next ? "ACTIVE" : "INACTIVE"}?`);
  if (!ok) return;

  await API.updateWorker(workerId, { is_active: next });

  await refreshAdminData();   // refresh admin tables + workersAll
  await loadBootstrapData();  // refresh daily dropdowns (active only)

  alert(`Worker is now ${next ? "ACTIVE" : "INACTIVE"}.`);
  
 };


  // -------- Autologin --------
  if (token) {
    try {
      await loadMeAndShowApp();
      await loadBootstrapData();
      await updateSyncBadge();
      setActiveTab("daily");
      await refreshApprovals();
      await refreshRuns();
      await refreshDue();
      if (me.role === "admin") await refreshAdminData();
    } catch {
      token = ""; localStorage.removeItem("token");
      show($("loginView"), true);
      show($("appView"), false);
    }
  }
}

boot();

