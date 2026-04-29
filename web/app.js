const state = {
  strategy: null,
  log: null,
  result: null,
  assetSortRobust: false,
  selectedPath: null,
};

const $ = (id) => document.getElementById(id);

function setStatus(text) {
  $("status").textContent = text;
}

function formatNumber(value, digits = 1) {
  const n = Number(value || 0);
  return n.toLocaleString("fr-FR", {
    maximumFractionDigits: digits,
    minimumFractionDigits: Math.abs(n - Math.round(n)) > 1e-9 ? digits : 0,
  });
}

function classFor(value) {
  const n = Number(value || 0);
  if (n > 0) return "pos";
  if (n < 0) return "neg";
  return "";
}

function wireTabs() {
  document.querySelectorAll(".tab").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((tab) => tab.classList.remove("active"));
      document.querySelectorAll(".view").forEach((view) => view.classList.remove("active"));
      button.classList.add("active");
      $(button.dataset.tab).classList.add("active");
      if (button.dataset.tab === "results" && state.result) drawHistogram(state.result.paths || []);
      if (button.dataset.tab === "paths" && state.result) renderPathView();
    });
  });
}

function wireFileInput(inputId, dropId, targetKey, labelId, multiple = false) {
  const input = $(inputId);
  const drop = $(dropId);
  const label = $(labelId);

  function assign(files) {
    const list = Array.from(files || []);
    if (multiple) {
      state[targetKey] = list;
      label.textContent = `${list.length} fichier${list.length > 1 ? "s" : ""}`;
    } else {
      state[targetKey] = list[0] || null;
      label.textContent = state[targetKey] ? state[targetKey].name : "aucun fichier";
    }
  }

  input.addEventListener("change", (event) => assign(event.target.files));
  ["dragenter", "dragover"].forEach((eventName) => {
    drop.addEventListener(eventName, (event) => {
      event.preventDefault();
      drop.classList.add("dragover");
    });
  });
  ["dragleave", "drop"].forEach((eventName) => {
    drop.addEventListener(eventName, (event) => {
      event.preventDefault();
      drop.classList.remove("dragover");
    });
  });
  drop.addEventListener("drop", (event) => assign(event.dataTransfer.files));
}

function wireRandomToggles() {
  [
    ["randomizeDrift", "drift"],
    ["randomizeVol", "vol"],
    ["randomizeHurst", "hurst"],
  ].forEach(([toggleId, inputId]) => {
    const toggle = $(toggleId);
    const input = $(inputId);
    toggle.addEventListener("change", () => {
      input.disabled = toggle.checked;
    });
  });
}

async function loadDefaultDataStatus() {
  try {
    const response = await fetch("/api/default-data");
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || "default data failed");
    $("dataStatus").innerHTML = `
      <strong>Round 5 data preloaded</strong><br />
      datamodel: ${payload.datamodel}<br />
      prices: ${payload.price_count} fichiers<br />
      trades: ${payload.trade_count} fichiers
    `;
  } catch (error) {
    $("dataStatus").textContent = `Erreur data Round 5: ${error.message}`;
  }
}

async function runSimulation() {
  if (!state.strategy) {
    setStatus("Strategy missing");
    return;
  }

  const button = $("runButton");
  button.disabled = true;
  setStatus("Running...");

  const form = new FormData();
  form.append("strategy", state.strategy);
  form.append("simulations", $("simulations").value);
  form.append("drift", $("drift").value);
  form.append("vol", $("vol").value);
  form.append("hurst", $("hurst").value);
  form.append("seed", $("seed").value);
  form.append("randomize_drift", $("randomizeDrift").checked ? "true" : "false");
  form.append("randomize_vol", $("randomizeVol").checked ? "true" : "false");
  form.append("randomize_hurst", $("randomizeHurst").checked ? "true" : "false");

  try {
    const response = await fetch("/api/run", { method: "POST", body: form });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || "run failed");
    state.result = payload;
    state.selectedPath = payload.paths && payload.paths.length ? payload.paths[0].path : null;
    renderResults(payload);
    renderPathView();
    setStatus("Done");
    document.querySelector('[data-tab="results"]').click();
  } catch (error) {
    setStatus("Error");
    alert(error.message);
  } finally {
    button.disabled = false;
  }
}

async function analyzeLog() {
  if (!state.log) {
    setStatus("Log missing");
    return;
  }
  const button = $("analyzeButton");
  button.disabled = true;
  setStatus("Analyzing...");

  const form = new FormData();
  form.append("log", state.log);

  try {
    const response = await fetch("/api/analyze-log", { method: "POST", body: form });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || "analysis failed");
    renderLogAnalysis(payload);
    setStatus("Done");
  } catch (error) {
    setStatus("Error");
    alert(error.message);
  } finally {
    button.disabled = false;
  }
}

function renderResults(result) {
  const summary = result.summary || {};
  const metrics = [
    ["Mean PnL", summary.mean_pnl, summary.mean_pnl],
    ["Robust PnL", summary.mean_robust_pnl, summary.mean_robust_pnl],
    ["Mean Edge", summary.mean_edge, summary.mean_edge],
    ["Mean Carry", summary.mean_carry, summary.mean_carry],
    ["Positive", `${formatNumber((summary.positive_rate || 0) * 100, 1)}%`, summary.positive_rate - 0.5],
  ];
  $("metrics").innerHTML = metrics
    .map(([label, value, tone]) => {
      const cls = Number(tone) > 0 ? "good" : Number(tone) < 0 ? "bad" : "warn";
      const formatted = typeof value === "string" ? value : formatNumber(value);
      return `<div class="metric ${cls}"><span>${label}</span><strong>${formatted}</strong></div>`;
    })
    .join("");

  $("pathCount").textContent = `${summary.paths || 0} paths`;
  drawHistogram(result.paths || []);
  renderBotTable(result.bots || {});
  renderAssetTable(result.assets || []);
}

function renderBotTable(bots) {
  $("botSummary").textContent = `${bots.total_events || 0} events / ${bots.unique_timestamps || 0} timestamps`;
  renderTable($("botTable"), bots.assets || [], [
    ["Asset", "asset"],
    ["Events", "events"],
    ["Buy", "buy_events"],
    ["Sell", "sell_events"],
    ["Qty", "quantity"],
    ["Avg Qty", "avg_quantity"],
    ["Avg Offset", "avg_offset"],
    ["Avg |Offset|", "avg_abs_offset"],
    ["Timestamps", "unique_timestamps"],
  ]);
  renderTable($("botTimestampTable"), bots.top_timestamps || [], [
    ["Day", "day"],
    ["Timestamp", "timestamp"],
    ["Events", "events"],
    ["Qty", "quantity"],
    ["Assets", "assets"],
  ]);
}

function renderAssetTable(rows) {
  const sorted = [...rows].sort((a, b) => {
    const key = state.assetSortRobust ? "mean_robust_pnl" : "mean_pnl";
    return Number(b[key] || 0) - Number(a[key] || 0);
  });
  renderTable($("assetTable"), sorted, [
    ["Asset", "asset"],
    ["PnL", "mean_pnl"],
    ["Edge", "mean_edge"],
    ["Carry", "mean_carry"],
    ["Robust", "mean_robust_pnl"],
    ["Pos %", "positive_rate", (v) => `${formatNumber(Number(v) * 100, 1)}%`],
    ["Trades", "mean_trades"],
    ["Abs Pos", "mean_abs_position"],
  ]);
}

function renderPathView() {
  if (!state.result) return;
  renderPathTable(state.result.paths || []);
  renderSelectedPathAssets();
}

function renderPathTable(rows) {
  const sorted = [...rows].sort((a, b) => Number(a.path || 0) - Number(b.path || 0));
  const tableRows = sorted.map((row) => ({
    ...row,
    selected: row.path === state.selectedPath ? "*" : "",
  }));
  renderTable($("pathTable"), tableRows, [
    ["", "selected"],
    ["Path", "path"],
    ["PnL", "pnl"],
    ["Edge", "edge"],
    ["Carry", "carry"],
    ["Trades", "trades"],
  ]);

  Array.from($("pathTable").querySelectorAll("tbody tr")).forEach((tr, index) => {
    const row = tableRows[index];
    tr.addEventListener("click", () => {
      state.selectedPath = row.path;
      renderPathView();
    });
  });
}

function renderSelectedPathAssets() {
  const path = state.selectedPath;
  $("selectedPathLabel").textContent = path === null ? "selectionne un path" : `path ${path}`;
  const limit = state.result.path_asset_limit || 0;
  $("pathAssetLimit").textContent = `details gardes pour les ${limit} premiers paths`;
  const rows = (state.result.path_assets || [])
    .filter((row) => row.path === path)
    .sort((a, b) => Number(b.pnl || 0) - Number(a.pnl || 0));
  renderTable($("pathAssetTable"), rows, [
    ["Asset", "asset"],
    ["PnL", "pnl"],
    ["Edge", "edge"],
    ["Carry", "carry"],
    ["Trades", "trades"],
    ["End Pos", "end_position"],
    ["Vol", "vol"],
    ["Drift", "drift"],
    ["H", "hurst"],
  ]);
}

function renderTable(table, rows, columns) {
  const head = `<thead><tr>${columns.map(([label]) => `<th>${label}</th>`).join("")}</tr></thead>`;
  const body = rows
    .slice(0, 500)
    .map((row) => {
      const cells = columns
        .map(([_label, key, formatter], index) => {
          const raw = row[key];
          const value = formatter ? formatter(raw) : typeof raw === "number" ? formatNumber(raw) : raw;
          const cls = index > 0 && typeof raw === "number" ? classFor(raw) : "";
          return `<td class="${cls}">${value ?? ""}</td>`;
        })
        .join("");
      return `<tr>${cells}</tr>`;
    })
    .join("");
  table.innerHTML = `${head}<tbody>${body}</tbody>`;
}

function drawHistogram(paths) {
  const canvas = $("histogram");
  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#090d12";
  ctx.fillRect(0, 0, width, height);

  const values = paths.map((row) => Number(row.pnl || 0));
  if (!values.length) return;

  const min = Math.min(...values);
  const max = Math.max(...values);
  const bins = Math.min(36, Math.max(8, Math.ceil(Math.sqrt(values.length))));
  const counts = Array.from({ length: bins }, () => 0);
  values.forEach((value) => {
    const index = max === min ? 0 : Math.min(bins - 1, Math.floor(((value - min) / (max - min)) * bins));
    counts[index] += 1;
  });
  const maxCount = Math.max(...counts);
  const pad = 28;
  const barGap = 4;
  const barWidth = (width - pad * 2) / bins - barGap;

  ctx.strokeStyle = "#26313b";
  ctx.beginPath();
  ctx.moveTo(pad, height - pad);
  ctx.lineTo(width - pad, height - pad);
  ctx.stroke();

  counts.forEach((count, index) => {
    const x = pad + index * ((width - pad * 2) / bins);
    const barHeight = ((height - pad * 2) * count) / maxCount;
    const y = height - pad - barHeight;
    const grad = ctx.createLinearGradient(0, y, 0, height - pad);
    grad.addColorStop(0, "#37e6d4");
    grad.addColorStop(1, "#ffbf5c");
    ctx.fillStyle = grad;
    ctx.fillRect(x, y, barWidth, barHeight);
  });

  ctx.fillStyle = "#8da0a8";
  ctx.font = "13px system-ui";
  ctx.fillText(`min ${formatNumber(min)}`, pad, height - 8);
  ctx.fillText(`max ${formatNumber(max)}`, width - pad - 110, height - 8);
}

function renderLogAnalysis(payload) {
  $("logKind").textContent = payload.kind || "-";
  const summary = payload.summary || {};
  $("logSummary").innerHTML = Object.entries(summary)
    .map(([key, value]) => `<div class="log-pill"><span>${key}</span><strong>${typeof value === "number" ? formatNumber(value) : value}</strong></div>`)
    .join("");

  const rows = payload.assets || [];
  const columns = rows[0] && "edge" in rows[0]
    ? [["Asset", "asset"], ["PnL", "pnl"], ["Edge", "edge"], ["Carry", "carry"], ["Trades", "trades"]]
    : [["Asset", "asset"], ["PnL", "pnl"], ["First Mid", "first_mid"], ["Last Mid", "last_mid"], ["Drift", "drift"]];
  renderTable($("logTable"), rows, columns);
}

function boot() {
  wireTabs();
  wireFileInput("strategyInput", "strategyDrop", "strategy", "strategyName");
  wireFileInput("logInput", "logDrop", "log", "logName");
  wireRandomToggles();
  loadDefaultDataStatus();
  $("runButton").addEventListener("click", runSimulation);
  $("analyzeButton").addEventListener("click", analyzeLog);
  $("assetSort").addEventListener("click", () => {
    state.assetSortRobust = !state.assetSortRobust;
    if (state.result) renderAssetTable(state.result.assets || []);
    $("assetSort").textContent = state.assetSortRobust ? "Sort PnL" : "Sort robust";
  });
}

boot();
