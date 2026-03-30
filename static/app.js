const tableSelect    = document.getElementById("tableSelect");
const sortSelect     = document.getElementById("sortSelect");
const sortDir        = document.getElementById("sortDir");
const searchInput    = document.getElementById("searchInput");
const sidebarFilters = document.getElementById("sidebar-filters");
const statusEl       = document.getElementById("status");
const resultsEl      = document.getElementById("results");

let currentFilters = [];
let currentColumns = [];
let uiConfig = { card_fields: [], modal_groups: [] };

// ── Status ────────────────────────────────────────────────────
function setStatus(msg) {
  statusEl.textContent = msg || "";
}

// ── Tables ────────────────────────────────────────────────────
async function loadTables() {
  const res  = await fetch("/api/tables");
  const data = await res.json();

  tableSelect.innerHTML = "";
  for (const t of data.tables || []) {
    const opt = document.createElement("option");
    opt.value = t; opt.textContent = t;
    tableSelect.appendChild(opt);
  }

  if (tableSelect.value) {
    await Promise.all([loadUiConfig(), loadFilters()]);
    await runQuery();
  }
}

// ── UI Config ─────────────────────────────────────────────────
async function loadUiConfig() {
  const table = tableSelect.value;
  const res   = await fetch(`/api/ui_config?table=${encodeURIComponent(table)}`);
  const data  = await res.json();
  uiConfig = {
    card_fields:  data.card_fields  || [],
    modal_groups: data.modal_groups || [],
  };
}

// ── Filters ───────────────────────────────────────────────────
async function loadFilters() {
  const table = tableSelect.value;
  const res   = await fetch(`/api/filter_options?table=${encodeURIComponent(table)}`);
  const data  = await res.json();
  currentFilters = data.filters || [];
  renderSidebar();
}

function renderSidebar() {
  sidebarFilters.innerHTML = "";

  for (const filter of currentFilters) {
    const wrap = document.createElement("details");
    wrap.className = "filter-group";
    wrap.open = true;

    const summary = document.createElement("summary");
    summary.className = "filter-summary";
    summary.innerHTML = `<span class="filter-arrow">▶</span>${filter.column}`;

    const content = document.createElement("div");
    content.className = "filter-content";

    wrap.appendChild(summary);
    wrap.appendChild(content);

    if (filter.kind === "categorical") {
      for (const value of filter.options) {
        const label = document.createElement("label");
        label.className = "check";
        const input = document.createElement("input");
        input.type = "checkbox";
        input.value = value;
        input.dataset.column = filter.column;
        input.addEventListener("change", runQuery);
        label.appendChild(input);
        label.append(" " + value);
        content.appendChild(label);
      }
    }

    if (filter.kind === "numeric") {
      const sliderWrap = document.createElement("div");
      sliderWrap.className = "slider-group";
      sliderWrap.dataset.column = filter.column;
      sliderWrap.dataset.role   = "numeric-filter";

      const defaultMax = Number(filter.max);

      const enableLabel = document.createElement("label");
      enableLabel.className = "check";
      const enableBox = document.createElement("input");
      enableBox.type = "checkbox";
      enableBox.className = "range-enable";
      enableLabel.appendChild(enableBox);
      enableLabel.append(" Enable filter");

      const valueLabel = document.createElement("div");
      valueLabel.className = "slider-values";
      valueLabel.textContent = `0 – ${defaultMax}`;

      const minSlider = document.createElement("input");
      minSlider.type = "range"; minSlider.min = 0;
      minSlider.max = filter.max; minSlider.value = 0;
      minSlider.step = 1; minSlider.className = "range-slider range-min";
      minSlider.disabled = true;

      const maxSlider = document.createElement("input");
      maxSlider.type = "range"; maxSlider.min = 0;
      maxSlider.max = filter.max; maxSlider.value = defaultMax;
      maxSlider.step = 1; maxSlider.className = "range-slider range-max";
      maxSlider.disabled = true;

      function syncSliderLabel() {
        let lo = Number(minSlider.value), hi = Number(maxSlider.value);
        if (lo > hi) {
          if (document.activeElement === minSlider) { hi = lo; maxSlider.value = hi; }
          else                                       { lo = hi; minSlider.value = lo; }
        }
        valueLabel.textContent = `${lo} – ${hi}`;
      }

      enableBox.addEventListener("change", () => {
        minSlider.disabled = maxSlider.disabled = !enableBox.checked;
        runQuery();
      });
      minSlider.addEventListener("input", () => { syncSliderLabel(); if (enableBox.checked) runQuery(); });
      maxSlider.addEventListener("input", () => { syncSliderLabel(); if (enableBox.checked) runQuery(); });

      sliderWrap.append(enableLabel, valueLabel, minSlider, maxSlider);
      content.appendChild(sliderWrap);
    }

    sidebarFilters.appendChild(wrap);
  }
}

function gatherCategoricalFilters() {
  const result = {};
  sidebarFilters.querySelectorAll('input[type="checkbox"][data-column]:checked').forEach(cb => {
    const col = cb.dataset.column;
    if (!result[col]) result[col] = [];
    result[col].push(cb.value);
  });
  return result;
}

function gatherRanges() {
  const result = {};
  sidebarFilters.querySelectorAll('[data-role="numeric-filter"]').forEach(group => {
    if (!group.querySelector(".range-enable")?.checked) return;
    const col = group.dataset.column;
    result[col] = {
      min: Number(group.querySelector(".range-min").value),
      max: Number(group.querySelector(".range-max").value)
    };
  });
  return result;
}

// ── Query ─────────────────────────────────────────────────────
async function runQuery() {
  const payload = {
    table:   tableSelect.value,
    search:  searchInput.value.trim(),
    filters: gatherCategoricalFilters(),
    ranges:  gatherRanges()
  };

  setStatus("Loading…");
  resultsEl.innerHTML = `<div class="state-msg">Loading…</div>`;

  const res  = await fetch("/api/query", {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify(payload)
  });
  const data = await res.json();

  if (!res.ok) {
    setStatus(data.error || "Query failed");
    resultsEl.innerHTML = `<div class="state-msg">${data.error || "Query failed"}</div>`;
    return;
  }

  currentColumns = data.columns || [];
  updateSortOptions(currentColumns);

  let rows = data.rows || [];
  rows = sortRows(rows);
  renderTiles(rows);
  setStatus(`${rows.length} result${rows.length !== 1 ? "s" : ""}`);
}

// ── Sort ──────────────────────────────────────────────────────
function updateSortOptions(columns) {
  const current = sortSelect.value;
  sortSelect.innerHTML = `<option value="">— none —</option>`;
  for (const col of columns) {
    const opt = document.createElement("option");
    opt.value = col; opt.textContent = col;
    if (col === current) opt.selected = true;
    sortSelect.appendChild(opt);
  }
}

function sortRows(rows) {
  const col = sortSelect.value;
  const dir = sortDir.value;
  if (!col) return rows;
  return [...rows].sort((a, b) => {
    const av = a[col], bv = b[col];
    const an = parseFloat(av), bn = parseFloat(bv);
    const numeric = !isNaN(an) && !isNaN(bn);
    let cmp = numeric ? an - bn : String(av ?? "").localeCompare(String(bv ?? ""));
    return dir === "desc" ? -cmp : cmp;
  });
}

// ── Helpers ───────────────────────────────────────────────────
const TILE_PREVIEW_MAX = 5;

function isBlank(v) {
  if (v === null || v === undefined) return true;
  const s = String(v).trim().toLowerCase();
  return s === "" || s === "na" || s === "n/a" || s === "null" || s === "none";
}

function escHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;")
    .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

// ── Tile rendering ────────────────────────────────────────────
function getCardFields(row) {
  const configured = uiConfig.card_fields;

  if (configured.length > 0) {
    const matched = configured
      .filter(f => f in row && !isBlank(row[f]))
      .slice(0, TILE_PREVIEW_MAX);
    if (matched.length > 0) return matched;
  }

  // Fallback: first N populated fields in row order
  return Object.keys(row)
    .filter(k => !isBlank(row[k]))
    .slice(0, TILE_PREVIEW_MAX);
}

function renderTiles(rows) {
  if (!rows.length) {
    resultsEl.innerHTML = `<div class="state-msg">No results found.</div>`;
    return;
  }
  resultsEl.innerHTML = rows.map((row, i) => buildTileHTML(row, i)).join("");
  resultsEl.querySelectorAll(".tile").forEach((el, i) => {
    el.addEventListener("click", () => openModal(rows[i]));
  });
}

function buildTileHTML(row, index) {
  const fields = getCardFields(row);
  if (!fields.length) return "";

  const [titleField, ...restFields] = fields;
  const titleVal = row[titleField];

  const allPopulated = Object.keys(row).filter(k => !isBlank(row[k]));
  const shownSet = new Set(fields);
  const extraCount = allPopulated.filter(k => !shownSet.has(k)).length;

  const fieldsHTML = restFields.map(k => `
    <div class="tile-field">
      <span class="field-key">${escHtml(k)}</span>
      <span class="field-val">${escHtml(String(row[k]))}</span>
    </div>`).join("");

  const footerHTML = extraCount > 0
    ? `<div class="tile-footer">+${extraCount} more field${extraCount !== 1 ? "s" : ""}</div>`
    : "";

  return `
    <div class="tile" style="animation-delay:${Math.min(index * 20, 300)}ms">
      <div class="tile-title">${escHtml(String(titleVal))}</div>
      <div class="tile-fields">${fieldsHTML}</div>
      ${footerHTML}
    </div>`;
}

// ── Modal rendering ───────────────────────────────────────────
function buildModalGroups(row) {
  const groups  = uiConfig.modal_groups;
  const allKeys = Object.keys(row).filter(k => !isBlank(row[k]));
  const claimed = new Set();
  const rendered = [];

  for (const group of groups) {
    const fields = (group.fields || []).filter(f => f in row && !isBlank(row[f]));
    if (!fields.length) continue;
    fields.forEach(f => claimed.add(f));
    rendered.push({ label: group.label, fields });
  }

  // Any field not in a named group goes into "Other"
  const remainder = allKeys.filter(k => !claimed.has(k));
  if (remainder.length > 0) {
    rendered.push({ label: "Other", fields: remainder });
  }

  return rendered;
}

function openModal(row) {
  const overlay = document.getElementById("modal-overlay");
  const content = document.getElementById("modal-content");

  // Title: first configured card field that's populated, else first populated key
  const titleField =
    uiConfig.card_fields.find(f => f in row && !isBlank(row[f])) ||
    Object.keys(row).find(k => !isBlank(row[k]));
  const titleVal = titleField ? row[titleField] : "Entry";

  const groups = buildModalGroups(row);

  const groupsHTML = groups.map(group => {
    const rowsHTML = group.fields.map(f => `
      <span class="modal-key">${escHtml(f)}</span>
      <span class="modal-val">${escHtml(String(row[f]))}</span>`).join("");

    return `
      <div class="modal-group">
        <div class="modal-group-label">${escHtml(group.label)}</div>
        <div class="modal-fields">${rowsHTML}</div>
      </div>`;
  }).join("");

  content.innerHTML = `
    <div class="modal-title">${escHtml(String(titleVal))}</div>
    ${groupsHTML}`;

  overlay.classList.remove("hidden");
}

function closeModal(e) {
  if (e.target === document.getElementById("modal-overlay")) {
    document.getElementById("modal-overlay").classList.add("hidden");
  }
}

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") document.getElementById("modal-overlay").classList.add("hidden");
});

// ── Event wiring ──────────────────────────────────────────────
tableSelect.addEventListener("change", async () => {
  await Promise.all([loadUiConfig(), loadFilters()]);
  await runQuery();
});

sortSelect.addEventListener("change", runQuery);
sortDir.addEventListener("change", runQuery);
searchInput.addEventListener("input", debounce(runQuery, 300));
searchInput.addEventListener("keydown", (e) => { if (e.key === "Enter") runQuery(); });

function debounce(fn, ms) {
  let t;
  return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}

// ── Init ──────────────────────────────────────────────────────
loadTables();
