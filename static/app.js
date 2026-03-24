const tableSelect = document.getElementById("tableSelect");
const searchInput = document.getElementById("searchInput");
const searchBtn = document.getElementById("searchBtn");
const sidebar = document.getElementById("sidebar");
const statusEl = document.getElementById("status");
const resultsEl = document.getElementById("results");

let currentFilters = [];
let activeRanges = {};

function setStatus(msg) {
  statusEl.textContent = msg || "";
}

async function loadTables() {
  const res = await fetch("/api/tables");
  const data = await res.json();

  tableSelect.innerHTML = "";
  for (const t of data.tables || []) {
    const opt = document.createElement("option");
    opt.value = t;
    opt.textContent = t;
    tableSelect.appendChild(opt);
  }

  if (tableSelect.value) {
    await loadFilters();
    await runQuery();
  }
}

async function loadFilters() {
  const table = tableSelect.value;
  const res = await fetch(`/api/filter_options?table=${encodeURIComponent(table)}`);
  const data = await res.json();

  currentFilters = data.filters || [];
  activeRanges = {};
  renderSidebar();
}

function renderSidebar() {
  sidebar.innerHTML = "";

  for (const filter of currentFilters) {
    const wrap = document.createElement("details");
    wrap.className = "filter-group";
    wrap.open = true; // expanded by default

    const summary = document.createElement("summary");
    summary.className = "filter-summary";
    summary.textContent = filter.column;

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

        label.appendChild(input);
        label.append(" " + value);
        content.appendChild(label);
      }
    }

    if (filter.kind === "numeric") {
      const sliderWrap = document.createElement("div");
      sliderWrap.className = "slider-group";
      sliderWrap.dataset.column = filter.column;
      sliderWrap.dataset.role = "numeric-filter";

      const defaultMin = 0;
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
      valueLabel.textContent = `${defaultMin} - ${defaultMax}`;

      const minSlider = document.createElement("input");
      minSlider.type = "range";
      minSlider.min = 0;
      minSlider.max = filter.max;
      minSlider.value = defaultMin;
      minSlider.step = 1;
      minSlider.className = "range-slider range-min";
      minSlider.disabled = true;

      const maxSlider = document.createElement("input");
      maxSlider.type = "range";
      maxSlider.min = 0;
      maxSlider.max = filter.max;
      maxSlider.value = defaultMax;
      maxSlider.step = 1;
      maxSlider.className = "range-slider range-max";
      maxSlider.disabled = true;

      function syncSliderLabel() {
        let minVal = Number(minSlider.value);
        let maxVal = Number(maxSlider.value);

        if (minVal > maxVal) {
          if (document.activeElement === minSlider) {
            maxVal = minVal;
            maxSlider.value = maxVal;
          } else {
            minVal = maxVal;
            minSlider.value = minVal;
          }
        }

        valueLabel.textContent = `${minVal} - ${maxVal}`;
      }

      enableBox.addEventListener("change", () => {
        const enabled = enableBox.checked;
        minSlider.disabled = !enabled;
        maxSlider.disabled = !enabled;
        runQuery();
      });

      minSlider.addEventListener("input", () => {
        syncSliderLabel();
        if (enableBox.checked) runQuery();
      });

      maxSlider.addEventListener("input", () => {
        syncSliderLabel();
        if (enableBox.checked) runQuery();
      });

      sliderWrap.appendChild(enableLabel);
      sliderWrap.appendChild(valueLabel);
      sliderWrap.appendChild(minSlider);
      sliderWrap.appendChild(maxSlider);

      content.appendChild(sliderWrap);
    }

    sidebar.appendChild(wrap);
  }

  sidebar.querySelectorAll('input[type="checkbox"]:not([data-role="range-enabled"])').forEach(el => {
    el.addEventListener("change", runQuery);
  });
}

function gatherCategoricalFilters() {
  const result = {};
  sidebar.querySelectorAll('input[type="checkbox"]:checked').forEach(cb => {
    const col = cb.dataset.column;
    if (!result[col]) result[col] = [];
    result[col].push(cb.value);
  });
  return result;
}

function gatherRanges() {
  const result = {};

  sidebar.querySelectorAll('[data-role="numeric-filter"]').forEach(group => {
    const column = group.dataset.column;
    const enabled = group.querySelector(".range-enable")?.checked;
    if (!enabled) return;

    const minSlider = group.querySelector(".range-min");
    const maxSlider = group.querySelector(".range-max");
    if (!minSlider || !maxSlider) return;

    result[column] = {
      min: Number(minSlider.value),
      max: Number(maxSlider.value)
    };
  });

  return result;
}

async function runQuery() {
  const payload = {
    table: tableSelect.value,
    search: searchInput.value.trim(),
    filters: gatherCategoricalFilters(),
    ranges: gatherRanges()
  };

  setStatus("Loading...");

  const res = await fetch("/api/query", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify(payload)
  });

  const data = await res.json();

  if (!res.ok) {
    setStatus(data.error || "Query failed");
    resultsEl.innerHTML = "";
    return;
  }

  renderTable(data.columns || [], data.rows || []);
  setStatus(`Rows shown: ${(data.rows || []).length}`);
}

function renderTable(columns, rows) {
  if (!rows.length) {
    resultsEl.innerHTML = "<p>No matching rows.</p>";
    return;
  }

  let html = "<table><thead><tr>";
  html += columns.map(c => `<th>${c}</th>`).join("");
  html += "</tr></thead><tbody>";

  for (const row of rows) {
    html += "<tr>" + columns.map(c => `<td>${row[c] ?? ""}</td>`).join("") + "</tr>";
  }

  html += "</tbody></table>";
  resultsEl.innerHTML = html;
}

tableSelect.addEventListener("change", async () => {
  await loadFilters();
  await runQuery();
});

searchBtn.addEventListener("click", runQuery);
searchInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") runQuery();
});

loadTables();