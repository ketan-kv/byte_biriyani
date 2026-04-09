const fileInput = document.getElementById("datasetFile");
const runButton = document.getElementById("runButton");
const runDbButton = document.getElementById("runDbButton");
const runStatus = document.getElementById("runStatus");
const resultArea = document.getElementById("resultArea");
const missingStrategy = document.getElementById("missingStrategy");

const tabFile = document.getElementById("tabFile");
const tabDb = document.getElementById("tabDb");
const fileSource = document.getElementById("fileSource");
const dbSource = document.getElementById("dbSource");

const domainValue = document.getElementById("domainValue");
const confidenceValue = document.getElementById("confidenceValue");
const rowsValue = document.getElementById("rowsValue");
const insightCountValue = document.getElementById("insightCountValue");
const storyList = document.getElementById("storyList");
const insightCards = document.getElementById("insightCards");
const pipelineLogs = document.getElementById("pipelineLogs");
const dynamicCharts = document.getElementById("dynamicCharts");
const chatMessages = document.getElementById("chatMessages");
const chatInput = document.getElementById("chatInput");
const chatSendButton = document.getElementById("chatSendButton");
const chatStatus = document.getElementById("chatStatus");
const chatChart = document.getElementById("chatChart");
const chatActions = document.getElementById("chatActions");

let chartIds = [];
let hasAnalysisContext = false;

function getOrCreateChatSessionId() {
  const key = "amdais-chat-session-id";
  const existing = localStorage.getItem(key);
  if (existing) {
    return existing;
  }
  const generated = window.crypto?.randomUUID ? window.crypto.randomUUID() : `session-${Date.now()}`;
  localStorage.setItem(key, generated);
  return generated;
}

const chatSessionId = getOrCreateChatSessionId();

function setStatus(type, text) {
  runStatus.className = `status ${type}`;
  runStatus.textContent = text;
}

function formatPercent(value) {
  return `${(Number(value || 0) * 100).toFixed(1)}%`;
}

function formatLogValue(value) {
  if (value === null || value === undefined) {
    return "-";
  }
  if (typeof value === "number") {
    return Number.isInteger(value) ? value.toLocaleString() : value.toFixed(3);
  }
  if (typeof value === "boolean") {
    return value ? "yes" : "no";
  }
  if (Array.isArray(value)) {
    return `${value.length} item(s)`;
  }
  if (typeof value === "object") {
    return `${Object.keys(value).length} field(s)`;
  }
  return String(value);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function setChatStatus(type, text) {
  if (!chatStatus) {
    return;
  }
  chatStatus.className = `status ${type}`;
  chatStatus.textContent = text;
}

function appendChatMessage(role, text, meta = "") {
  if (!chatMessages) {
    return;
  }
  const item = document.createElement("article");
  item.className = `chat-bubble ${role}`;
  item.innerHTML = `
    <div class="chat-head">${role === "user" ? "You" : "AMDAIS Assistant"}${meta ? ` | ${escapeHtml(meta)}` : ""}</div>
    <p>${escapeHtml(text)}</p>
  `;
  chatMessages.appendChild(item);
  chatMessages.scrollTop = chatMessages.scrollHeight;
}

function renderChatData(data) {
  if (!chatMessages || !data || typeof data !== "object") {
    return;
  }
  const columns = Array.isArray(data.columns) ? data.columns : null;
  const rows = Array.isArray(data.rows) ? data.rows : null;

  if (columns && rows) {
    const preview = rows.slice(0, 8);
    const table = document.createElement("div");
    table.className = "chat-data";
    table.innerHTML = `
      <div class="chat-data-title">Query result preview</div>
      <div class="chat-table-wrap">
        <table>
          <thead><tr>${columns.map((c) => `<th>${escapeHtml(c)}</th>`).join("")}</tr></thead>
          <tbody>
            ${preview
              .map((row) => `<tr>${columns.map((c) => `<td>${escapeHtml(row?.[c] ?? "-")}</td>`).join("")}</tr>`)
              .join("")}
          </tbody>
        </table>
      </div>
    `;
    chatMessages.appendChild(table);
    chatMessages.scrollTop = chatMessages.scrollHeight;
    return;
  }

  const block = document.createElement("pre");
  block.className = "chat-json";
  block.textContent = JSON.stringify(data, null, 2);
  chatMessages.appendChild(block);
  chatMessages.scrollTop = chatMessages.scrollHeight;
}

function renderChatActions(actions) {
  if (!chatActions) {
    return;
  }
  if (!Array.isArray(actions) || !actions.length) {
    chatActions.classList.add("hidden");
    chatActions.innerHTML = "";
    return;
  }

  chatActions.classList.remove("hidden");
  chatActions.innerHTML = actions
    .map(
      (action, idx) => `
        <article class="chat-action-card">
          <h4>Action ${idx + 1}</h4>
          <p><strong>What:</strong> ${escapeHtml(action.description || "")}</p>
          <p><strong>Why:</strong> ${escapeHtml(action.reasoning || "")}</p>
          <p><strong>Impact:</strong> ${escapeHtml(action.expected_impact || "")}</p>
        </article>
      `
    )
    .join("");
}

function renderChatChart(chartPayload) {
  if (!chatChart) {
    return;
  }
  if (!chartPayload || !Array.isArray(chartPayload.data)) {
    chatChart.classList.add("hidden");
    chatChart.innerHTML = "";
    return;
  }

  chatChart.classList.remove("hidden");
  safePlot(
    chatChart,
    chartPayload.data,
    chartPayload.layout || {},
    {
      responsive: true,
      displaylogo: false,
    },
    "Could not render this chat chart for the current dataset."
  );
}

async function sendChatMessage() {
  const message = String(chatInput?.value || "").trim();
  if (!message) {
    return;
  }
  if (!hasAnalysisContext) {
    setChatStatus("error", "Run an analysis first so the copilot has dataset context.");
    return;
  }

  appendChatMessage("user", message);
  chatInput.value = "";
  chatSendButton.disabled = true;
  setChatStatus("loading", "AMDAIS Assistant is thinking...");

  try {
    const response = await fetch("/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message, session_id: chatSessionId }),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.detail || "Chat request failed");
    }

    appendChatMessage("assistant", payload.message || "No response", payload.intent || "");
    renderChatData(payload.data || {});
    renderChatActions(payload.actions || []);
    renderChatChart(payload.chart || null);
    setChatStatus("ok", "AMDAIS Assistant response ready");
  } catch (error) {
    appendChatMessage("assistant", `I could not complete that request: ${error.message}`);
    setChatStatus("error", `Chat failed: ${error.message}`);
  } finally {
    chatSendButton.disabled = false;
  }
}

function getUserPreferences() {
  return {
    missing_strategy: missingStrategy?.value || "none",
  };
}

function activateTab(type) {
  const fileActive = type === "file";
  tabFile.classList.toggle("active", fileActive);
  tabDb.classList.toggle("active", !fileActive);
  fileSource.classList.toggle("hidden", !fileActive);
  dbSource.classList.toggle("hidden", fileActive);
}

function renderStoryline(storyline) {
  if (!Array.isArray(storyline) || !storyline.length) {
    storyList.innerHTML = "<p>No executive storyline generated.</p>";
    return;
  }
  storyList.innerHTML = storyline
    .map((item) => {
      const type = String(item.type || "context");
      return `
        <article class="story-card ${type}">
          <h4>${item.title || "Story point"}</h4>
          <p>${item.message || ""}</p>
        </article>
      `;
    })
    .join("");
}

function renderPipelineLogs(logs) {
  if (!Array.isArray(logs) || !logs.length) {
    pipelineLogs.innerHTML = "<p>No pipeline logs available.</p>";
    return;
  }
  pipelineLogs.innerHTML = logs
    .map((log) => {
      const details = JSON.stringify(log.details || {}, null, 2);
      const detailEntries = Object.entries(log.details || {}).slice(0, 8);
      const detailSummary = detailEntries
        .map(([key, value]) => `<span class="log-kv"><b>${key}</b> ${formatLogValue(value)}</span>`)
        .join("");
      return `
        <article class="log-item">
          <div class="log-head">
            <span class="log-step">${log.step || "step"}</span>
            <span class="log-agent">${log.agent || "agent"} | ${log.status || "ok"}</span>
          </div>
          <div class="log-summary">${detailSummary || "No summarized details"}</div>
          <details class="log-raw">
            <summary>View raw details</summary>
            <pre>${details}</pre>
          </details>
        </article>
      `;
    })
    .join("");
}

function renderInsights(insights) {
  if (!Array.isArray(insights) || insights.length === 0) {
    insightCards.innerHTML = "<p>No insights returned.</p>";
    return;
  }

  insightCards.innerHTML = insights
    .map((item) => {
      const sev = String(item.severity || "INFO").toUpperCase();
      const refs = Array.isArray(item.data_refs) ? item.data_refs.join(", ") : "";
      return `
        <article class="insight-card">
          <span class="sev ${sev}">${sev}</span>
          <h3>${item.title || "Insight"}</h3>
          <p>${item.explanation || ""}</p>
          <p><strong>Action:</strong> ${item.recommendation || ""}</p>
          <p><strong>Confidence:</strong> ${formatPercent(item.confidence || 0)}</p>
          ${refs ? `<p><strong>Data refs:</strong> ${refs}</p>` : ""}
        </article>
      `;
    })
    .join("");
}

function toFiniteNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function sanitizeXY(xValues, yValues, maxPoints = 2500) {
  const xs = Array.isArray(xValues) ? xValues : [];
  const ys = Array.isArray(yValues) ? yValues : [];
  const len = Math.min(xs.length, ys.length);
  const cleanX = [];
  const cleanY = [];

  for (let i = 0; i < len; i += 1) {
    const x = xs[i];
    const y = toFiniteNumber(ys[i]);
    if (y === null || x === null || x === undefined || x === "" || x === "NaT") {
      continue;
    }
    cleanX.push(x);
    cleanY.push(y);
  }

  if (cleanX.length <= maxPoints) {
    return { x: cleanX, y: cleanY };
  }

  const step = Math.ceil(cleanX.length / maxPoints);
  return {
    x: cleanX.filter((_, idx) => idx % step === 0),
    y: cleanY.filter((_, idx) => idx % step === 0),
  };
}

function safePlot(targetId, traces, layout, config, fallbackText) {
  try {
    Plotly.newPlot(targetId, traces, layout, config);
  } catch (error) {
    const node = typeof targetId === "string" ? document.getElementById(targetId) : targetId;
    if (node) {
      node.innerHTML = `<p class="muted">${escapeHtml(fallbackText || "Chart rendering failed for this dataset.")}</p>`;
    }
    // eslint-disable-next-line no-console
    console.error("Plot rendering failed", error);
  }
}

function drawTextChart(targetId, title, text) {
  safePlot(
    targetId,
    [{ type: "scatter", x: [0], y: [0], mode: "text", text: [text] }],
    {
      title,
      margin: { t: 50, r: 10, l: 10, b: 20 },
      paper_bgcolor: "#fbfefb",
      plot_bgcolor: "#fbfefb",
      xaxis: { visible: false },
      yaxis: { visible: false },
    },
    { responsive: true, displaylogo: false },
    text
  );
}

function drawTrendChart(targetId, descriptive) {
  const profile = descriptive?.trend_profile || {};
  const series = profile.series || [];
  if (!profile.available || !series.length) {
    drawTextChart(targetId, "Trend over time", profile.reason || "No trend data available");
    return;
  }

  const x = series.map((r) => r.period);
  const yCount = series.map((r) => Number(r.records || 0));
  const yMetric = series.map((r) => (r.metric_mean === null || r.metric_mean === undefined ? null : Number(r.metric_mean)));
  const countSeries = sanitizeXY(x, yCount, 1500);
  const metricSeries = sanitizeXY(x, yMetric, 1500);
  const hasMetric = metricSeries.x.length > 0;

  if (!countSeries.x.length && !metricSeries.x.length) {
    drawTextChart(targetId, "Trend over time", "No valid trend points after data cleanup");
    return;
  }

  const traces = [];
  if (countSeries.x.length) {
    traces.push({
      type: "scatter",
      mode: "lines+markers",
      x: countSeries.x,
      y: countSeries.y,
      name: "record count",
      line: { color: "#0f9272", width: 2 },
    });
  }
  if (hasMetric) {
    traces.push({
      type: "scatter",
      mode: "lines",
      x: metricSeries.x,
      y: metricSeries.y,
      name: profile.metric_column || "metric mean",
      yaxis: "y2",
      line: { color: "#2456a6", width: 2, dash: "dot" },
    });
  }

  safePlot(
    targetId,
    traces,
    {
      title: `Trend by ${profile.date_column || "time"}`,
      margin: { t: 50, r: 45, l: 45, b: 70 },
      xaxis: { tickangle: -35, automargin: true },
      yaxis: { title: "records" },
      yaxis2: hasMetric ? { title: "metric mean", overlaying: "y", side: "right" } : undefined,
      paper_bgcolor: "#fbfefb",
      plot_bgcolor: "#fbfefb",
    },
    { responsive: true, displaylogo: false },
    "Trend chart could not be rendered for this dataset."
  );
}

function drawDistributionChart(targetId, descriptive) {
  const profile = descriptive?.distribution_profile || {};
  const values = (profile.values_sample || []).map((v) => toFiniteNumber(v)).filter((v) => v !== null);
  if (!profile.available || !values.length) {
    drawTextChart(targetId, "Distribution", profile.reason || "No distribution data available");
    return;
  }

  safePlot(
    targetId,
    [
      {
        type: "histogram",
        x: values,
        marker: { color: "#0f9272" },
        opacity: 0.85,
        nbinsx: 30,
      },
    ],
    {
      title: `Distribution of ${profile.metric_column}`,
      margin: { t: 50, r: 10, l: 45, b: 60 },
      xaxis: { title: profile.metric_column || "value" },
      yaxis: { title: "frequency" },
      paper_bgcolor: "#fbfefb",
      plot_bgcolor: "#fbfefb",
      annotations: profile.summary && profile.summary.median !== undefined ? [
        {
          x: toFiniteNumber(profile.summary.median),
          y: 1,
          xref: "x",
          yref: "paper",
          text: "median",
          showarrow: true,
          arrowhead: 2,
        },
      ] : [],
    },
    { responsive: true, displaylogo: false },
    "Distribution chart could not be rendered for this dataset."
  );
}

function drawParetoChart(targetId, descriptive) {
  const profile = descriptive?.segment_pareto || {};
  const rows = profile.rows || [];
  if (!profile.available || !rows.length) {
    drawTextChart(targetId, "Segment Pareto", profile.reason || "No pareto segmentation available");
    return;
  }

  const clean = rows
    .map((r) => ({
      segment: r.segment,
      value: toFiniteNumber(r.value),
      cumulative: toFiniteNumber(r.cumulative_pct),
    }))
    .filter((r) => r.segment !== null && r.segment !== undefined && r.segment !== "" && r.value !== null && r.cumulative !== null);

  if (!clean.length) {
    drawTextChart(targetId, "Segment Pareto", "No valid pareto points after data cleanup");
    return;
  }

  const x = clean.map((r) => r.segment);
  const y = clean.map((r) => r.value);
  const yCum = clean.map((r) => r.cumulative * 100);

  safePlot(
    targetId,
    [
      { type: "bar", x, y, name: "segment value", marker: { color: "#8a59c2" } },
      { type: "scatter", mode: "lines+markers", x, y: yCum, name: "cumulative %", yaxis: "y2", line: { color: "#d26a1d" } },
    ],
    {
      title: `Top segments by ${profile.metric_column || "metric"}`,
      margin: { t: 50, r: 45, l: 45, b: 100 },
      xaxis: { tickangle: -35, automargin: true, title: profile.segment_column || "segment" },
      yaxis: { title: "value" },
      yaxis2: { title: "cumulative %", overlaying: "y", side: "right", range: [0, 110] },
      paper_bgcolor: "#fbfefb",
      plot_bgcolor: "#fbfefb",
      annotations: [{ x: x[Math.min(2, x.length - 1)], y: 80, yref: "y2", text: "80% focus zone", showarrow: false }],
    },
    { responsive: true, displaylogo: false },
    "Pareto chart could not be rendered for this dataset."
  );
}

function drawDriverScatterChart(targetId, diagnostic) {
  const profile = diagnostic?.driver_scatter || {};
  const points = profile.points || [];
  if (!profile.available || !points.length) {
    drawTextChart(targetId, "Driver relationship", profile.reason || "No driver scatter available");
    return;
  }

  const pairs = sanitizeXY(
    points.map((p) => p.x),
    points.map((p) => p.y),
    2500
  );
  if (!pairs.x.length) {
    drawTextChart(targetId, "Driver relationship", "No valid scatter points after data cleanup");
    return;
  }

  safePlot(
    targetId,
    [
      {
        type: "scattergl",
        mode: "markers",
        x: pairs.x,
        y: pairs.y,
        marker: { color: "#0f9272", size: 5, opacity: 0.45 },
      },
    ],
    {
      title: `${profile.x_col} vs ${profile.y_col} (corr ${Number(profile.corr || 0).toFixed(2)})`,
      margin: { t: 50, r: 10, l: 55, b: 55 },
      xaxis: { title: profile.x_col || "x" },
      yaxis: { title: profile.y_col || "y" },
      paper_bgcolor: "#fbfefb",
      plot_bgcolor: "#fbfefb",
    },
    { responsive: true, displaylogo: false },
    "Driver scatter chart could not be rendered for this dataset."
  );
}

function drawQualityChart(targetId, descriptive, diagnostic) {
  const missing = (diagnostic?.missingness || []).slice(0, 6);
  const outliers = (diagnostic?.outlier_scan || []).slice(0, 6);
  const duplicatePct = Number(descriptive?.overview?.duplicate_pct || 0) * 100;

  const categories = [
    ...missing.map((x) => `missing:${x.column}`),
    ...outliers.map((x) => `outlier:${x.column}`),
    "duplicate_rows",
  ];
  const values = [
    ...missing.map((x) => Number(x.missing_pct || 0) * 100),
    ...outliers.map((x) => Number(x.outlier_pct || 0) * 100),
    duplicatePct,
  ];
  const colors = [
    ...missing.map(() => "#c57618"),
    ...outliers.map(() => "#a5302b"),
    "#2456a6",
  ];

  const rows = categories
    .map((label, idx) => ({ label, value: toFiniteNumber(values[idx]), color: colors[idx] }))
    .filter((r) => r.label && r.value !== null);

  if (!rows.length) {
    drawTextChart(targetId, "Data quality pressure map", "No valid quality data available");
    return;
  }

  safePlot(
    targetId,
    [{ type: "bar", x: rows.map((r) => r.label), y: rows.map((r) => r.value), marker: { color: rows.map((r) => r.color) } }],
    {
      title: "Data quality pressure map",
      margin: { t: 50, r: 10, l: 45, b: 110 },
      xaxis: { tickangle: -35, automargin: true },
      yaxis: { title: "% impact" },
      paper_bgcolor: "#fbfefb",
      plot_bgcolor: "#fbfefb",
    },
    { responsive: true, displaylogo: false },
    "Quality chart could not be rendered for this dataset."
  );
}

function drawCorrelationHeatmapChart(targetId, diagnostic) {
  const heat = diagnostic?.correlation_heatmap || {};
  const cols = heat.columns || [];
  const matrix = heat.matrix || [];
  if (!cols.length || !matrix.length) {
    drawTextChart(targetId, "Correlation map", "Not enough numeric columns");
    return;
  }

  const cleanMatrix = matrix
    .filter((row) => Array.isArray(row) && row.length === cols.length)
    .map((row) => row.map((v) => toFiniteNumber(v)));
  const hasValue = cleanMatrix.some((row) => row.some((v) => v !== null));
  if (!hasValue) {
    drawTextChart(targetId, "Correlation map", "Correlation matrix has no valid values");
    return;
  }

  safePlot(
    targetId,
    [{ type: "heatmap", z: cleanMatrix, x: cols, y: cols, colorscale: "RdBu", zmin: -1, zmax: 1 }],
    {
      title: "Correlation structure",
      margin: { t: 50, r: 10, l: 70, b: 100 },
      xaxis: { tickangle: -40, automargin: true },
      yaxis: { automargin: true },
      paper_bgcolor: "#fbfefb",
      plot_bgcolor: "#fbfefb",
    },
    { responsive: true, displaylogo: false },
    "Correlation chart could not be rendered for this dataset."
  );
}

function drawCategoricalCompositionChart(targetId, descriptive) {
  const profile = descriptive?.categorical_profile || [];
  if (!profile.length) {
    drawTextChart(targetId, "Categorical composition", "No categorical profile available");
    return;
  }

  const selected = profile[0];
  const rows = (selected.top_values || [])
    .map((r) => ({ value: r.value, count: toFiniteNumber(r.count) }))
    .filter((r) => r.value !== undefined && r.value !== null && r.value !== "" && r.count !== null);

  if (!rows.length) {
    drawTextChart(targetId, "Categorical composition", "No valid category counts available");
    return;
  }

  safePlot(
    targetId,
    [
      {
        type: "bar",
        x: rows.map((r) => r.value),
        y: rows.map((r) => r.count),
        marker: { color: "#2a6f9b" },
      },
    ],
    {
      title: `Top categories in ${selected.column}`,
      margin: { t: 50, r: 10, l: 45, b: 90 },
      xaxis: { tickangle: -35, automargin: true },
      yaxis: { title: "count" },
      paper_bgcolor: "#fbfefb",
      plot_bgcolor: "#fbfefb",
    },
    { responsive: true, displaylogo: false },
    "Category composition chart could not be rendered for this dataset."
  );
}

function drawOutlierChart(targetId, diagnostic) {
  const data = (diagnostic?.outlier_scan || []).slice(0, 10);
  if (!data.length) {
    drawTextChart(targetId, "Outlier pressure", "No outlier scan available");
    return;
  }

  const rows = data
    .map((d) => ({ column: d.column, value: toFiniteNumber(d.outlier_pct) }))
    .filter((d) => d.column && d.value !== null);

  if (!rows.length) {
    drawTextChart(targetId, "Outlier pressure", "No valid outlier values available");
    return;
  }

  safePlot(
    targetId,
    [{ type: "bar", x: rows.map((d) => d.column), y: rows.map((d) => d.value * 100), marker: { color: "#a5302b" } }],
    {
      title: "Outlier pressure by metric",
      margin: { t: 50, r: 10, l: 45, b: 90 },
      yaxis: { title: "% outliers" },
      xaxis: { tickangle: -35, automargin: true },
      paper_bgcolor: "#fbfefb",
      plot_bgcolor: "#fbfefb",
    },
    { responsive: true, displaylogo: false },
    "Outlier chart could not be rendered for this dataset."
  );
}

function drawMissingnessChart(targetId, diagnostic) {
  const data = (diagnostic?.missingness || []).slice(0, 10);
  if (!data.length) {
    drawTextChart(targetId, "Missingness", "No missingness profile available");
    return;
  }

  const rows = data
    .map((d) => ({ column: d.column, value: toFiniteNumber(d.missing_pct) }))
    .filter((d) => d.column && d.value !== null);

  if (!rows.length) {
    drawTextChart(targetId, "Missingness", "No valid missingness values available");
    return;
  }

  safePlot(
    targetId,
    [{ type: "bar", x: rows.map((d) => d.column), y: rows.map((d) => d.value * 100), marker: { color: "#c57618" } }],
    {
      title: "Missingness by column",
      margin: { t: 50, r: 10, l: 45, b: 90 },
      yaxis: { title: "% missing" },
      xaxis: { tickangle: -35, automargin: true },
      paper_bgcolor: "#fbfefb",
      plot_bgcolor: "#fbfefb",
    },
    { responsive: true, displaylogo: false },
    "Missingness chart could not be rendered for this dataset."
  );
}

function getTop5ChartSpecs(analysis) {
  const descriptive = analysis?.descriptive || {};
  const diagnostic = analysis?.diagnostic || {};

  const candidates = [
    {
      key: "trend",
      family: "trend",
      score: descriptive?.trend_profile?.available ? 100 : 0,
      available: Boolean(descriptive?.trend_profile?.available),
      title: "Trend Story",
      subtitle: "How volume or metric changes over time",
      render: (id) => drawTrendChart(id, descriptive),
    },
    {
      key: "distribution",
      family: "distribution",
      score: descriptive?.distribution_profile?.available ? 96 : 0,
      available: Boolean(descriptive?.distribution_profile?.available),
      title: "Distribution Story",
      subtitle: "Spread and central tendency of key metric",
      render: (id) => drawDistributionChart(id, descriptive),
    },
    {
      key: "pareto",
      family: "segment",
      score: descriptive?.segment_pareto?.available ? 94 : 0,
      available: Boolean(descriptive?.segment_pareto?.available),
      title: "Segment Pareto Story",
      subtitle: "Top segments that drive most of the metric",
      render: (id) => drawParetoChart(id, descriptive),
    },
    {
      key: "scatter",
      family: "relationship",
      score: diagnostic?.driver_scatter?.available ? 93 : 0,
      available: Boolean(diagnostic?.driver_scatter?.available),
      title: "Driver Relationship Story",
      subtitle: "Strongest variable relationship",
      render: (id) => drawDriverScatterChart(id, diagnostic),
    },
    {
      key: "quality",
      family: "quality",
      score: 92,
      available: true,
      title: "Data Quality Story",
      subtitle: "Missingness, outliers, and duplicate pressure",
      render: (id) => drawQualityChart(id, descriptive, diagnostic),
    },
    {
      key: "corr_heat",
      family: "relationship2",
      score: (diagnostic?.correlation_heatmap?.columns || []).length > 1 ? 88 : 0,
      available: (diagnostic?.correlation_heatmap?.columns || []).length > 1,
      title: "Correlation Map",
      subtitle: "Strength map of numeric relationships",
      render: (id) => drawCorrelationHeatmapChart(id, diagnostic),
    },
    {
      key: "categorical",
      family: "composition",
      score: (descriptive?.categorical_profile || []).length ? 84 : 0,
      available: (descriptive?.categorical_profile || []).length > 0,
      title: "Category Composition Story",
      subtitle: "Top values in most informative category",
      render: (id) => drawCategoricalCompositionChart(id, descriptive),
    },
    {
      key: "missing",
      family: "quality2",
      score: (diagnostic?.missingness || []).length ? 82 : 0,
      available: (diagnostic?.missingness || []).length > 0,
      title: "Missingness Story",
      subtitle: "Columns with highest missing rates",
      render: (id) => drawMissingnessChart(id, diagnostic),
    },
    {
      key: "outlier",
      family: "quality3",
      score: (diagnostic?.outlier_scan || []).length ? 81 : 0,
      available: (diagnostic?.outlier_scan || []).length > 0,
      title: "Outlier Story",
      subtitle: "Metrics with highest anomaly pressure",
      render: (id) => drawOutlierChart(id, diagnostic),
    },
  ];

  const available = candidates.filter((c) => c.available);
  const bestByFamily = new Map();
  for (const c of available) {
    const current = bestByFamily.get(c.family);
    if (!current || c.score > current.score) {
      bestByFamily.set(c.family, c);
    }
  }

  const uniqueFamily = Array.from(bestByFamily.values()).sort((a, b) => b.score - a.score);
  const selected = uniqueFamily.slice(0, 5);
  if (selected.length < 5) {
    for (const c of available.sort((a, b) => b.score - a.score)) {
      if (selected.find((x) => x.key === c.key)) {
        continue;
      }
      selected.push(c);
      if (selected.length >= 5) {
        break;
      }
    }
  }
  return selected;
}

function renderDynamicCharts(analysis) {
  if (!dynamicCharts) {
    return;
  }

  dynamicCharts.innerHTML = "";
  chartIds = [];

  const specs = getTop5ChartSpecs(analysis);
  if (!specs.length) {
    dynamicCharts.innerHTML = "<p>No chartable outputs for this dataset.</p>";
    return;
  }

  specs.forEach((spec, idx) => {
    const id = `chart-${idx}`;
    chartIds.push(id);
    const subtitle = spec.subtitle ? `<p class=\"muted\">${spec.subtitle}</p>` : "";
    dynamicCharts.insertAdjacentHTML(
      "beforeend",
      `<div class=\"chart-wrap\"><h3>${spec.title}</h3>${subtitle}<div id=\"${id}\" class=\"chart\"></div></div>`
    );
    try {
      spec.render(id);
    } catch (error) {
      drawTextChart(id, spec.title, "Chart rendering failed for this view");
      // eslint-disable-next-line no-console
      console.error(`Failed rendering chart ${spec.key}`, error);
    }
  });
}

function renderAll(payload) {
  domainValue.textContent = String(payload.domain || "unknown").toUpperCase();
  confidenceValue.textContent = formatPercent(payload.confidence || 0);
  rowsValue.textContent = String(payload.rows || 0);
  insightCountValue.textContent = String((payload.insights || []).length);

  renderStoryline(payload.executive_storyline || []);
  renderPipelineLogs(payload.pipeline_logs || []);
  renderInsights(payload.insights || []);
  renderDynamicCharts(payload.analysis || {});
  hasAnalysisContext = true;
  setChatStatus("ok", "Chat is active. Ask about insights, proof, charts, or simulations.");
  if (chatMessages && !chatMessages.children.length) {
    appendChatMessage("assistant", "Analysis context loaded. Ask me what changed, why, or what to do next.");
  }

  resultArea.classList.remove("hidden");
  requestAnimationFrame(() => {
    chartIds.forEach((id) => {
      const el = document.getElementById(id);
      if (el) {
        Plotly.Plots.resize(el);
      }
    });
  });
}

async function runFileAnalysis() {
  if (!fileInput.files.length) {
    setStatus("error", "Please choose a file first");
    return;
  }
  const formData = new FormData();
  formData.append("file", fileInput.files[0]);
  formData.append("user_preferences", JSON.stringify(getUserPreferences()));

  runButton.disabled = true;
  setStatus("loading", "Agents are analyzing your full dataset...");
  try {
    const response = await fetch("/run-domain-pipeline", { method: "POST", body: formData });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.detail || "Unknown backend error");
    }
    renderAll(payload);
    setStatus("ok", "Analysis complete. Storyline, logs, and charts updated.");
  } catch (error) {
    setStatus("error", `Analysis failed: ${error.message}`);
  } finally {
    runButton.disabled = false;
  }
}

async function runDbAnalysis() {
  const dbUrl = document.getElementById("dbUrl")?.value?.trim();
  const dbQuery = document.getElementById("dbQuery")?.value?.trim();
  const dbTable = document.getElementById("dbTable")?.value?.trim();
  const rowLimit = Number(document.getElementById("dbLimit")?.value || 200000);

  if (!dbUrl) {
    setStatus("error", "Database URL is required");
    return;
  }
  if ((dbQuery && dbTable) || (!dbQuery && !dbTable)) {
    setStatus("error", "Provide either SQL query or table name");
    return;
  }

  const payload = {
    database_url: dbUrl,
    query: dbQuery || null,
    table_name: dbTable || null,
    row_limit: rowLimit,
    user_preferences: getUserPreferences(),
  };

  runDbButton.disabled = true;
  setStatus("loading", "Running autonomous database analysis...");
  try {
    const response = await fetch("/run-domain-pipeline-db", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const out = await response.json();
    if (!response.ok) {
      throw new Error(out.detail || "Unknown backend error");
    }
    renderAll(out);
    setStatus("ok", "Database analysis complete.");
  } catch (error) {
    setStatus("error", `Database analysis failed: ${error.message}`);
  } finally {
    runDbButton.disabled = false;
  }
}

tabFile?.addEventListener("click", () => activateTab("file"));
tabDb?.addEventListener("click", () => activateTab("db"));
runButton?.addEventListener("click", runFileAnalysis);
runDbButton?.addEventListener("click", runDbAnalysis);
chatSendButton?.addEventListener("click", sendChatMessage);
chatInput?.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    sendChatMessage();
  }
});

window.addEventListener("resize", () => {
  chartIds.forEach((id) => {
    const el = document.getElementById(id);
    if (el) {
      Plotly.Plots.resize(el);
    }
  });

  if (chatChart && !chatChart.classList.contains("hidden") && chatChart.data) {
    Plotly.Plots.resize(chatChart);
  }
});
