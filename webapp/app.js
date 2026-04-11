/* ═══════════════════════════════════════════════════════════════
   AMDAIS — Workspace JavaScript
   SSE streaming · Pipeline animation · Chat assistant · Charts
   ═══════════════════════════════════════════════════════════════ */

/* ── State ──────────────────────────────────────────────────────── */
const state = {
  file: null,
  activeStage: null,
  isRunning: false,
  lastResult: null,
  chatOpen: false,
  pipelineConfig: {
    detect:    { domain_override: '', exclude_columns: '' },
    structure: { missing_strategy: 'none', outlier_handling: 'keep' },
    analyze:   { focus_columns: '', correlation_depth: 10, outlier_threshold: 1.5 },
    insight:   { min_severity: 'INFO', insight_count: 6, insight_style: 'detailed' },
  },
};
let chartIds = [];

/* ── DOM ────────────────────────────────────────────────────────── */
const $ = (id) => document.getElementById(id);

const uploadZone    = $('uploadZone');
const fileInput     = $('fileInput');
const chooseFileBtn = $('chooseFileBtn');
const fileChip      = $('fileChip');
const fileChipName  = $('fileChipName');
const fileChipSize  = $('fileChipSize');
const fileRemoveBtn = $('fileRemoveBtn');
const runBtn        = $('runBtn');
const rerunBtn      = $('rerunBtn');
const statusBar     = $('statusBar');
const resultsArea   = $('resultsArea');
const stageWrapper  = $('stageConfigWrapper');
const nodeEls       = document.querySelectorAll('.pipeline-node');

// Config fields
const cfgDomainOverride   = $('cfg-domain-override');
const cfgExcludeCols      = $('cfg-exclude-cols');
const cfgMissingStrategy  = $('cfg-missing-strategy');
const cfgOutlierHandling  = $('cfg-outlier-handling');
const cfgFocusCols        = $('cfg-focus-cols');
const cfgCorrDepth        = $('cfg-correlation-depth');
const cfgOutlierThresh    = $('cfg-outlier-threshold');
const cfgMinSeverity      = $('cfg-min-severity');
const cfgInsightCount     = $('cfg-insight-count');
const cfgInsightStyle     = $('cfg-insight-style');

// Chat
const chatFab      = $('chatFab');
const chatFabDot   = $('chatFabDot');
const chatPanel    = $('chatPanel');
const chatCloseBtn = $('chatCloseBtn');
const chatMessages = $('chatMessages');
const chatInput    = $('chatInput');
const chatSendBtn  = $('chatSendBtn');

/* ── Utilities ──────────────────────────────────────────────────── */
function escHtml(str) {
  return String(str ?? '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
function fmtPercent(v) { return `${(Number(v || 0) * 100).toFixed(1)}%`; }
function fmtNum(v)     { return Number(v || 0).toLocaleString(); }
function fmtBytes(b)   {
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
  return (b / 1048576).toFixed(1) + ' MB';
}
function fmtLogVal(v) {
  if (v == null) return '—';
  if (typeof v === 'number') return Number.isInteger(v) ? v.toLocaleString() : v.toFixed(3);
  if (typeof v === 'boolean') return v ? 'yes' : 'no';
  if (Array.isArray(v)) return `${v.length} item(s)`;
  if (typeof v === 'object') return `${Object.keys(v).length} field(s)`;
  return String(v);
}

/* ── File Handling ──────────────────────────────────────────────── */
function setFile(file) {
  state.file = file;
  fileChipName.textContent = file.name;
  fileChipSize.textContent = fmtBytes(file.size);
  uploadZone.classList.add('has-file');
  fileChip.classList.remove('hidden');
  runBtn.disabled = false;
  setStatus('idle', 'File ready — configure stages or click Run Analysis');
  rerunBtn.classList.add('hidden');
}

function clearFile() {
  state.file = null;
  fileInput.value = '';
  uploadZone.classList.remove('has-file');
  fileChip.classList.add('hidden');
  runBtn.disabled = true;
  setStatus('idle', 'Choose a file to begin');
  resetNodes();
  resultsArea.classList.add('hidden');
  resultsArea.classList.remove('visible');
  rerunBtn.classList.add('hidden');
}

uploadZone.addEventListener('click', (e) => {
  if (fileRemoveBtn.contains(e.target)) return;
  if (!state.file) fileInput.click();
});
chooseFileBtn.addEventListener('click', (e) => { e.stopPropagation(); fileInput.click(); });
fileInput.addEventListener('change', () => { if (fileInput.files[0]) setFile(fileInput.files[0]); });
fileRemoveBtn.addEventListener('click', (e) => { e.stopPropagation(); clearFile(); });

uploadZone.addEventListener('dragover', (e) => { e.preventDefault(); uploadZone.classList.add('dragging'); });
uploadZone.addEventListener('dragleave', () => uploadZone.classList.remove('dragging'));
uploadZone.addEventListener('drop', (e) => {
  e.preventDefault(); uploadZone.classList.remove('dragging');
  const f = e.dataTransfer.files[0];
  const ext = f?.name?.split('.').pop().toLowerCase();
  if (f && ['csv', 'xlsx', 'xls'].includes(ext)) setFile(f);
  else setStatus('error', 'Only CSV or Excel files are supported.');
});

/* ── Status Bar ─────────────────────────────────────────────────── */
function setStatus(type, text) {
  statusBar.className = `status-bar ${type}`;
  if (type === 'loading') {
    statusBar.innerHTML =
      `<div class="loading-dots"><span></span><span></span><span></span></div>${escHtml(text)}`;
  } else {
    statusBar.textContent = text;
  }
}

/* ── Pipeline Node State ────────────────────────────────────────── */
const NODE_STAGES = ['detect', 'structure', 'analyze', 'insight'];
const NODE_LABELS = { idle: 'Waiting', processing: 'Processing…', done: 'Done', error: 'Error' };

function setNodeState(id, s) {
  const el = $(`node-${id}`);
  const st = $(`status-${id}`);
  if (!el) return;
  el.className = `pipeline-node ${s}`;
  if (st) st.textContent = NODE_LABELS[s] || s;
}

function setConn(id, s) {
  const el = $(`conn-${id}`);
  if (el) el.className = `pipeline-connector ${s}`;
}

function resetNodes() {
  NODE_STAGES.forEach(s => setNodeState(s, 'idle'));
  ['detect-structure', 'structure-analyze', 'analyze-insight'].forEach(c => setConn(c, ''));
}

/* ── Stage Config ────────────────────────────────────────────────── */
function syncFromControls() {
  state.pipelineConfig.detect.domain_override     = cfgDomainOverride.value;
  state.pipelineConfig.detect.exclude_columns     = cfgExcludeCols.value;
  state.pipelineConfig.structure.missing_strategy = cfgMissingStrategy.value;
  state.pipelineConfig.structure.outlier_handling = cfgOutlierHandling.value;
  state.pipelineConfig.analyze.focus_columns      = cfgFocusCols.value;
  state.pipelineConfig.analyze.correlation_depth  = cfgCorrDepth.value;
  state.pipelineConfig.analyze.outlier_threshold  = cfgOutlierThresh.value;
  state.pipelineConfig.insight.min_severity       = cfgMinSeverity.value;
  state.pipelineConfig.insight.insight_count      = cfgInsightCount.value;
  state.pipelineConfig.insight.insight_style      = cfgInsightStyle.value;
}

function syncToControls() {
  const c = state.pipelineConfig;
  cfgDomainOverride.value  = c.detect.domain_override  || '';
  cfgExcludeCols.value     = c.detect.exclude_columns  || '';
  cfgMissingStrategy.value = c.structure.missing_strategy;
  cfgOutlierHandling.value = c.structure.outlier_handling;
  cfgFocusCols.value       = c.analyze.focus_columns   || '';
  cfgCorrDepth.value       = c.analyze.correlation_depth;
  cfgOutlierThresh.value   = c.analyze.outlier_threshold;
  cfgMinSeverity.value     = c.insight.min_severity;
  cfgInsightCount.value    = c.insight.insight_count;
  cfgInsightStyle.value    = c.insight.insight_style;
}

function openStageConfig(stageId) {
  document.querySelectorAll('.stage-config').forEach(el => el.classList.add('hidden'));
  const cfgEl = $(`config-${stageId}`);
  if (!cfgEl) return;

  if (state.activeStage === stageId) {
    stageWrapper.classList.remove('open');
    state.activeStage = null;
    document.querySelectorAll('.pipeline-node').forEach(n => n.classList.remove('active'));
  } else {
    cfgEl.classList.remove('hidden');
    stageWrapper.classList.add('open');
    state.activeStage = stageId;
    document.querySelectorAll('.pipeline-node').forEach(n => n.classList.remove('active'));
    const nodeEl = $(`node-${stageId}`);
    if (nodeEl && !nodeEl.classList.contains('processing') && !nodeEl.classList.contains('done')) {
      nodeEl.classList.add('active');
    }
  }
}

nodeEls.forEach(node => {
  node.addEventListener('click', () => {
    if (state.isRunning) return;
    openStageConfig(node.dataset.stage);
  });
  node.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); node.click(); }
  });
});

[cfgDomainOverride, cfgExcludeCols, cfgMissingStrategy, cfgOutlierHandling,
 cfgFocusCols, cfgCorrDepth, cfgOutlierThresh, cfgMinSeverity, cfgInsightCount, cfgInsightStyle
].forEach(el => el?.addEventListener('change', syncFromControls));

/* ── Build user_preferences payload ─────────────────────────────── */
function buildPrefs() {
  const c = state.pipelineConfig;
  const splitCols = (s) => String(s || '').split(',').map(x => x.trim()).filter(Boolean);
  return {
    domain_override:     c.detect.domain_override || null,
    exclude_columns:     splitCols(c.detect.exclude_columns),
    missing_strategy:    c.structure.missing_strategy,
    outlier_handling:    c.structure.outlier_handling,
    focus_columns:       splitCols(c.analyze.focus_columns),
    correlation_depth:   parseInt(c.analyze.correlation_depth, 10),
    outlier_threshold:   parseFloat(c.analyze.outlier_threshold),
    min_severity:        c.insight.min_severity,
    insight_count:       parseInt(c.insight.insight_count, 10),
    insight_style:       c.insight.insight_style,
  };
}

/* ── SSE Pipeline Runner ─────────────────────────────────────────── */
async function runAnalysis() {
  if (!state.file || state.isRunning) return;

  syncFromControls();
  state.isRunning = true;
  runBtn.disabled = true;
  rerunBtn.classList.add('hidden');

  // Close stage config + reset
  stageWrapper.classList.remove('open');
  state.activeStage = null;
  document.querySelectorAll('.pipeline-node.active').forEach(n => n.classList.remove('active'));
  resetNodes();
  resultsArea.classList.add('hidden');
  resultsArea.classList.remove('visible');

  $('stageSummariesList').innerHTML = '';
  $('stageSummaries').classList.add('hidden');

  setStatus('loading', 'Starting pipeline…');

  const form = new FormData();
  form.append('file', state.file);
  form.append('user_preferences', JSON.stringify(buildPrefs()));

  try {
    const resp = await fetch('/run-domain-pipeline-stream', { method: 'POST', body: form });

    if (!resp.ok) {
      let msg = 'Upload failed';
      try { msg = (await resp.json()).detail || msg; } catch (_) {}
      throw new Error(msg);
    }

    const reader  = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const lines = buf.split('\n');
      buf = lines.pop(); // keep incomplete
      for (const line of lines) {
        if (line.startsWith('data: ')) {
          const raw = line.slice(6).trim();
          if (raw) { try { handleEvent(JSON.parse(raw)); } catch (_) {} }
        }
      }
    }
  } catch (err) {
    setStatus('error', `Pipeline error: ${err.message}`);
    NODE_STAGES.forEach(s => {
      if ($(`node-${s}`)?.classList.contains('processing')) setNodeState(s, 'error');
    });
  } finally {
    state.isRunning = false;
    runBtn.disabled = false;
  }
}

function handleEvent(ev) {
  const { stage, status, details, result, summary } = ev;
  
  if (summary && summary.length) {
    if (status === 'ok') {
      renderStageSummary(stage, summary);
    }
  }

  switch (stage) {
    case 'input':
      setNodeState('detect', 'processing');
      setStatus('loading', 'Detecting domain…');
      break;

    case 'intent_detection':
      if (status === 'error') {
        setNodeState('detect', 'error');
        setStatus('error', `Detection failed: ${details?.error || 'unknown'}`);
      } else {
        const dom  = details?.domain || 'unknown';
        const conf = ((details?.confidence || 0) * 100).toFixed(0);
        $('status-detect').textContent = `${dom} · ${conf}%`;
        setStatus('loading', `Domain: ${dom.toUpperCase()} — researching…`);
      }
      break;

    case 'domain_research':
      setNodeState('detect', 'done');
      setNodeState('structure', 'processing');
      setConn('detect-structure', 'flowing');
      setStatus('loading', 'Structuring and profiling dataset…');
      break;

    case 'analysis':
      setNodeState('structure', 'done');
      setNodeState('analyze', 'processing');
      setConn('structure-analyze', 'flowing');
      setStatus('loading', 'Running analytics engine…');
      break;

    case 'insight_generation':
      setNodeState('analyze', 'done');
      setNodeState('insight', 'processing');
      setConn('analyze-insight', 'flowing');
      setStatus('loading', 'Generating AI insights…');
      break;

    case 'storyline':
      $('status-insight').textContent = 'Building storyline…';
      break;

    case 'done':
      setNodeState('insight', 'done');
      setConn('detect-structure',  'done');
      setConn('structure-analyze', 'done');
      setConn('analyze-insight',   'done');
      if (result) {
        state.lastResult = result;
        renderResults(result);
        setStatus('ok', 'Analysis complete.');
        rerunBtn.classList.remove('hidden');
      }
      break;

    case 'error':
      setStatus('error', `Error: ${details?.error || 'unknown'}`);
      break;
  }
}

runBtn.addEventListener('click', runAnalysis);
rerunBtn.addEventListener('click', runAnalysis);

/* ── Results Rendering ───────────────────────────────────────────── */
function renderResults(payload) {
  $('metricDomain').textContent     = String(payload.domain || '—').toUpperCase();
  $('metricConfidence').textContent = fmtPercent(payload.confidence || 0);
  $('metricRows').textContent       = fmtNum(payload.rows);
  $('metricInsights').textContent   = String((payload.insights || []).length);

  renderInsights(payload.insights || []);
  renderCharts(payload.analysis || {});

  resultsArea.classList.remove('hidden');
  requestAnimationFrame(() => {
    resultsArea.classList.add('visible');
    resultsArea.scrollIntoView({ behavior: 'smooth', block: 'start' });
  });
  setTimeout(() => chartIds.forEach(id => {
    const el = document.getElementById(id);
    if (el) Plotly.Plots.resize(el);
  }), 420);
}

/* Stage Summaries (Live Feed) */
function renderStageSummary(stage, summaryStrings) {
  if (!summaryStrings || !summaryStrings.length) return;
  const list = $('stageSummariesList');
  $('stageSummaries').classList.remove('hidden');
  
  const card = document.createElement('div');
  card.className = 'stage-summary-card';
  card.innerHTML = `
    <div class="summary-stage-name">${stage} completed</div>
    <ul class="summary-bullets">
      ${summaryStrings.map(s => `<li>${escHtml(s)}</li>`).join('')}
    </ul>
  `;
  list.appendChild(card);
  card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

/* Insights */
function renderInsights(items) {
  const el = $('insightCards');
  if (!items.length) { el.innerHTML = '<p style="color:var(--text-muted)">No insights generated.</p>'; return; }
  el.innerHTML = items.map(item => {
    const sev  = escHtml((item.severity || 'INFO').toUpperCase());
    const conf = Math.round((item.confidence || 0) * 100);
    const refs = (Array.isArray(item.data_refs) ? item.data_refs : [])
      .map(r => `<span class="data-ref">${escHtml(r)}</span>`).join('');
    return `<article class="insight-card ${sev}">
      <div><span class="insight-sev ${sev}">${sev}</span></div>
      <div class="insight-title">${escHtml(item.title || 'Insight')}</div>
      <div class="insight-explanation">${escHtml(item.explanation || '')}</div>
      <div class="insight-action">
        <strong>Recommended Action</strong>${escHtml(item.recommendation || '')}
      </div>
      <div class="insight-footer">
        <div class="confidence-wrap">
          <div class="confidence-bar"><div class="confidence-fill" style="width:${conf}%"></div></div>
          <span>${conf}% confidence</span>
        </div>${refs}
      </div>
      <button class="explore-btn" onclick="openInsightDeep('${escHtml(item.id)}')" aria-label="Explore insight">Explore →</button>
    </article>`;
  }).join('');
}

async function openInsightDeep(id) {
  const insight = (state.lastResult?.insights || []).find(i => String(i.id) === String(id));
  if (!insight) return;

  const overlay = $('insightOverlay');
  const panel = $('insightDeepPanel');
  
  overlay.classList.remove('hidden');
  panel.classList.remove('closed');
  
  $('deepPanelCategory').textContent = insight.category;
  $('deepPanelTitle').textContent = insight.title;
  
  $('deepContent').classList.add('hidden');
  $('deepLoading').classList.remove('hidden');

  try {
    const resp = await fetch('/insight-deep', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        insight: insight,
        context: state.lastResult?.analysis || {},
        domain: state.lastResult?.domain || 'unknown',
      })
    });
    if (!resp.ok) throw new Error('Failed to generate deep insight');
    const res = await resp.json();
    const d = res.deep_analysis || {};

    $('deepExplanation').textContent = d.extended_explanation || insight.explanation;
    $('deepImpact').textContent = d.business_impact || 'Requires business review';
    $('deepBenchmark').textContent = d.comparable_benchmark || 'N/A';
    $('deepQuickWin').textContent = d.quick_win || 'N/A';
    
    const tags = Array.isArray(d.related_metrics) ? d.related_metrics : insight.data_refs || [];
    $('deepTags').innerHTML = tags.map(t => `<span class="data-ref">${escHtml(t)}</span>`).join('');
    
    const hyps = Array.isArray(d.root_cause_hypotheses) ? d.root_cause_hypotheses : [];
    $('deepHypotheses').innerHTML = hyps.map(h => `<li>${escHtml(h)}</li>`).join('');
    
    const steps = Array.isArray(d.investigation_steps) ? d.investigation_steps : [insight.recommendation];
    $('deepSteps').innerHTML = steps.map(s => `<li>${escHtml(s)}</li>`).join('');
    
    $('deepLoading').classList.add('hidden');
    $('deepContent').classList.remove('hidden');
  } catch (err) {
    $('deepLoading').classList.add('hidden');
    alert(err.message);
  }
}

$('deepCloseBtn').addEventListener('click', () => {
  $('insightDeepPanel').classList.add('closed');
  $('insightOverlay').classList.add('hidden');
});
$('insightOverlay').addEventListener('click', () => {
  $('insightDeepPanel').classList.add('closed');
  $('insightOverlay').classList.add('hidden');
});

/* ── Charts ──────────────────────────────────────────────────────── */
const LAYOUT_BASE = {
  paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
  font: { color: '#94a3b8', family: 'Inter, sans-serif', size: 11 },
  xaxis: { gridcolor: 'rgba(255,255,255,0.05)', zerolinecolor: 'rgba(255,255,255,0.08)', tickfont: { size: 10 } },
  yaxis: { gridcolor: 'rgba(255,255,255,0.05)', zerolinecolor: 'rgba(255,255,255,0.08)', tickfont: { size: 10 } },
  margin: { t: 38, r: 14, l: 42, b: 60 },
  legend: { bgcolor: 'rgba(0,0,0,0.4)', bordercolor: 'rgba(255,255,255,0.1)', borderwidth: 1 },
  hoverlabel: { bgcolor: '#0e1d33', bordercolor: 'rgba(255,255,255,0.12)', font: { color: '#e2e8f0' } },
};
const CFG = { responsive: true, displaylogo: false, displayModeBar: false };

function lay(extra = {}) {
  return { ...LAYOUT_BASE, ...extra,
    xaxis: { ...LAYOUT_BASE.xaxis, ...(extra.xaxis || {}) },
    yaxis: { ...LAYOUT_BASE.yaxis, ...(extra.yaxis || {}) },
  };
}

function textChart(id, title, text) {
  Plotly.newPlot(id,
    [{ type: 'scatter', x: [0], y: [0], mode: 'text',
       text: [`<span style="fill:#64748b;color:#64748b">${text}</span>`] }],
    lay({ title: { text: title, font: { size: 12, color: '#64748b' } },
          xaxis: { visible: false }, yaxis: { visible: false } }),
    CFG);
}

function drawTrend(id, d) {
  const p = d?.trend_profile || {};
  const s = p.series || [];
  if (!p.available || !s.length) { textChart(id, 'Trend', p.reason || 'No data'); return; }
  const x = s.map(r => r.period);
  const yc = s.map(r => Number(r.records || 0));
  const ym = s.map(r => r.metric_mean ?? null);
  const hasM = ym.some(v => v !== null);
  const traces = [{ type: 'scatter', mode: 'lines+markers', x, y: yc, name: 'records',
    line: { color: '#00c9a7', width: 2 }, marker: { size: 4, color: '#00c9a7' } }];
  if (hasM) traces.push({ type: 'scatter', mode: 'lines', x, y: ym,
    name: p.metric_column || 'metric', yaxis: 'y2',
    line: { color: '#8b5cf6', width: 2, dash: 'dot' } });
  Plotly.newPlot(id, traces, lay({
    title: { text: `Trend by ${p.date_column || 'time'}`, font: { size: 12, color: '#e2e8f0' } },
    xaxis: { tickangle: -30, automargin: true },
    yaxis: { title: 'records' },
    yaxis2: hasM ? { title: 'metric', overlaying: 'y', side: 'right',
      gridcolor: 'rgba(255,255,255,0.03)', tickfont: { size: 10 } } : undefined,
    margin: { t: 38, r: 52, l: 48, b: 70 },
  }), CFG);
}

function drawDist(id, d) {
  const p = d?.distribution_profile || {};
  const v = p.values_sample || [];
  if (!p.available || !v.length) { textChart(id, 'Distribution', p.reason || 'No data'); return; }
  const med = p.summary?.median;
  Plotly.newPlot(id,
    [{ type: 'histogram', x: v, nbinsx: 32,
       marker: { color: '#00c9a7', opacity: 0.75, line: { color: 'rgba(0,201,167,0.25)', width: 1 } } }],
    lay({ title: { text: `Distribution — ${p.metric_column}`, font: { size: 12, color: '#e2e8f0' } },
          xaxis: { title: p.metric_column || 'value' }, yaxis: { title: 'frequency' },
          annotations: med != null ? [{ x: Number(med), y: 1, xref: 'x', yref: 'paper', text: 'median',
            showarrow: true, arrowhead: 2, arrowcolor: '#f59e0b', font: { color: '#f59e0b', size: 10 } }] : [],
    }), CFG);
}

function drawPareto(id, d) {
  const p = d?.segment_pareto || {};
  const rows = p.rows || [];
  if (!p.available || !rows.length) { textChart(id, 'Segment Pareto', p.reason || 'No data'); return; }
  const x = rows.map(r => r.segment);
  const y = rows.map(r => Number(r.value || 0));
  const yc = rows.map(r => Number(r.cumulative_pct || 0) * 100);
  Plotly.newPlot(id,
    [{ type: 'bar', x, y, name: 'value', marker: { color: '#8b5cf6', opacity: 0.82 } },
     { type: 'scatter', mode: 'lines+markers', x, y: yc, name: 'cum %', yaxis: 'y2',
       line: { color: '#f59e0b', width: 2 }, marker: { size: 4, color: '#f59e0b' } }],
    lay({ title: { text: `Segment Pareto — ${p.metric_column || 'metric'}`, font: { size: 12, color: '#e2e8f0' } },
          xaxis: { tickangle: -35, automargin: true, title: p.segment_column || 'segment' },
          yaxis: { title: 'value' },
          yaxis2: { title: 'cumulative %', overlaying: 'y', side: 'right', range: [0, 110], tickfont: { size: 10 } },
          annotations: x.length ? [{ x: x[Math.min(2, x.length - 1)], y: 80, yref: 'y2',
            text: '80% zone', showarrow: false, font: { color: '#f59e0b', size: 9 } }] : [],
          margin: { t: 38, r: 52, l: 48, b: 90 },
    }), CFG);
}

function drawScatter(id, dx) {
  // Try using the multi-scatter we created
  const pairs = dx?.top_scatter_pairs || [];
  if (!pairs.length) {
    const p = dx?.driver_scatter || {};
    const pts = p.points || [];
    if (!p.available || !pts.length) { textChart(id, 'Driver Relationship', p.reason || 'No data'); return; }
    Plotly.newPlot(id,
      [{ type: 'scattergl', mode: 'markers', x: pts.map(p => p.x), y: pts.map(p => p.y),
         marker: { color: '#00c9a7', size: 4, opacity: 0.45 } }],
      lay({ title: { text: `${p.x_col} vs ${p.y_col} (r=${Number(p.corr || 0).toFixed(2)})`, font: { size: 12, color: '#e2e8f0' } },
            xaxis: { title: p.x_col || 'x' }, yaxis: { title: p.y_col || 'y' },
      }), CFG);
    return;
  }

  // Draw first pair
  const first = pairs[0];
  Plotly.newPlot(id,
    [{ type: 'scattergl', mode: 'markers', x: first.points.map(p => p.x), y: first.points.map(p => p.y),
        marker: { color: '#00c9a7', size: 4, opacity: 0.45 } }],
    lay({ title: { text: `${first.x_col} vs ${first.y_col} (r=${Number(first.corr || 0).toFixed(2)})`, font: { size: 12, color: '#e2e8f0' } },
          xaxis: { title: first.x_col || 'x' }, yaxis: { title: first.y_col || 'y' },
    }), CFG);
}

function drawQuality(id, d, dx) {
  const mis = (dx?.missingness || []).slice(0, 8);
  const out = (dx?.outlier_scan || []).slice(0, 6);
  const dup = Number(d?.overview?.duplicate_pct || 0) * 100;
  const cats   = [...mis.map(x => `miss: ${x.column}`), ...out.map(x => `outlier: ${x.column}`), 'duplicates'];
  const vals   = [...mis.map(x => x.missing_pct * 100), ...out.map(x => x.outlier_pct * 100), dup];
  const colors = [...mis.map(() => '#f59e0b'), ...out.map(() => '#ef4444'), '#8b5cf6'];
  Plotly.newPlot(id,
    [{ type: 'bar', x: cats, y: vals, marker: { color: colors, opacity: 0.85 } }],
    lay({ title: { text: 'Data Quality Pressure Map', font: { size: 12, color: '#e2e8f0' } },
          xaxis: { tickangle: -40, automargin: true }, yaxis: { title: '% impact' },
          margin: { t: 38, r: 14, l: 48, b: 110 },
    }), CFG);
}

function drawHeatmap(id, dx) {
  const h = dx?.correlation_heatmap || {};
  const cols = h.columns || []; const mat = h.matrix || [];
  if (!cols.length || !mat.length) { textChart(id, 'Correlation Map', 'Not enough numeric columns'); return; }
  Plotly.newPlot(id,
    [{ type: 'heatmap', z: mat, x: cols, y: cols,
       colorscale: [[0, '#ef4444'], [0.5, '#0e1d33'], [1, '#00c9a7']],
       zmin: -1, zmax: 1,
       colorbar: { tickfont: { color: '#64748b', size: 9 }, outlinecolor: 'rgba(255,255,255,0.08)' } }],
    lay({ title: { text: 'Correlation Structure', font: { size: 12, color: '#e2e8f0' } },
          xaxis: { tickangle: -40, automargin: true }, yaxis: { automargin: true },
          margin: { t: 38, r: 30, l: 70, b: 100 },
    }), CFG);
}

function drawCategory(id, d) {
  const prof = d?.categorical_profile || [];
  if (!prof.length) { textChart(id, 'Category Composition', 'No categorical data'); return; }
  const sel = prof[0]; const rows = sel.top_values || [];
  Plotly.newPlot(id,
    [{ type: 'bar', x: rows.map(r => r.value), y: rows.map(r => Number(r.count || 0)),
       marker: { color: '#38bdf8', opacity: 0.85 } }],
    lay({ title: { text: `Top values — ${sel.column}`, font: { size: 12, color: '#e2e8f0' } },
          xaxis: { tickangle: -35, automargin: true }, yaxis: { title: 'count' },
          margin: { t: 38, r: 14, l: 48, b: 90 },
    }), CFG);
}

function drawOutlier(id, dx) {
  const data = (dx?.outlier_scan || []).slice(0, 10);
  if (!data.length) { textChart(id, 'Outlier Pressure', 'No outlier data'); return; }
  Plotly.newPlot(id,
    [{ type: 'bar', x: data.map(d => d.column), y: data.map(d => d.outlier_pct * 100),
       marker: { color: '#ef4444', opacity: 0.85 } }],
    lay({ title: { text: 'Outlier Pressure by Column', font: { size: 12, color: '#e2e8f0' } },
          xaxis: { tickangle: -35, automargin: true }, yaxis: { title: '% outliers' },
          margin: { t: 38, r: 14, l: 48, b: 90 },
    }), CFG);
}

function pickCharts(analysis) {
  const d  = analysis?.descriptive || {};
  const dx = analysis?.diagnostic  || {};
  const specs = [
    { key: 'trend',    fam: 'trend',     score: d?.trend_profile?.available ? 100 : 0,
      avail: !!d?.trend_profile?.available,
      title: 'Trend Story', sub: 'How volume or metric changes over time', draw: id => drawTrend(id, d) },
    { key: 'dist',     fam: 'dist',      score: d?.distribution_profile?.available ? 96 : 0,
      avail: !!d?.distribution_profile?.available,
      title: 'Distribution Story', sub: 'Spread and shape of the key metric', draw: id => drawDist(id, d) },
    { key: 'pareto',   fam: 'segment',   score: d?.segment_pareto?.available ? 94 : 0,
      avail: !!d?.segment_pareto?.available,
      title: 'Segment Pareto', sub: 'Top segments that drive the metric', draw: id => drawPareto(id, d) },
    { key: 'scatter',  fam: 'rel',       score: dx?.top_scatter_pairs?.length ? 92 : (dx?.driver_scatter?.available ? 90 : 0),
      avail: !!dx?.top_scatter_pairs?.length || !!dx?.driver_scatter?.available,
      title: 'Driver Relationship', sub: 'Strongest variable pair', draw: id => drawScatter(id, dx) },
    { key: 'quality',  fam: 'qual',      score: 91, avail: true,
      title: 'Data Quality Map', sub: 'Missingness, outliers, and duplicates', draw: id => drawQuality(id, d, dx) },
    { key: 'heatmap',  fam: 'heat',      score: (dx?.correlation_heatmap?.columns || []).length > 1 ? 88 : 0,
      avail: (dx?.correlation_heatmap?.columns || []).length > 1,
      title: 'Correlation Map', sub: 'Full numeric correlation structure', draw: id => drawHeatmap(id, dx) },
    { key: 'category', fam: 'cat',       score: (d?.categorical_profile || []).length ? 84 : 0,
      avail: !!(d?.categorical_profile || []).length,
      title: 'Category Composition', sub: 'Top values in most informative column', draw: id => drawCategory(id, d) },
    { key: 'outlier',  fam: 'out',       score: (dx?.outlier_scan || []).length ? 80 : 0,
      avail: !!(dx?.outlier_scan || []).length,
      title: 'Outlier Pressure', sub: 'Metrics with highest anomaly rate', draw: id => drawOutlier(id, dx) },
  ];
  const avail = specs.filter(c => c.avail);
  const fams  = new Map();
  for (const c of avail) {
    if (!fams.has(c.fam) || c.score > fams.get(c.fam).score) fams.set(c.fam, c);
  }
  return Array.from(fams.values()).sort((a, b) => b.score - a.score).slice(0, 6);
}

function renderCharts(analysis) {
  const grid = $('chartGrid');
  grid.innerHTML = '';
  chartIds = [];
  const specs = pickCharts(analysis);
  if (!specs.length) { grid.innerHTML = '<p style="color:var(--text-muted)">No chartable outputs.</p>'; return; }
  specs.forEach((spec, i) => {
    const id = `chart-${i}`;
    chartIds.push(id);
    grid.insertAdjacentHTML('beforeend', `
      <div class="chart-wrap">
        <h3>${escHtml(spec.title)}</h3>
        <p class="chart-subtitle">${escHtml(spec.sub || '')}</p>
        <div id="${id}" class="chart"></div>
      </div>`);
    spec.draw(id);
  });
}

/* ── Chat ────────────────────────────────────────────────────────── */
chatFab.addEventListener('click', () => {
  state.chatOpen = !state.chatOpen;
  chatPanel.classList.toggle('open', state.chatOpen);
  if (state.chatOpen) { chatInput.focus(); chatFab.classList.remove('has-updates'); }
});
chatCloseBtn.addEventListener('click', () => {
  state.chatOpen = false; chatPanel.classList.remove('open');
});

chatInput.addEventListener('input', () => { chatSendBtn.disabled = !chatInput.value.trim(); });
chatInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter' && !e.shiftKey && !chatSendBtn.disabled) { e.preventDefault(); sendChat(); }
});
chatSendBtn.addEventListener('click', sendChat);

document.querySelectorAll('.chat-chip').forEach(chip =>
  chip.addEventListener('click', () => {
    chatInput.value = chip.dataset.msg || '';
    chatSendBtn.disabled = false;
    sendChat();
  })
);

function addMsg(role, html, meta = {}) {
  const wrap = document.createElement('div');
  wrap.className = `chat-message ${role}`;
  wrap.innerHTML = `<div class="chat-bubble">${html}</div>`;

  if (meta.stage && meta.updates && Object.keys(meta.updates).length) {
    const badge = document.createElement('div');
    badge.className = 'chat-updates-badge';
    badge.innerHTML = `✦ Updated <strong>${meta.stage}</strong> stage`;
    wrap.appendChild(badge);

    if (state.file) {
      const rb = document.createElement('button');
      rb.className = 'chat-rerun-btn'; rb.textContent = '↻ Re-run with new settings';
      rb.addEventListener('click', () => {
        chatPanel.classList.remove('open'); state.chatOpen = false; runAnalysis();
      });
      wrap.appendChild(rb);
    }
  }
  chatMessages.appendChild(wrap);
  chatMessages.scrollTop = chatMessages.scrollHeight;
}

function showTyping() {
  const d = document.createElement('div');
  d.className = 'chat-message bot'; d.id = 'chat-typing';
  d.innerHTML = '<div class="typing-bubble"><span></span><span></span><span></span></div>';
  chatMessages.appendChild(d); chatMessages.scrollTop = chatMessages.scrollHeight;
}
function removeTyping() { $('chat-typing')?.remove(); }

async function sendChat() {
  const msg = chatInput.value.trim();
  if (!msg) return;
  chatInput.value = ''; chatSendBtn.disabled = true;

  addMsg('user', escHtml(msg));
  showTyping();
  syncFromControls();

  try {
    const resp = await fetch('/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: msg,
        pipeline_state: state.pipelineConfig,
        context: state.lastResult
          ? { 
              domain: state.lastResult.domain, rows: state.lastResult.rows, 
              ...state.lastResult.analysis?.descriptive, ...state.lastResult.analysis?.diagnostic 
            } : {},
      }),
    });
    removeTyping();

    const res = await resp.json();
    const { reply, stage, updates, fallback } = res;

    if (stage && updates && Object.keys(updates).length) {
      applyChatUpdates(stage, updates);
    }
    addMsg('bot', escHtml(reply || 'Got it.'), { stage, updates });

    if (fallback) {
      addMsg('bot', '<small style="color:var(--text-dim)">⚠ Rule-based parser used (Ollama unavailable)</small>');
    }
  } catch (err) {
    removeTyping();
    addMsg('bot', `<span style="color:var(--critical)">Assistant error: ${escHtml(err.message)}</span>`);
  }
}

function applyChatUpdates(stage, updates) {
  const map = { ingest: 'detect', detect: 'detect', structure: 'structure', analyze: 'analyze', insight: 'insight' };
  const key = map[stage] || stage;
  if (!state.pipelineConfig[key]) return;

  Object.assign(state.pipelineConfig[key], updates);
  syncToControls();
  flashNode(key);
  openStageConfig(key);
  chatFab.classList.add('has-updates');
  if (state.file) rerunBtn.classList.remove('hidden');
}

function flashNode(stageId) {
  const id = stageId === 'ingest' ? 'detect' : stageId;
  const el = $(`node-${id}`);
  if (!el) return;
  el.classList.add('chat-updated');
  setTimeout(() => el.classList.remove('chat-updated'), 2200);
}

/* ── Resize Charts on Window Resize ─────────────────────────────── */
window.addEventListener('resize', () =>
  chartIds.forEach(id => { const el = document.getElementById(id); if (el) Plotly.Plots.resize(el); })
);
