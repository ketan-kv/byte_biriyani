# AMDAIS Codebase Documentation

Autonomous Multi-Domain Analytics and Insight System (AMDAIS).

This document is a **code-oriented** guide to the repository: architecture, runtime flows, module responsibilities, and the main functions/classes you’ll touch when extending the system.

---

## 1) What this system does

AMDAIS ingests mixed data (CSV/logs/PDF text/sensor streams), structures it into normalized records, persists it (SQLite + Parquet), runs analytics (descriptive/diagnostic/predictive), then generates actionable insights. It exposes results via:

- **FastAPI** service (JSON endpoints)
- **Streamlit** dashboard (operations console)
- A small **static webapp** served by FastAPI (optional) and an advanced standalone **React UI** (`ui/`).

---

## 2) Repository layout

Top-level packages:

- `main.py` — runtime bootstrap (DB init, orchestrator, scheduler, file watcher, FastAPI)
- `agents/` — “agent” layer that orchestrates pipelines, analytics, and insight generation
- `pipelines/` — ingestion and structuring workflows
- `analytics/` — descriptive/diagnostic/predictive computations and insight fusion
- `storage/` — persistence (SQLite schema + helpers, Parquet store)
- `api/` — FastAPI app factory, routes, request schemas
- `frontend/` — Streamlit dashboard + reusable components
- `utils/` — config, logging, parsing, file watcher helpers
- `tests/` — unit/integration tests
- `webapp/` — static HTML/CSS/JS pages served by FastAPI
- `ui/` — modern React frontend utilizing Vite to connect with FastAPI and Ollama LLM.

---

## 3) How the runtime starts

### 3.1 Entry point: `main.py`

Key functions:

- `build_runtime() -> (config, orchestrator, scheduler, observer)`
  - Loads config via `utils.config_loader.load_config()`
  - Ensures SQLite path exists, initializes DB tables via `storage.db.init_db()`
  - Creates `agents.orchestrator_agent.OrchestratorAgent`
  - Starts an APScheduler background scheduler:
    - Cron job(s) to run the full pipeline at configured hours
    - Interval job to poll for sensor anomaly flags
  - Starts watchdog file watcher via `utils.file_watcher.start_file_watcher()`

- `run()`
  - Builds runtime and creates FastAPI app via `api.main.create_app()`
  - Runs Uvicorn on `127.0.0.1:8000`
  - On shutdown: stops file watcher + scheduler

### 3.2 App factory: `api/main.py`

- `create_app(orchestrator, config) -> FastAPI`
  - Sets `app.state.orchestrator` and `app.state.config`
  - Adds routes from `api/routes/*`
  - Serves static pages from `webapp/` when present
  - Health endpoint: `GET /health`

---

## 4) Configuration

### 4.1 YAML config

- Loaded from `config/config.yaml` (default path in `utils/config_loader.py`)
- Consumed mainly by `main.py` and `OrchestratorAgent`

Typical config keys used in code:

- `paths.sqlite_path` — SQLite DB file (default `data/structured/mineral_db.sqlite`)
- `paths.sensor_parquet_path` — sensor Parquet file (default `data/structured/sensor_data.parquet`)
- `paths.insights_path` — insights JSON file (default `data/insights/latest_insights.json`)
- `paths.<domain>_raw` — raw drop folders watched by watchdog
- `scheduler.run_hours` — cron hours for pipeline runs (default `[6, 18]`)
- `scheduler.anomaly_poll_seconds` — anomaly poll frequency (default `30`)

### 4.2 Environment variables (LLM)

Several agents use `ollama` for local LLM calls:

- `OLLAMA_MODEL` (default `mistral:latest`)
- `OLLAMA_HOST` (optional)
- `OLLAMA_API_KEY` (optional; used as `Authorization: Bearer ...`)

Used in:

- `agents/insight_agent.py`
- `agents/intent_agent.py`
- `agents/research_agent.py`

If Ollama isn’t reachable or JSON parsing fails, the code falls back to deterministic/heuristic outputs.

---

## 5) End-to-end data flow

There are two main flows:

### 5.1 “Raw file ingestion” flow

1. A file is dropped into a watched folder OR submitted via `POST /ingest`
2. `OrchestratorAgent.on_new_file(filepath, file_type)`
3. Ingestion: `pipelines.ingestion.ingestion_router.ingest()`
4. Structuring: `StructuringAgent.run(raw_data, file_type)`
5. Persistence:
   - DB tables via `storage.db.insert_many()`
   - Sensor time series via `storage.parquet_store.append_sensor_data()`
6. Analytics scheduled or triggered via `OrchestratorAgent.run_pipeline()`
7. Insight generation:
   - Fusion insights via `InsightAgent.generate()` (rule-based)
   - Urgent anomaly insights via `InsightAgent.generate_urgent()`
8. Insights are persisted as JSON (`data/insights/latest_insights.json` by default)

### 5.2 “Domain-adaptive upload” flow (any CSV/Excel)

1. Streamlit uploads file → FastAPI: `POST /run-domain-pipeline`
2. FastAPI parses file into `pandas.DataFrame`
3. `OrchestratorAgent.run_domain_pipeline(df, user_preferences)`
4. Domain detection via `IntentAgent.detect_with_confidence()` (LLM)
5. Domain research via `ResearchAgent.research()` (cache → file → LLM)
6. Uploaded-dataset analytics via `AnalysisAgent.run_uploaded_dataset_analysis()`
7. LLM insights via `InsightAgent.generate_with_llm()` + storyline via `build_executive_storyline()`
8. Insights are saved to the same insights JSON file

---

## 6) Agents layer (`agents/`)

### 6.1 `OrchestratorAgent` (`agents/orchestrator_agent.py`)

Role: **central coordinator**.

Core methods:

- `on_new_file(filepath, file_type) -> dict`
  - Ingests + structures + stores, and enqueues a `new_data` event

- `run_pipeline() -> dict`
  - Runs DB/sensor analytics (`AnalysisAgent.run_all()`)
  - Generates fused insights (`InsightAgent.generate()`)
  - Writes insights to disk

- `run_domain_pipeline(df, user_preferences=None) -> dict`
  - Domain detection + research + analysis profiling + LLM insight generation
  - Returns a response containing `analysis`, `insights`, `pipeline_logs`, and metadata

- `watch_sensor_anomaly() -> list[dict]`
  - Polls `AnalysisAgent.check_anomaly_flag()`
  - Creates CRITICAL urgent insights when needed

Persistence helper:

- `_store_structured(payload)` writes to DB and/or Parquet depending on structured output.

### 6.2 `StructuringAgent` (`agents/structuring_agent.py`)

Role: convert raw payloads into normalized records.

- Geological reports (`geological_report`)
  - Cleans text, extracts minerals/depths/grades/zones/dates via `RuleEngine`
  - Extracts entities via spaCy (`pipelines.structuring.nlp_pipeline`)
  - Normalizes record via `pipelines.structuring.normalizer.normalize_geological_record`
  - Falls back to `LLMParser.parse_geo()` if rule-based extraction yields nothing

- Sensor CSV (`sensor_csv`)
  - Cleans/resamples via `pipelines.ingestion.sensor_stream.process_sensor_batch`
  - Adds anomalies via `zscore_anomalies` (ensures `is_anomaly` exists)

- Incident reports (`incident_report`)
  - Extracts fields via regex + `RuleEngine` (equipment/zone)
  - Root-cause fallback via `LLMParser.infer_root_cause()`
  - Normalizes via `normalize_incident_record`

- Production logs (`production_log`)
  - Normalizes date
  - Computes `efficiency_pct` when possible

### 6.3 `AnalysisAgent` (`agents/analysis_agent.py`)

Role: execute analytics bundles.

Key methods:

- `run_all() -> dict`
  - Opens SQLite connection
  - Calls:
    - `descriptive_analytics(conn)`
    - `diagnostic_analytics(conn)`
    - `predictive_analytics(conn)`

- `run_uploaded_dataset_analysis(df, user_preferences=None) -> dict`
  - Generic profiling for an arbitrary dataset:
    - detects datetime-like columns
    - numeric + categorical profiling
    - missingness and outlier scan
    - correlation scan + small heatmap
    - lightweight trend + distribution + segmentation heuristics
  - Supports `missing_strategy` (`none|mean|median|zero|drop`) controlled by user preferences

Note: `OrchestratorAgent` also calls anomaly-flag related methods on `AnalysisAgent` (`update_anomaly_flag`, `check_anomaly_flag`) which are defined later in the file.

### 6.4 `InsightAgent` (`agents/insight_agent.py`)

Role: convert analysis bundles into insight objects.

- `generate(analysis) -> list[dict]`
  - Uses deterministic fusion in `analytics.insight_fuser.fuse_signals()`

- `generate_with_llm(analysis, vocabulary, domain) -> list[dict]`
  - Calls Ollama with a strict “JSON-only” prompt
  - Normalizes results into insight objects (adds UUIDs)
  - Falls back to `_fallback_uploaded_insights()` when LLM fails

- `build_executive_storyline(analysis, insights, domain) -> list[dict]`
  - Builds a narrative list (context → process → key relationships → risks)

- `generate_urgent(anomaly_flag) -> list[dict]`
  - Produces CRITICAL alerts when anomaly monitor is active

### 6.5 `IntentAgent` (`agents/intent_agent.py`)

Role: infer dataset domain from columns + sample rows.

- `detect(df) -> str`
- `detect_with_confidence(df) -> (domain, confidence)`

Domains allowed by prompt: `mining, healthcare, ecommerce, finance, manufacturing, logistics, agriculture, energy, unknown`.

### 6.6 `ResearchAgent` (`agents/research_agent.py`)

Role: obtain domain knowledge (KPIs, thresholds, vocabulary).

Lookup layers:

1. In-memory cache (`self._cache`)
2. Local file `knowledge/{domain}.json` (if present)
3. LLM self-research via Ollama

Convenience methods:

- `get_kpis(domain, columns)`
- `get_thresholds(domain, columns)`
- `get_vocabulary(domain, columns)`

---

## 7) Pipelines (`pipelines/`)

### 7.1 Ingestion (`pipelines/ingestion/`)

- `ingestion_router.py`
  - `detect_file_type(path)` maps file to one of:
    - `geological_report`, `sensor_csv`, `production_log`, `incident_report`, or `unknown`
  - `ingest(filepath, file_type=None)` delegates to parsers:
    - `pdf_extractor.extract_pdf()` for PDFs
    - `csv_parser.parse_sensor_csv()` / `parse_production_csv()` / `parse_generic_csv()`
    - `log_parser.parse_log()` for incident-style text logs

- `sensor_stream.py`
  - `process_sensor_batch(df)` resamples to 1-minute resolution per `(equipment_id, sensor_type)`
  - `extract_sensor_features(df, window="1h")` computes rolling stats
  - `zscore_anomalies(df, threshold=3.0)` adds `z_score` + `is_anomaly`
  - `isolation_forest_anomalies(df)` adds `anomaly_if` (optional)

### 7.2 Structuring (`pipelines/structuring/`)

- `rule_engine.py` (`RuleEngine`)
  - Regex + keyword extractors for minerals, depths, grades, dates, zones, equipment IDs

- `nlp_pipeline.py`
  - `load_nlp()` loads `en_core_web_sm` or falls back to a blank spaCy model
  - Adds an `entity_ruler` with simple mineral/zone/equipment patterns
  - `extract_entities(nlp, text)` returns `(entity_text, label)` pairs

- `llm_parser.py` (`LLMParser`)
  - Deterministic fallback parser for geological reports
  - Simple heuristic `infer_root_cause()` for incident text

- `normalizer.py`
  - `normalize_geological_record()` standardizes dates and units
  - `normalize_incident_record()` standardizes date and severity casing

---

## 8) Analytics (`analytics/`)

### 8.1 Descriptive (`analytics/descriptive.py`)

- `production_trend(conn) -> dict`
  - Daily yield, yield by mineral, efficiency trend, downtime, and top zones

- `mineral_distribution(conn) -> list[dict]`
  - Aggregates average grade + sample counts per mineral + zone

### 8.2 Diagnostic (`analytics/diagnostic.py`)

- `diagnose_efficiency_drop(conn, sensor_parquet_path, threshold_pct=10.0) -> list[dict]`
  - Finds large efficiency drops by zone
  - Correlates with anomalies in sensor parquet around the drop window

- `summarize_anomalies(sensor_df) -> dict`
  - Totals and grouping of anomalies by equipment and sensor type

### 8.3 Predictive (`analytics/predictive.py`)

- `_heuristic_failure_risk(sensor_df) -> dict`
  - Uses recent anomalies to estimate short-term failure probability per equipment

- `forecast_yield(conn, periods=7) -> list[dict]`
  - Robust fallback forecast based on trailing mean

- `predictive_bundle(conn, sensor_parquet_path) -> dict`
  - Returns `{failure_risk, yield_forecast}`

### 8.4 Insight fusion (`analytics/insight_fuser.py`)

- `fuse_signals(descriptive, diagnostic, predictive) -> list[dict]`
  - Creates `Insight` objects (dataclass) and sorts by severity
  - Produces CRITICAL equipment risk insights when failure risk + anomaly count exceed thresholds

---

## 9) Persistence (`storage/`)

### 9.1 SQLite DB (`storage/db.py`)

- `init_db(db_path)` runs schema migrations from `storage/migrations/001_init_tables.sql`
- `insert_many(table_name, records, db_path)` bulk inserts records

SQLite is used for:

- Geological records
- Extraction (production) logs
- Incident reports

### 9.2 Parquet store (`storage/parquet_store.py`)

- `append_sensor_data(df, parquet_path)` appends sensor rows (read/concat/write)
- `read_sensor_data(parquet_path)` reads sensor history

Parquet is used for sensor time-series (efficient reads for analytics + dashboard).

---

## 10) API layer (`api/`)

### 10.1 Schemas (`api/schemas.py`)

- `IngestRequest`: `{path: str, file_type?: str}`
- `RunPipelineResponse`: `{insights_generated: int, status: str}`
- `DatabaseDomainPipelineRequest`:
  - requires exactly one of `query` or `table_name`
  - `row_limit` bounded `100..1_000_000`

### 10.2 Routes (`api/routes/`)

- `GET /health` — system health check

- `POST /ingest` (tags: `ingestion`)
  - Body: `IngestRequest`
  - Verifies local file exists, then calls `orchestrator.on_new_file()`

- `POST /run-pipeline` (tags: `pipeline`)
  - Runs `orchestrator.run_pipeline()`

- `GET /insights` (tags: `insights`)
  - Returns the persisted insights JSON

- `GET /analytics/{analysis_type}` (tags: `analytics`)
  - `analysis_type ∈ {descriptive, diagnostic, predictive}`

- `POST /run-domain-pipeline` (tags: `domain_pipeline`)
  - Multipart upload (CSV/Excel) + optional `user_preferences` JSON string

- `POST /run-domain-pipeline-db` (tags: `domain_pipeline`)
  - Uses SQLAlchemy to read from a database URL and then runs the same domain pipeline

---

## 11) Streamlit dashboard (`frontend/`)

### 11.1 `frontend/dashboard.py`

- Page chooser: `Overview`, `Sensor Monitor`, `Insights`, `Raw vs Structured`, `Domain Upload`
- Uses `AnalysisAgent.run_all()` to compute analytics for display
- Reads insights JSON + sensor parquet with caching (`@st.cache_data(ttl=30)`)

### 11.2 Pages

- `frontend/pages/overview.py`
  - Visualizes production trends using chart helpers

- `frontend/pages/domain_upload.py`
  - Uploads CSV/Excel and calls `POST http://127.0.0.1:8000/run-domain-pipeline`
  - Displays detected domain, KPIs, priorities, and insight cards

(Other pages follow the same pattern: a `render(...)` function building a Streamlit view.)

### 11.3 Components

- `frontend/components/charts.py`
  - Simple wrappers around Streamlit chart rendering

- `frontend/components/insight_cards.py`
  - Renders insight objects in a consistent UI card layout

---

## 12) Utilities (`utils/`)

- `config_loader.py`
  - `load_config()` reads YAML config

- `file_watcher.py`
  - Uses watchdog to detect new files and call orchestrator callback
  - Main entry: `start_file_watcher(watch_paths, callback)`

- `logger.py`
  - `get_logger(name="amdais")` returns a configured `logging.Logger`

- `date_parser.py`
  - `normalize_date(raw)` normalizes multiple date formats into a consistent string

- `text_cleaner.py`
  - `clean_text(text)` normalizes whitespace and basic text noise

- `unit_normalizer.py`
  - `standardize_unit(raw_unit)` and `normalize_units(value)`

---

## 13) Models (`models/`)

- `models/schemas.py`
  - Pydantic models describing core entities:
    - `GeologicalRecord`, `Insight`, `AnalysisBundle`

- `models/train_failure_model.py`
  - Training utilities for a failure classifier + scaler
  - `save_model()` persists a pickle

- `models/yield_forecast.py`
  - `forecast_yield_simple(prod_df, periods=7)` convenience forecast helper

---

## 14) Static webapp and React Client (`webapp/` & `ui/`)

FastAPI serves vanilla HTML/Vanilla-JS:

- `GET /` → `webapp/index.html`
- `GET /app` → `webapp/app.html`
- `GET /about` → `webapp/about.html`
- Mounted static assets under `/static`

Additionally, a modern React client is available in `ui/`. 
To serve it through FastAPI, build the UI so it generates the `webapp/react` subfolder:
```bash
cd ui
npm install
npm run build
```
Once built, it is auto-served by FastAPI under `GET /react`.

---

## 15) Testing (`tests/`)

Test entrypoints:

- `scripts/test_all.ps1` — PowerShell runner
- Direct `pytest` is also supported

Test coverage includes:

- Ingestion routing and parsing (`test_ingestion.py`)
- Structuring behaviors (`test_structuring.py`)
- Analytics agent end-to-end (`test_analytics.py`)
- FastAPI smoke flow (`test_integration.py`)

---

## 16) How to run (developer notes)

### Install

```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

### Start API + scheduler + file watcher

```bash
python main.py
```

### Start Streamlit dashboard

```bash
streamlit run frontend/dashboard.py
```

### Run tests (PowerShell)

```powershell
./scripts/test_all.ps1
```

---

## 17) Extension points (where to modify)

Common changes and where they go:

- Add a new ingestion type → `pipelines/ingestion/ingestion_router.py` + new parser module
- Add new structured DB table → `storage/migrations/001_init_tables.sql` + `StructuringAgent` mapping
- Add analytics output → implement in `analytics/` + wire into `AnalysisAgent`
- Change insight rules → `analytics/insight_fuser.py` or `InsightAgent` methods
- Add a new API endpoint → `api/routes/*.py` + include in `api/main.py`
- Add dashboard view → `frontend/pages/*.py` + wire into `frontend/dashboard.py`

---

## 18) Glossary

- **Structured DB**: SQLite tables holding normalized records
- **Sensor store**: Parquet file containing time-series sensor values and anomaly flags
- **Insight**: A normalized object `{severity, category, title, explanation, recommendation, confidence, ...}` generated by fusion rules or LLM
- **Domain pipeline**: Flow for arbitrary user datasets (domain detection + research + profiling + LLM insights)
