# AMDAIS

Autonomous Multi-Domain Analytics and Insight System.

AMDAIS is a modular analytics platform that ingests mixed data sources, structures raw records, runs analysis pipelines, and produces actionable insights through API and dashboard surfaces.

## Core Capabilities

- Multi-source ingestion (CSV, logs, PDF text, sensor streams)
- Structuring and normalization pipeline with rule and NLP components
- Agent-based orchestration for ingestion, analysis, and insight generation
- Descriptive, diagnostic, and predictive analytics bundles
- Insight ranking with severity and recommendation output
- FastAPI service endpoints for automation and integrations
- Streamlit dashboard for monitoring and exploration

## Architecture Overview

- `agents/`: orchestration and domain logic (`analysis`, `insight`, `intent`, `research`)
- `pipelines/`: ingestion and structuring workflows
- `analytics/`: analysis modules and signal fusion
- `storage/`: SQLite and Parquet persistence
- `api/`: FastAPI app, routes, and schemas
- `frontend/`: Streamlit dashboard and reusable components
- `utils/`: shared helpers (config, logging, parsing, watcher)
- `tests/`: unit and integration test coverage

## Quick Start

1. Create and activate a Python virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

3. Run the backend runtime:

```bash
python main.py
```

4. Run the dashboard (optional):

```bash
streamlit run frontend/dashboard.py
```

## API Endpoints

- `GET /health`
- `POST /ingest`
- `POST /run-pipeline`
- `GET /insights`
- `GET /analytics/{analysis_type}`

## Testing

Windows PowerShell helper:

```powershell
./scripts/test_all.ps1
```

Optional pytest-only mode:

```powershell
./scripts/test_all.ps1 -SkipApiSmoke
```

## Notes

- Set environment variables in `.env` as needed (for example API keys).
- Export/report artifacts are intentionally excluded from repository publishing.
