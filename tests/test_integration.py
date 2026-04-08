from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from agents.orchestrator_agent import OrchestratorAgent
from api.main import create_app
from storage.db import init_db
from utils.config_loader import load_config


def _build_client() -> TestClient:
    cfg = load_config()
    init_db(cfg["paths"]["sqlite_path"])
    app = create_app(OrchestratorAgent(cfg), cfg)
    return TestClient(app)


def test_api_smoke_flow() -> None:
    client = _build_client()

    r_health = client.get("/health")
    assert r_health.status_code == 200

    r_ingest = client.post(
        "/ingest",
        json={"path": "tests/fixtures/sample_production.csv", "file_type": "production_log"},
    )
    assert r_ingest.status_code == 200

    r_pipeline = client.post("/run-pipeline")
    assert r_pipeline.status_code == 200

    r_insights = client.get("/insights")
    assert r_insights.status_code == 200
    assert "count" in r_insights.json()

    r_analytics = client.get("/analytics/descriptive")
    assert r_analytics.status_code == 200


def test_ingest_missing_file_returns_404() -> None:
    client = _build_client()
    missing = Path("tests/fixtures/does_not_exist.csv")
    r = client.post(
        "/ingest",
        json={"path": str(missing), "file_type": "sensor_csv"},
    )
    assert r.status_code == 404
