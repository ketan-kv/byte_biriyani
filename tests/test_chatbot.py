from __future__ import annotations

import pandas as pd
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


def _prime_chat_context(client: TestClient) -> None:
    df = pd.DataFrame(
        [
            {
                "log_date": "2024-03-14",
                "region": "north",
                "category": "A",
                "yield_tonnes": 50.0,
                "downtime_hours": 0.4,
                "risk_score": 0.21,
            },
            {
                "log_date": "2024-03-15",
                "region": "north",
                "category": "B",
                "yield_tonnes": 43.0,
                "downtime_hours": 0.8,
                "risk_score": 0.45,
            },
            {
                "log_date": "2024-03-16",
                "region": "south",
                "category": "A",
                "yield_tonnes": 61.0,
                "downtime_hours": 0.2,
                "risk_score": 0.16,
            },
        ]
    )
    result = {
        "domain": "manufacturing",
        "analysis": {
            "descriptive": {
                "overview": {"rows": 3, "columns": 6},
                "data_prep": {"missing_strategy": "none", "numeric_missing_before": 0, "numeric_missing_after": 0},
            },
            "diagnostic": {
                "missingness": [{"column": "downtime_hours", "missing_pct": 0.0}],
                "outlier_scan": [{"column": "risk_score", "outlier_pct": 0.0}],
                "correlation_top": [{"left": "yield_tonnes", "right": "risk_score", "corr": -0.72}],
            },
            "predictive": {
                "risk_signals": ["Downtime is trending upward in north region."],
                "action_plan": ["Investigate maintenance backlog in north region."],
            },
        },
        "insights": [
            {
                "severity": "WARNING",
                "title": "Downtime pressure in north",
                "explanation": "North region has elevated downtime relative to baseline.",
                "recommendation": "Prioritize maintenance and shift balancing in north.",
                "confidence": 0.81,
                "data_refs": ["region", "downtime_hours"],
            }
        ],
    }
    client.app.state.decision_copilot.update_context(df, result)


def test_chat_data_query_response_shape() -> None:
    client = _build_client()
    _prime_chat_context(client)

    response = client.post("/chat", json={"message": "average yield_tonnes by mineral_type", "session_id": "s1"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["intent"] == "DATA_QUERY"
    assert payload["type"] == "text"
    assert isinstance(payload["data"], dict)


def test_chat_chart_request_returns_plotly_json() -> None:
    client = _build_client()
    _prime_chat_context(client)

    response = client.post("/chat", json={"message": "show trend of yield_tonnes", "session_id": "s2"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["intent"] == "CHART_REQUEST"
    if payload["type"] in {"chart", "mixed"}:
        assert isinstance(payload["chart"], dict)
        assert "data" in payload["chart"]


def test_chat_recommendations_returns_actions() -> None:
    client = _build_client()
    _prime_chat_context(client)

    response = client.post("/chat", json={"message": "what next", "session_id": "s3"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["intent"] == "RECOMMENDATION"
    assert isinstance(payload["actions"], list)
    assert len(payload["actions"]) >= 1
    assert "description" in payload["actions"][0]
    assert "reasoning" in payload["actions"][0]
    assert "expected_impact" in payload["actions"][0]


def test_chat_proof_mode_includes_evidence() -> None:
    client = _build_client()
    _prime_chat_context(client)

    response = client.post("/chat", json={"message": "prove it", "session_id": "s4"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["intent"] == "PROOF"
    assert "evidence" in payload["data"]


def test_chat_simulation_detected() -> None:
    client = _build_client()
    _prime_chat_context(client)

    response = client.post(
        "/chat",
        json={"message": "what if yield_tonnes drops by 10%", "session_id": "s5"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["intent"] == "SIMULATION"
    assert payload["type"] in {"mixed", "text"}


def test_chat_total_rows_question_returns_exact_count() -> None:
    client = _build_client()
    _prime_chat_context(client)

    response = client.post("/chat", json={"message": "how many matches are there in total?", "session_id": "s6"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["intent"] == "DATA_QUERY"
    assert payload["data"]["row_count"] == 3
    assert "insert number" not in payload["message"].lower()


def test_chat_threshold_question_returns_count_and_percentage() -> None:
    client = _build_client()
    _prime_chat_context(client)

    response = client.post(
        "/chat",
        json={"message": "in how many matches is yield_tonnes above 45?", "session_id": "s7"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["intent"] == "DATA_QUERY"
    assert payload["data"]["metric"] == "yield_tonnes"
    assert payload["data"]["match_count"] == 2
    assert "insert number" not in payload["message"].lower()