from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from agents.analysis_agent import AnalysisAgent
from storage.db import init_db, insert_many


def _seed_db(db_path: Path) -> None:
    init_db(db_path)
    insert_many(
        "extraction_logs",
        [
            {
                "log_date": "2024-03-14",
                "shift": "day",
                "zone_id": "ZA-N",
                "equipment_id": "CRUSHER-01",
                "mineral_type": "Copper",
                "yield_tonnes": 50.0,
                "ore_processed_t": 900.0,
                "efficiency_pct": 5.5,
                "downtime_hours": 0.2,
                "operator_id": "OP-111",
                "notes": "ok",
                "source_file": "prod1.csv",
            },
            {
                "log_date": "2024-03-15",
                "shift": "day",
                "zone_id": "ZA-N",
                "equipment_id": "CRUSHER-01",
                "mineral_type": "Copper",
                "yield_tonnes": 42.0,
                "ore_processed_t": 900.0,
                "efficiency_pct": 4.6,
                "downtime_hours": 0.8,
                "operator_id": "OP-111",
                "notes": "drop",
                "source_file": "prod2.csv",
            },
        ],
        db_path=db_path,
    )


def test_analysis_agent_run_all(tmp_path: Path) -> None:
    db_path = tmp_path / "mineral_db.sqlite"
    parquet_path = tmp_path / "sensor_data.parquet"
    _seed_db(db_path)

    sensor_df = pd.DataFrame(
        [
            {
                "timestamp": "2024-03-15T08:00:00Z",
                "equipment_id": "CRUSHER-01",
                "sensor_type": "vibration",
                "value": 20.0,
                "is_anomaly": True,
                "zone_id": "ZA-N",
            }
        ]
    )
    sensor_df.to_parquet(parquet_path, index=False)

    agent = AnalysisAgent(str(db_path), str(parquet_path))
    out = agent.run_all()
    assert set(out.keys()) == {"descriptive", "diagnostic", "predictive"}
