from __future__ import annotations

import pandas as pd

from agents.structuring_agent import StructuringAgent


def test_structure_geological_report() -> None:
    agent = StructuringAgent()
    raw = {
        "type": "geological_report",
        "content": "Zone ZA-N reported Gold grade 4.2 g/t at depth 120 m on 2024-03-15",
        "source_path": "sample_report.pdf",
    }
    result = agent.run(raw, "geological_report")
    assert result["table"] == "geological_records"
    assert len(result["records"]) >= 1


def test_structure_sensor_csv() -> None:
    agent = StructuringAgent()
    df = pd.DataFrame(
        [
            {
                "timestamp": "2024-03-15T08:32:00Z",
                "equipment_id": "PUMP-003",
                "sensor_type": "vibration",
                "value": 14.7,
                "unit": "mm/s",
                "zone_id": "ZA-N",
            }
        ]
    )
    raw = {"type": "sensor_csv", "content": df, "source_path": "sensor.csv"}
    result = agent.run(raw, "sensor_csv")
    assert "sensor_df" in result
    assert len(result["sensor_df"]) >= 1
