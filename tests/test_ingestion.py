from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipelines.ingestion.csv_parser import parse_production_csv, parse_sensor_csv
from pipelines.ingestion.ingestion_router import detect_file_type
from pipelines.ingestion.log_parser import parse_log


def test_detect_file_type_sensor() -> None:
    assert detect_file_type("data/raw/sensor/sensors_2024-03-15.csv") == "sensor_csv"


def test_parse_sensor_csv(tmp_path: Path) -> None:
    sample = tmp_path / "sensor.csv"
    sample.write_text(
        "timestamp,equipment_id,sensor_type,value,unit,zone_id\n"
        "2024-03-15T08:32:00Z,PUMP-003,vibration,14.7,mm/s,ZA-N\n",
        encoding="utf-8",
    )
    df = parse_sensor_csv(sample)
    assert len(df) == 1
    assert "is_anomaly" in df.columns


def test_parse_production_csv_computes_efficiency(tmp_path: Path) -> None:
    sample = tmp_path / "production.csv"
    sample.write_text(
        "log_date,zone_id,yield_tonnes,ore_processed_t\n"
        "2024-03-15,ZA-N,42.3,890.0\n",
        encoding="utf-8",
    )
    df = parse_production_csv(sample)
    assert not pd.isna(df.loc[0, "efficiency_pct"])


def test_parse_log_extracts_fields(tmp_path: Path) -> None:
    sample = tmp_path / "incident.log"
    sample.write_text("Date: 2024-03-15\nEquipment: CRUSHER-01\n", encoding="utf-8")
    payload = parse_log(sample)
    assert payload["fields"]["date"] == "2024-03-15"
