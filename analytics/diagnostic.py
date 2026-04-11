from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


def diagnose_efficiency_drop(
    conn: sqlite3.Connection,
    sensor_parquet_path: str | Path,
    threshold_pct: float = 10.0,
) -> list[dict]:
    try:
        prod_df = pd.read_sql("SELECT * FROM extraction_logs", conn)
    except Exception:
        return []

    if prod_df.empty:
        return []

    prod_df["date"] = pd.to_datetime(prod_df["log_date"], errors="coerce")
    prod_df = prod_df.dropna(subset=["date"]).sort_values("date")
    prod_df["eff_change"] = prod_df.groupby("zone_id", dropna=False)["efficiency_pct"].pct_change() * 100

    drops = prod_df[prod_df["eff_change"] < -abs(threshold_pct)]
    if drops.empty:
        return []

    sensor_path = Path(sensor_parquet_path)
    if sensor_path.exists():
        sensor_df = pd.read_parquet(sensor_path)
        if "timestamp" in sensor_df.columns:
            sensor_df["timestamp"] = pd.to_datetime(sensor_df["timestamp"], errors="coerce", utc=True)
            sensor_df["timestamp"] = sensor_df["timestamp"].dt.tz_localize(None)
    else:
        sensor_df = pd.DataFrame()

    diagnoses: list[dict] = []
    for _, row in drops.iterrows():
        if sensor_df.empty:
            related = pd.DataFrame()
        else:
            start = row["date"] - pd.Timedelta(hours=2)
            end = row["date"] + pd.Timedelta(hours=2)
            related = sensor_df[
                (sensor_df.get("zone_id") == row.get("zone_id"))
                & (sensor_df.get("timestamp") >= start)
                & (sensor_df.get("timestamp") <= end)
                & (sensor_df.get("is_anomaly") == True)
            ]

        suspected = (
            related[["equipment_id", "sensor_type", "value"]].to_dict("records")
            if not related.empty
            else []
        )
        diagnoses.append(
            {
                "date": str(row["date"]),
                "zone_id": row.get("zone_id"),
                "eff_drop_pct": round(float(row["eff_change"]), 2),
                "suspected_cause": suspected,
            }
        )
    return diagnoses


def summarize_anomalies(sensor_df: pd.DataFrame) -> dict:
    if sensor_df.empty:
        return {"total_anomalies": 0, "by_equipment": {}, "by_sensor_type": {}}

    working = sensor_df.copy()
    if "is_anomaly" not in working.columns:
        working["is_anomaly"] = False
    if "equipment_id" not in working.columns:
        working["equipment_id"] = "unknown"
    if "sensor_type" not in working.columns:
        working["sensor_type"] = "unknown"
    anomalies = working[working["is_anomaly"] == True]

    return {
        "total_anomalies": int(len(anomalies)),
        "by_equipment": anomalies.groupby("equipment_id").size().to_dict(),
        "by_sensor_type": anomalies.groupby("sensor_type").size().to_dict(),
    }
