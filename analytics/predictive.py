from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


def _heuristic_failure_risk(sensor_df: pd.DataFrame) -> dict:
    if sensor_df.empty:
        return {}

    working = sensor_df.copy()
    if "timestamp" in working.columns:
        working["timestamp"] = pd.to_datetime(working["timestamp"], errors="coerce")
        recent_cutoff = working["timestamp"].max() - pd.Timedelta(hours=6)
        working = working[working["timestamp"] >= recent_cutoff]

    if "is_anomaly" not in working.columns:
        working["is_anomaly"] = False

    out: dict[str, dict] = {}
    for equipment_id, group in working.groupby("equipment_id"):
        anomaly_count = int(group["is_anomaly"].sum())
        base = min(0.2 + anomaly_count * 0.12, 0.95)
        out[str(equipment_id)] = {
            "probability": round(base, 3),
            "horizon_hours": 24,
            "anomaly_count_6h": anomaly_count,
        }
    return out


def forecast_yield(conn: sqlite3.Connection, periods: int = 7) -> list[dict]:
    try:
        prod_df = pd.read_sql("SELECT log_date, yield_tonnes FROM extraction_logs ORDER BY log_date", conn)
    except Exception:
        return []

    if prod_df.empty:
        return []

    prod_df["log_date"] = pd.to_datetime(prod_df["log_date"], errors="coerce")
    prod_df = prod_df.dropna(subset=["log_date", "yield_tonnes"])
    if prod_df.empty:
        return []

    daily = prod_df.groupby("log_date", as_index=False)["yield_tonnes"].sum()

    # Keep a robust fallback forecast for minimal setup environments.
    last_mean = float(daily["yield_tonnes"].tail(7).mean())
    start = daily["log_date"].max()
    future_dates = [start + pd.Timedelta(days=i) for i in range(1, periods + 1)]
    return [{"ds": d.strftime("%Y-%m-%d"), "yhat": round(last_mean, 2)} for d in future_dates]


def predictive_bundle(conn: sqlite3.Connection, sensor_parquet_path: str | Path) -> dict:
    sensor_path = Path(sensor_parquet_path)
    sensor_df = pd.read_parquet(sensor_path) if sensor_path.exists() else pd.DataFrame()
    failure_risk = _heuristic_failure_risk(sensor_df)
    yield_forecast = forecast_yield(conn)
    return {"failure_risk": failure_risk, "yield_forecast": yield_forecast}
