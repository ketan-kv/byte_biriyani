from __future__ import annotations

import pandas as pd
from sklearn.ensemble import IsolationForest


def process_sensor_batch(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    working = df.copy()
    working["timestamp"] = pd.to_datetime(working["timestamp"], errors="coerce")
    working = working.dropna(subset=["timestamp"]).sort_values("timestamp")

    if "sensor_type" not in working.columns:
        working["sensor_type"] = "unknown"

    agg_spec: dict[str, str] = {"value": "mean"}
    for col in ["unit", "zone_id", "source_file"]:
        if col in working.columns:
            agg_spec[col] = "first"

    resampled = (
        working.set_index("timestamp")
        .groupby(["equipment_id", "sensor_type"])
        .resample("1min")
        .agg(agg_spec)
        .ffill(limit=5)
        .reset_index()
    )
    return resampled


def extract_sensor_features(df: pd.DataFrame, window: str = "1h") -> pd.DataFrame:
    if df.empty:
        return df
    working = df.copy()
    working["timestamp"] = pd.to_datetime(working["timestamp"], errors="coerce")
    working = working.dropna(subset=["timestamp"]) 
    features = (
        working.groupby(["equipment_id", "sensor_type"]) 
        .rolling(window, on="timestamp")["value"]
        .agg(
            mean_val="mean",
            std_val="std",
            max_val="max",
            min_val="min",
        )
        .reset_index()
    )
    features["range_val"] = features["max_val"] - features["min_val"]
    return features


def zscore_anomalies(df: pd.DataFrame, threshold: float = 3.0) -> pd.DataFrame:
    if df.empty:
        return df
    working = df.copy()
    std = working["value"].std()
    if std is None or std == 0:
        working["z_score"] = 0.0
        working["is_anomaly"] = False
        return working
    working["z_score"] = (working["value"] - working["value"].mean()) / std
    working["is_anomaly"] = working["z_score"].abs() > threshold
    return working


def isolation_forest_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    working = df.copy()
    cols = [c for c in ["value", "z_score", "range_val"] if c in working.columns]
    if not cols:
        working["anomaly_if"] = False
        return working
    data = working[cols].fillna(0)
    clf = IsolationForest(contamination=0.05, random_state=42)
    working["anomaly_if"] = clf.fit_predict(data) == -1
    return working
