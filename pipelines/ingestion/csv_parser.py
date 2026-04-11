from __future__ import annotations

from pathlib import Path

import pandas as pd


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def parse_sensor_csv(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["timestamp"])
    df = _normalize_columns(df)
    df = df.dropna(subset=["timestamp"])
    if "is_anomaly" not in df.columns:
        df["is_anomaly"] = False
    if "source_file" not in df.columns:
        df["source_file"] = str(path)
    return df


def parse_production_csv(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["log_date"])
    df = _normalize_columns(df)
    if "efficiency_pct" not in df.columns and {"yield_tonnes", "ore_processed_t"}.issubset(df.columns):
        denom = df["ore_processed_t"].replace(0, pd.NA)
        df["efficiency_pct"] = (df["yield_tonnes"] / denom) * 100
    df["source_file"] = str(path)
    return df


def parse_generic_csv(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = _normalize_columns(df)
    df["source_file"] = str(path)
    return df
