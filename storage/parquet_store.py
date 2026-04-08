from __future__ import annotations

from pathlib import Path

import pandas as pd


DEFAULT_SENSOR_PARQUET = Path("data/structured/sensor_data.parquet")


def append_sensor_data(df: pd.DataFrame, parquet_path: str | Path = DEFAULT_SENSOR_PARQUET) -> int:
    path = Path(parquet_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, df], ignore_index=True)
        combined.to_parquet(path, index=False)
        return len(df)

    df.to_parquet(path, index=False)
    return len(df)


def read_sensor_data(parquet_path: str | Path = DEFAULT_SENSOR_PARQUET) -> pd.DataFrame:
    path = Path(parquet_path)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)
