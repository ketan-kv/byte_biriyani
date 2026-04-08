from __future__ import annotations

from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler


def train_failure_model(sensor_features: pd.DataFrame) -> tuple[GradientBoostingClassifier, StandardScaler]:
    features = [c for c in ["mean_vibration", "max_vibration", "std_vibration", "mean_temperature", "anomaly_count_6h"] if c in sensor_features.columns]
    if not features or "failed_in_24h" not in sensor_features.columns:
        raise ValueError("Training data must contain feature columns and failed_in_24h label")

    x = sensor_features[features].fillna(0)
    y = sensor_features["failed_in_24h"].astype(int)

    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(x)
    model = GradientBoostingClassifier(n_estimators=100, max_depth=4)
    model.fit(x_scaled, y)
    return model, scaler


def save_model(model: GradientBoostingClassifier, scaler: StandardScaler, output_path: str = "models/failure_model.pkl") -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": model, "scaler": scaler}, output_path)
