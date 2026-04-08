from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from analytics.descriptive import mineral_distribution, production_trend
from analytics.diagnostic import diagnose_efficiency_drop, summarize_anomalies
from analytics.predictive import predictive_bundle


class AnalysisAgent:
    def __init__(self, db_path: str, sensor_parquet_path: str) -> None:
        self.db_path = Path(db_path)
        self.sensor_parquet_path = Path(sensor_parquet_path)
        self.anomaly_flag: dict | None = None

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def run_all(self) -> dict:
        with self._connect() as conn:
            descriptive = self.descriptive_analytics(conn)
            diagnostic = self.diagnostic_analytics(conn)
            predictive = self.predictive_analytics(conn)
        return {
            "descriptive": descriptive,
            "diagnostic": diagnostic,
            "predictive": predictive,
        }
    
    # ADD this method inside the AnalysisAgent class, after run_all()

    def run_all_with_context(self, knowledge: dict) -> dict:
        """Run standard analytics then enrich with domain knowledge context."""
        base = self.run_all()
        base["domain_kpis"] = knowledge.get("kpis", [])
        base["domain_thresholds"] = knowledge.get("anomaly_thresholds", {})
        base["analysis_priorities"] = knowledge.get("analysis_priorities", [])
        return base

    def descriptive_analytics(self, conn: sqlite3.Connection) -> dict:
        return {
            "production": production_trend(conn),
            "mineral_distribution": mineral_distribution(conn),
        }

    def diagnostic_analytics(self, conn: sqlite3.Connection) -> dict:
        sensor_df = pd.read_parquet(self.sensor_parquet_path) if self.sensor_parquet_path.exists() else pd.DataFrame()
        anomaly_summary = summarize_anomalies(sensor_df)
        efficiency_drops = diagnose_efficiency_drop(conn, self.sensor_parquet_path)
        return {
            "anomalies": anomaly_summary,
            "efficiency_drops": efficiency_drops,
        }

    def predictive_analytics(self, conn: sqlite3.Connection) -> dict:
        return predictive_bundle(conn, self.sensor_parquet_path)

    def update_anomaly_flag(self) -> None:
        if not self.sensor_parquet_path.exists():
            return
        sensor_df = pd.read_parquet(self.sensor_parquet_path)
        if sensor_df.empty or "is_anomaly" not in sensor_df.columns:
            return
        recent = sensor_df.tail(500)
        anomalies = recent[recent["is_anomaly"] == True]
        if anomalies.empty:
            return
        self.anomaly_flag = {
            "count": int(len(anomalies)),
            "equipment_ids": sorted({str(x) for x in anomalies.get("equipment_id", [])}),
        }

    def check_anomaly_flag(self) -> dict | None:
        self.update_anomaly_flag()
        flag = self.anomaly_flag
        self.anomaly_flag = None
        return flag
