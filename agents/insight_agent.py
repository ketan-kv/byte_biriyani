from __future__ import annotations

from analytics.insight_fuser import fuse_signals


class InsightAgent:
    def generate(self, analysis: dict) -> list[dict]:
        return fuse_signals(
            descriptive=analysis.get("descriptive", {}),
            diagnostic=analysis.get("diagnostic", {}),
            predictive=analysis.get("predictive", {}),
        )

    def generate_urgent(self, anomaly_flag: dict) -> list[dict]:
        eq_list = anomaly_flag.get("equipment_ids", []) if anomaly_flag else []
        count = anomaly_flag.get("count", 0) if anomaly_flag else 0
        if not eq_list:
            return []
        return [
            {
                "id": f"urgent-{idx}",
                "severity": "CRITICAL",
                "category": "equipment",
                "title": f"Urgent anomaly detected for {eq_id}",
                "explanation": f"Continuous anomaly monitor detected active alert ({count} events observed).",
                "recommendation": f"Inspect {eq_id} immediately and reduce load until checks are complete.",
                "confidence": 0.9,
                "data_refs": [eq_id],
            }
            for idx, eq_id in enumerate(eq_list, start=1)
        ]
