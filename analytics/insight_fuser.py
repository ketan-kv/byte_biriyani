from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from uuid import uuid4


SEVERITY_ORDER = {"CRITICAL": 0, "WARNING": 1, "INFO": 2}


@dataclass
class Insight:
    id: str
    severity: str
    category: str
    title: str
    explanation: str
    recommendation: str
    confidence: float
    data_refs: list[str]
    generated_at: str


def _new_insight(
    severity: str,
    category: str,
    title: str,
    explanation: str,
    recommendation: str,
    confidence: float,
    data_refs: list[str] | None = None,
) -> Insight:
    return Insight(
        id=str(uuid4()),
        severity=severity,
        category=category,
        title=title,
        explanation=explanation,
        recommendation=recommendation,
        confidence=max(0.0, min(confidence, 1.0)),
        data_refs=data_refs or [],
        generated_at=datetime.now(timezone.utc).isoformat(),
    )


def fuse_signals(descriptive: dict, diagnostic: dict, predictive: dict) -> list[dict]:
    insights: list[Insight] = []

    for eq_id, risk_data in predictive.get("failure_risk", {}).items():
        risk = float(risk_data.get("probability", 0))
        anomaly_count = int(risk_data.get("anomaly_count_6h", 0))
        if risk > 0.7 and anomaly_count > 3:
            insights.append(
                _new_insight(
                    severity="CRITICAL",
                    category="equipment",
                    title=f"{eq_id} combined failure signal",
                    explanation=(
                        f"Model indicates {risk * 100:.0f}% failure risk in next "
                        f"{risk_data.get('horizon_hours', 24)}h with {anomaly_count} recent anomalies."
                    ),
                    recommendation=f"Stop and inspect {eq_id} immediately.",
                    confidence=min(risk + 0.1, 1.0),
                    data_refs=[eq_id],
                )
            )
        elif risk > 0.5:
            insights.append(
                _new_insight(
                    severity="WARNING",
                    category="equipment",
                    title=f"Elevated failure risk for {eq_id}",
                    explanation=f"Predicted risk is {risk * 100:.0f}% in next 24h.",
                    recommendation=f"Schedule preemptive maintenance for {eq_id}.",
                    confidence=risk,
                    data_refs=[eq_id],
                )
            )

    for drop in diagnostic.get("efficiency_drops", []):
        if not drop.get("suspected_cause"):
            insights.append(
                _new_insight(
                    severity="WARNING",
                    category="yield",
                    title=f"Unexplained efficiency drop in {drop.get('zone_id', 'unknown')}",
                    explanation=f"Efficiency dropped {drop.get('eff_drop_pct', 'n/a')}% without correlated sensor anomalies.",
                    recommendation="Review operator logs and ore feed quality for this zone.",
                    confidence=0.65,
                )
            )

    if not insights:
        insights.append(
            _new_insight(
                severity="INFO",
                category="system",
                title="No critical signals detected",
                explanation="Current data does not indicate urgent failures or severe production degradation.",
                recommendation="Continue monitoring and run scheduled analytics.",
                confidence=0.8,
            )
        )

    return [asdict(i) for i in sorted(insights, key=lambda item: SEVERITY_ORDER[item.severity])]
