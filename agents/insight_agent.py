from __future__ import annotations

import json
import os
from uuid import uuid4

import ollama
from dotenv import load_dotenv

from analytics.insight_fuser import fuse_signals


load_dotenv()

OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "mistral:latest")
OLLAMA_HOST = os.getenv("OLLAMA_HOST")
OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY")

client_kwargs = {"host": OLLAMA_HOST} if OLLAMA_HOST else {}
if OLLAMA_API_KEY:
    client_kwargs["headers"] = {"Authorization": f"Bearer {OLLAMA_API_KEY}"}
OLLAMA_CLIENT = ollama.Client(**client_kwargs)


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

    def generate_with_llm(self, analysis: dict, vocabulary: list[str], domain: str) -> list[dict]:
        compact_payload = self._build_compact_payload(analysis)
        prompt = (
            f"You are a senior {domain} data analyst.\n"
            f"Domain vocabulary: {vocabulary}\n"
            "Use the analytics summary to produce business-ready insights and a decision narrative.\n"
            "Return ONLY valid JSON as an array of 4 to 8 objects sorted by business impact.\n"
            "Each object must contain exactly these keys: "
            '"severity", "category", "title", "explanation", "recommendation", "confidence", "data_refs".\n'
            'Rules: "severity" is one of INFO, WARNING, CRITICAL; "confidence" is a float between 0 and 1; '
            '"data_refs" is a list of strings.\n'
            'Make recommendations specific and action-oriented.\n'
            f"Analytics payload: {json.dumps(compact_payload, default=str)}"
        )

        try:
            response = OLLAMA_CLIENT.chat(
                model=OLLAMA_MODEL,
                messages=[{"role": "user", "content": prompt}],
                format="json",
            )
            raw = response["message"]["content"]
            parsed = json.loads(raw)
            if isinstance(parsed, dict) and isinstance(parsed.get("insights"), list):
                parsed = parsed["insights"]
            if not isinstance(parsed, list):
                return self._fallback_uploaded_insights(analysis, domain)

            normalized: list[dict] = []
            for item in parsed:
                if not isinstance(item, dict):
                    continue
                normalized.append(
                    {
                        "id": str(uuid4()),
                        "severity": self._normalize_severity(item.get("severity", "INFO")),
                        "category": str(item.get("category", "general")),
                        "title": str(item.get("title", "Insight")),
                        "explanation": str(item.get("explanation", "")),
                        "recommendation": str(item.get("recommendation", "")),
                        "confidence": max(0.0, min(1.0, float(item.get("confidence", 0.6)))),
                        "data_refs": item.get("data_refs", []) if isinstance(item.get("data_refs", []), list) else [],
                    }
                )
            return normalized or self._fallback_uploaded_insights(analysis, domain)
        except Exception:
            return self._fallback_uploaded_insights(analysis, domain)

    def _normalize_severity(self, severity: str) -> str:
        s = str(severity).strip().upper()
        if s not in {"INFO", "WARNING", "CRITICAL"}:
            return "INFO"
        return s

    def _build_compact_payload(self, analysis: dict) -> dict:
        descriptive = analysis.get("descriptive", {})
        diagnostic = analysis.get("diagnostic", {})
        predictive = analysis.get("predictive", {})

        return {
            "overview": descriptive.get("overview", {}),
            "top_numeric": descriptive.get("numeric_profile", [])[:8],
            "top_categorical": descriptive.get("categorical_profile", [])[:6],
            "time_profile": descriptive.get("time_profile", {}),
            "missingness": diagnostic.get("missingness", [])[:10],
            "outliers": diagnostic.get("outlier_scan", [])[:10],
            "correlations": diagnostic.get("correlation_top", [])[:10],
            "risk_signals": predictive.get("risk_signals", []),
        }

    def _fallback_uploaded_insights(self, analysis: dict, domain: str) -> list[dict]:
        descriptive = analysis.get("descriptive", {})
        diagnostic = analysis.get("diagnostic", {})
        predictive = analysis.get("predictive", {})

        insights: list[dict] = []
        missing = diagnostic.get("missingness", [])
        outliers = diagnostic.get("outlier_scan", [])
        corr = diagnostic.get("correlation_top", [])
        overview = descriptive.get("overview", {})

        top_missing = next((x for x in missing if x.get("missing_pct", 0) > 0.2), None)
        if top_missing:
            insights.append(
                {
                    "id": str(uuid4()),
                    "severity": "WARNING",
                    "category": "data_quality",
                    "title": f"High missingness in {top_missing['column']}",
                    "explanation": f"{top_missing['column']} is missing in {top_missing['missing_pct']:.1%} of records, which can skew domain conclusions.",
                    "recommendation": "Define imputation or data collection rules for this field before KPI reporting.",
                    "confidence": 0.82,
                    "data_refs": [str(top_missing["column"])],
                }
            )

        top_outlier = next((x for x in outliers if x.get("outlier_pct", 0) > 0.08), None)
        if top_outlier:
            insights.append(
                {
                    "id": str(uuid4()),
                    "severity": "WARNING",
                    "category": "anomaly",
                    "title": f"Metric volatility spike in {top_outlier['column']}",
                    "explanation": f"{top_outlier['column']} has {top_outlier['outlier_pct']:.1%} outlier points, signaling unstable process behavior.",
                    "recommendation": "Segment this metric by time and operational slice to isolate root-cause drivers.",
                    "confidence": 0.79,
                    "data_refs": [str(top_outlier["column"])],
                }
            )

        strong_corr = next((x for x in corr if abs(x.get("corr", 0)) > 0.8), None)
        if strong_corr:
            insights.append(
                {
                    "id": str(uuid4()),
                    "severity": "INFO",
                    "category": "relationship",
                    "title": f"Strong driver relationship detected: {strong_corr['left']} vs {strong_corr['right']}",
                    "explanation": f"Correlation is {strong_corr['corr']:.2f}, indicating these variables move together and can explain outcome shifts.",
                    "recommendation": "Use this pair in diagnostic drill-downs and forecasting features for better signal capture.",
                    "confidence": 0.76,
                    "data_refs": [str(strong_corr["left"]), str(strong_corr["right"])],
                }
            )

        dup_pct = float(overview.get("duplicate_pct", 0.0))
        if dup_pct > 0.03:
            insights.append(
                {
                    "id": str(uuid4()),
                    "severity": "WARNING",
                    "category": "data_governance",
                    "title": "Duplicate records may bias KPI trends",
                    "explanation": f"Duplicate rows are {dup_pct:.1%} of the dataset, which can inflate counts and distort trend analysis.",
                    "recommendation": "Apply deduplication keys and validate row-level uniqueness before publishing dashboards.",
                    "confidence": 0.73,
                    "data_refs": ["duplicate_rows"],
                }
            )

        for signal in predictive.get("risk_signals", [])[:2]:
            insights.append(
                {
                    "id": str(uuid4()),
                    "severity": "INFO",
                    "category": "risk_signal",
                    "title": f"{domain.title()} risk signal",
                    "explanation": str(signal),
                    "recommendation": "Track this signal in a weekly analytics review and tie it to business outcomes.",
                    "confidence": 0.68,
                    "data_refs": [],
                }
            )

        if not insights:
            insights.append(
                {
                    "id": str(uuid4()),
                    "severity": "INFO",
                    "category": "summary",
                    "title": "Stable baseline detected",
                    "explanation": "No critical risk pattern was detected from current profiling.",
                    "recommendation": "Deepen domain-specific analysis by selecting a target KPI and segmenting by top business dimensions.",
                    "confidence": 0.62,
                    "data_refs": [],
                }
            )
        return insights[:8]

    def build_executive_storyline(self, analysis: dict, insights: list[dict], domain: str) -> list[dict]:
        descriptive = analysis.get("descriptive", {})
        diagnostic = analysis.get("diagnostic", {})
        predictive = analysis.get("predictive", {})

        overview = descriptive.get("overview", {})
        prep = descriptive.get("data_prep", {})
        missing = diagnostic.get("missingness", [])
        outliers = diagnostic.get("outlier_scan", [])
        corr = diagnostic.get("correlation_top", [])
        risk_signals = predictive.get("risk_signals", [])

        storyline: list[dict] = [
            {
                "type": "context",
                "title": f"Domain context established for {domain.title()}",
                "message": (
                    f"The system processed {overview.get('rows', 0):,} rows across {overview.get('columns', 0)} columns "
                    "to build a full-dataset evidence baseline."
                ),
                "impact": "high",
            }
        ]

        if prep:
            storyline.append(
                {
                    "type": "process",
                    "title": "Data preparation applied",
                    "message": (
                        f"Missing-value strategy '{prep.get('missing_strategy', 'none')}' was used; "
                        f"numeric nulls moved from {prep.get('numeric_missing_before', 0):,} to {prep.get('numeric_missing_after', 0):,}."
                    ),
                    "impact": "medium",
                }
            )

        strong_corr = next((x for x in corr if abs(x.get("corr", 0)) > 0.7), None)
        if strong_corr:
            storyline.append(
                {
                    "type": "positive",
                    "title": "Strong explanatory signal found",
                    "message": (
                        f"{strong_corr['left']} and {strong_corr['right']} have correlation {strong_corr['corr']:.2f}, "
                        "which is useful for forecasting and root-cause analysis."
                    ),
                    "impact": "high",
                }
            )

        high_missing = next((x for x in missing if x.get("missing_pct", 0) > 0.15), None)
        if high_missing:
            storyline.append(
                {
                    "type": "risk",
                    "title": "Data reliability risk",
                    "message": (
                        f"{high_missing['column']} has {high_missing['missing_pct']:.1%} missing values; "
                        "business decisions on this field should be treated cautiously."
                    ),
                    "impact": "high",
                }
            )

        heavy_outlier = next((x for x in outliers if x.get("outlier_pct", 0) > 0.06), None)
        if heavy_outlier:
            storyline.append(
                {
                    "type": "opportunity",
                    "title": "Potential optimization hotspot",
                    "message": (
                        f"{heavy_outlier['column']} shows {heavy_outlier['outlier_pct']:.1%} outlier concentration; "
                        "this is a candidate for process improvement and anomaly controls."
                    ),
                    "impact": "medium",
                }
            )

        top_ai = insights[0] if insights else None
        if top_ai:
            storyline.append(
                {
                    "type": "decision",
                    "title": "Top AI recommendation",
                    "message": top_ai.get("recommendation", "No recommendation generated."),
                    "impact": "high",
                }
            )

        if risk_signals:
            storyline.append(
                {
                    "type": "next_step",
                    "title": "Actionable next step",
                    "message": str(risk_signals[0]),
                    "impact": "medium",
                }
            )

        return storyline[:8]
