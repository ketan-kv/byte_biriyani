"""Insight Agent — generates rich, actionable, balanced AI insights from analysis results."""
from __future__ import annotations

import json
import os
from uuid import uuid4

import ollama
from dotenv import load_dotenv

from analytics.insight_fuser import fuse_signals


load_dotenv()

OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "mistral")
OLLAMA_HOST = os.getenv("OLLAMA_HOST")
OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY")

client_kwargs = {"host": OLLAMA_HOST} if OLLAMA_HOST else {}
if OLLAMA_API_KEY:
    client_kwargs["headers"] = {"Authorization": f"Bearer {OLLAMA_API_KEY}"}
OLLAMA_CLIENT = ollama.Client(**client_kwargs)


_INSIGHT_SYSTEM = """You are a senior data analyst with 15+ years of experience across industries.
You read analytics results and produce insights that are:
1. BALANCED — include both positive findings and risks (not only problems)
2. SPECIFIC — reference actual numbers, column names, and percentages from the data
3. ACTIONABLE — every recommendation is a concrete step, not vague advice
4. DOMAIN-AWARE — explain findings in terms of the business domain
Return ONLY valid JSON."""

_INSIGHT_USER = """Domain: {domain}
Domain vocabulary: {vocabulary}
Domain correlation hypotheses: {hypotheses}
Decision rules for this domain: {decision_rules}
Positive signals to validate: {positive_signals}

Analytics payload:
{payload}

Generate {min_count} to {max_count} insights as a JSON array. Aim for this mix:
- ~30% positive insights (what's working well, exceeded benchmarks, strong signals)
- ~40% risk / warning insights (problems, anomalies, data quality)
- ~30% opportunity insights (correlation-driven, trend-driven recommendations)

Each insight object must have EXACTLY these keys:
- "severity": one of INFO | WARNING | CRITICAL
- "insight_type": one of positive | risk | opportunity | anomaly | trend | relationship | data_quality
- "category": domain-relevant category string
- "title": concise title (under 12 words) referencing a specific metric or column
- "explanation": 2–3 sentences. Must include specific numbers, column names, and percentages from the payload. Explain the business implication.
- "recommendation": numbered concrete steps starting with action verbs. E.g. "1. Segment the revenue column by region 2. Filter dates to last 90 days 3. Set alert threshold at [N]"
- "confidence": float 0.0–1.0
- "data_refs": list of column names that this insight is based on
- "deep_analysis": 4–5 sentences with additional drill-down context, hypotheses for why this pattern exists, what to investigate next, and business impact quantification if possible

Sort by business impact: CRITICAL first, then WARNING, then positive INFO, then opportunity INFO.
No markdown. JSON array only."""


_DEEP_INSIGHT_SYSTEM = """You are a senior data analyst conducting a deep investigation into one specific metric pattern.
Your response is thorough, data-backed, and helps a business user understand exactly what is happening and what to do.
Return ONLY valid JSON."""

_DEEP_INSIGHT_USER = """Domain: {domain}
Insight being explored:
{insight}

Full analysis context:
{context}

Produce a deep-dive analysis JSON object with exactly these keys:
- "extended_explanation": 5–6 sentences explaining the pattern in depth. Include statistical context, business implications, and confidence level.
- "root_cause_hypotheses": list of 3 possible root causes (each a 1–2 sentence string)
- "investigation_steps": numbered list of 5 concrete investigation steps
- "related_metrics": list of up to 5 column names from the dataset that are related to this insight
- "business_impact": string estimating the $ or % business impact if this insight is acted upon vs ignored
- "comparable_benchmark": string with an industry benchmark to compare against
- "quick_win": string describing the single fastest action that can be taken today (within 24 hours)

JSON only."""


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
                "insight_type": "anomaly",
                "category": "equipment",
                "title": f"Urgent anomaly detected for {eq_id}",
                "explanation": f"Continuous anomaly monitor detected active alert ({count} events observed).",
                "recommendation": f"1. Inspect {eq_id} immediately\n2. Reduce operational load until checks complete\n3. Log findings in maintenance system",
                "confidence": 0.9,
                "data_refs": [eq_id],
                "deep_analysis": f"Equipment {eq_id} has triggered {count} anomaly events in the monitoring window. This pattern indicates process instability that requires immediate physical inspection.",
            }
            for idx, eq_id in enumerate(eq_list, start=1)
        ]

    def generate_with_llm(
        self,
        analysis: dict,
        vocabulary: list[str],
        domain: str,
        count: int = 6,
        knowledge: dict | None = None,
    ) -> list[dict]:
        compact_payload = self._build_compact_payload(analysis)
        min_count = max(2, min(count, 10))
        max_count = min(12, min_count + 2)

        # Pull extra context from research knowledge if available
        knowledge = knowledge or {}
        hypotheses = json.dumps(knowledge.get("correlation_hypotheses", []))
        decision_rules = json.dumps(knowledge.get("decision_rules", []))
        positive_signals = json.dumps(knowledge.get("positive_signals", []))

        prompt = _INSIGHT_USER.format(
            domain=domain,
            vocabulary=json.dumps(vocabulary),
            hypotheses=hypotheses,
            decision_rules=decision_rules,
            positive_signals=positive_signals,
            payload=json.dumps(compact_payload, default=str),
            min_count=min_count,
            max_count=max_count,
        )

        try:
            response = OLLAMA_CLIENT.chat(
                model=OLLAMA_MODEL,
                messages=[
                    {"role": "system", "content": _INSIGHT_SYSTEM},
                    {"role": "user", "content": prompt},
                ],
                format="json",
                options={"num_ctx": 2048, "num_predict": 1024, "num_thread": 8},
                keep_alive="1h",
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
                normalized.append(self._normalize_insight(item))
            return normalized or self._fallback_uploaded_insights(analysis, domain)
        except Exception:
            return self._fallback_uploaded_insights(analysis, domain)

    def generate_deep_insight(
        self, insight: dict, analysis: dict, domain: str
    ) -> dict:
        """Generate an extended deep-dive analysis for a single insight card."""
        context = self._build_compact_payload(analysis)
        prompt = _DEEP_INSIGHT_USER.format(
            domain=domain,
            insight=json.dumps(insight, default=str),
            context=json.dumps(context, default=str),
        )
        try:
            response = OLLAMA_CLIENT.chat(
                model=OLLAMA_MODEL,
                messages=[
                    {"role": "system", "content": _DEEP_INSIGHT_SYSTEM},
                    {"role": "user", "content": prompt},
                ],
                format="json",
                options={"num_ctx": 2048, "num_predict": 1024, "num_thread": 8},
                keep_alive="1h",
            )
            raw = response["message"]["content"]
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                return self._fallback_deep(insight)
            return parsed
        except Exception:
            return self._fallback_deep(insight)

    def _fallback_deep(self, insight: dict) -> dict:
        return {
            "extended_explanation": insight.get("explanation", "No additional context available."),
            "root_cause_hypotheses": [
                "Data collection gap causing incomplete measurements.",
                "External business event affecting the metric.",
                "Process change not yet reflected in historical baselines.",
            ],
            "investigation_steps": [
                "1. Filter the dataset to the specific segment mentioned in the insight.",
                "2. Plot the metric over time to identify when the change began.",
                "3. Cross-reference with operational logs or business events.",
                "4. Compare against a control group or prior period.",
                "5. Escalate to domain stakeholders for context.",
            ],
            "related_metrics": insight.get("data_refs", []),
            "business_impact": "Impact quantification requires additional business context.",
            "comparable_benchmark": "Industry benchmarks vary; consult domain SME for specifics.",
            "quick_win": insight.get("recommendation", "Review this metric with a domain expert today."),
        }

    def _normalize_insight(self, item: dict) -> dict:
        return {
            "id": str(uuid4()),
            "severity": self._normalize_severity(item.get("severity", "INFO")),
            "insight_type": str(item.get("insight_type", "risk")),
            "category": str(item.get("category", "general")),
            "title": str(item.get("title", "Insight")),
            "explanation": str(item.get("explanation", "")),
            "recommendation": str(item.get("recommendation", "")),
            "confidence": max(0.0, min(1.0, float(item.get("confidence", 0.6)))),
            "data_refs": item.get("data_refs", []) if isinstance(item.get("data_refs", []), list) else [],
            "deep_analysis": str(item.get("deep_analysis", "")),
        }

    def _normalize_severity(self, severity: str) -> str:
        s = str(severity).strip().upper()
        if s not in {"INFO", "WARNING", "CRITICAL"}:
            return "INFO"
        return s

    def _build_compact_payload(self, analysis: dict) -> dict:
        descriptive = analysis.get("descriptive", {})
        diagnostic = analysis.get("diagnostic", {}
        )
        predictive = analysis.get("predictive", {})
        return {
            "overview": descriptive.get("overview", {}),
            "data_prep": descriptive.get("data_prep", {}),
            "top_numeric": descriptive.get("numeric_profile", [])[:10],
            "top_categorical": descriptive.get("categorical_profile", [])[:6],
            "time_profile": descriptive.get("time_profile", {}),
            "trend_profile": {
                k: v for k, v in descriptive.get("trend_profile", {}).items()
                if k != "series"
            },
            "distribution_summary": descriptive.get("distribution_profile", {}).get("summary", {}),
            "segment_pareto_top5": (descriptive.get("segment_pareto", {}).get("rows", []) or [])[:5],
            "missingness": diagnostic.get("missingness", [])[:10],
            "outliers": diagnostic.get("outlier_scan", [])[:10],
            "correlations": diagnostic.get("correlation_top", [])[:12],
            "risk_signals": predictive.get("risk_signals", []),
            "action_plan": predictive.get("action_plan", []),
            "domain_kpis": analysis.get("domain_kpis", [])[:6],
            "analysis_priorities": analysis.get("analysis_priorities", [])[:6],
        }

    def _fallback_uploaded_insights(self, analysis: dict, domain: str) -> list[dict]:
        """Rule-based insights used when LLM is unavailable."""
        descriptive = analysis.get("descriptive", {})
        diagnostic = analysis.get("diagnostic", {})
        predictive = analysis.get("predictive", {})

        insights: list[dict] = []
        missing = diagnostic.get("missingness", [])
        outliers = diagnostic.get("outlier_scan", [])
        corr = diagnostic.get("correlation_top", [])
        overview = descriptive.get("overview", {})
        numeric_profile = descriptive.get("numeric_profile", [])
        segment_pareto = descriptive.get("segment_pareto", {})
        trend = descriptive.get("trend_profile", {})

        # ── Positive insights ────────────────────────────────────────────────
        # Strong correlation = opportunity
        strong_positive = [x for x in corr if x.get("corr", 0) > 0.75]
        if strong_positive:
            sp = strong_positive[0]
            insights.append({
                "id": str(uuid4()), "severity": "INFO", "insight_type": "positive",
                "category": "relationship",
                "title": f"Strong predictive signal: {sp['left']} ↔ {sp['right']}",
                "explanation": (
                    f"{sp['left']} and {sp['right']} are highly correlated (r={sp['corr']:.2f}). "
                    f"This is a strong predictive relationship that can be used to forecast outcomes and build models. "
                    f"In the {domain} domain, this kind of signal typically explains 60%+ of outcome variance."
                ),
                "recommendation": (
                    f"1. Plot {sp['left']} vs {sp['right']} over time\n"
                    f"2. Use {sp['left']} as a leading indicator for {sp['right']}\n"
                    f"3. Build a simple regression model between these two variables\n"
                    f"4. Set monitoring alerts when {sp['left']} deviates from baseline"
                ),
                "confidence": 0.87,
                "data_refs": [sp["left"], sp["right"]],
                "deep_analysis": (
                    f"The correlation of {sp['corr']:.2f} between {sp['left']} and {sp['right']} "
                    f"is statistically strong and actionable in {domain} analytics. "
                    f"This means changes in {sp['left']} reliably predict changes in {sp['right']}. "
                    f"This can be used for real-time alerting systems and forecasting models."
                ),
            })

        # Top segment performance
        pareto_rows = segment_pareto.get("rows", [])
        if segment_pareto.get("available") and pareto_rows:
            top = pareto_rows[0]
            seg_col = segment_pareto.get("segment_column", "segment")
            met_col = segment_pareto.get("metric_column", "metric")
            insights.append({
                "id": str(uuid4()), "severity": "INFO", "insight_type": "opportunity",
                "category": "segmentation",
                "title": f"Top performer: '{top['segment']}' in {seg_col}",
                "explanation": (
                    f"Segment '{top['segment']}' leads all groups with a {met_col} value of {top['value']:.2f}, "
                    f"accounting for {top.get('cumulative_pct', 0):.1%} of cumulative performance. "
                    f"Replicating its success factors to lower segments could significantly lift overall outcomes."
                ),
                "recommendation": (
                    f"1. Identify what distinguishes '{top['segment']}' — workflow, resources, or timing\n"
                    f"2. Interview stakeholders in this segment for best-practice documentation\n"
                    f"3. Apply the top segment's operating model to bottom 20%\n"
                    f"4. Track {met_col} monthly for each {seg_col} to measure lift"
                ),
                "confidence": 0.81,
                "data_refs": [seg_col, met_col] if met_col else [seg_col],
                "deep_analysis": (
                    f"Segment '{top['segment']}' shows standout performance on {met_col}. "
                    f"A Pareto analysis of {seg_col} reveals concentration of value in the top segments. "
                    f"This pattern suggests focus investment on top performers while developing under-performers."
                ),
            })

        # Trend positive
        if trend.get("available") and trend.get("series"):
            series = trend["series"]
            if len(series) >= 3:
                first_val = series[0].get("records", 0)
                last_val = series[-1].get("records", 0)
                if last_val > first_val * 1.05:
                    pct_growth = ((last_val - first_val) / max(first_val, 1)) * 100
                    insights.append({
                        "id": str(uuid4()), "severity": "INFO", "insight_type": "trend",
                        "category": "growth",
                        "title": f"Growing data volume trend detected ({pct_growth:.0f}% over period)",
                        "explanation": (
                            f"Record volume grew from {first_val:,} in {series[0]['period']} to "
                            f"{last_val:,} in {series[-1]['period']}, a {pct_growth:.1f}% increase. "
                            f"This indicates expanding operations or higher capture rates."
                        ),
                        "recommendation": (
                            "1. Validate if growth reflects real operational expansion or data pipeline improvements\n"
                            "2. Ensure infrastructure scales with volume trajectory\n"
                            "3. Review data quality at peak volume periods for errors"
                        ),
                        "confidence": 0.77,
                        "data_refs": [trend.get("date_column", "date")],
                        "deep_analysis": (
                            f"The {pct_growth:.1f}% growth in data volume over the tracked period is a meaningful signal. "
                            f"In {domain}, this could reflect business growth, increased data capture maturity, or new data sources. "
                            f"Investigate whether the growth is consistent or was driven by a single event."
                        ),
                    })

        # ── Risk insights ────────────────────────────────────────────────────
        top_missing = next((x for x in missing if x.get("missing_pct", 0) > 0.15), None)
        if top_missing:
            pct = top_missing["missing_pct"]
            insights.append({
                "id": str(uuid4()), "severity": "WARNING", "insight_type": "data_quality",
                "category": "data_quality",
                "title": f"High missingness in '{top_missing['column']}' ({pct:.1%})",
                "explanation": (
                    f"Column '{top_missing['column']}' is missing {pct:.1%} of its values. "
                    f"This means {int(pct * overview.get('rows', 0)):,} rows cannot contribute to analysis on this field. "
                    f"Downstream models and KPI calculations using this column will be unreliable."
                ),
                "recommendation": (
                    f"1. Investigate WHY '{top_missing['column']}' is missing — form design, optional field, or ETL gap?\n"
                    f"2. If {'< 0.3' if pct < 0.3 else '> 0.3'}: {'apply median imputation' if pct < 0.3 else 'consider dropping or flagging the column'}\n"
                    f"3. Add a data completeness SLA — target < 5% missing for critical columns\n"
                    f"4. Implement upstream data validation to catch this at source"
                ),
                "confidence": 0.91,
                "data_refs": [str(top_missing["column"])],
                "deep_analysis": (
                    f"{top_missing['column']} has {pct:.1%} missingness which is above the acceptable threshold. "
                    f"This often indicates a systemic data collection problem such as optional form fields, "
                    f"system integration failures, or manual data entry skips. "
                    f"The impact cascades to any KPI or model that depends on this column."
                ),
            })

        top_outlier = next((x for x in outliers if x.get("outlier_pct", 0) > 0.05), None)
        if top_outlier:
            pct = top_outlier["outlier_pct"]
            count = top_outlier["outlier_count"]
            lb = top_outlier.get("lower_bound", "N/A")
            ub = top_outlier.get("upper_bound", "N/A")
            insights.append({
                "id": str(uuid4()), "severity": "WARNING", "insight_type": "anomaly",
                "category": "anomaly",
                "title": f"Anomaly concentration in '{top_outlier['column']}' ({pct:.1%} flagged)",
                "explanation": (
                    f"{count:,} records ({pct:.1%}) in '{top_outlier['column']}' fall outside the expected range "
                    f"[{lb:.1f} – {ub:.1f}]. "
                    f"This rate signals process instability, data entry errors, or genuine business anomalies requiring investigation."
                ),
                "recommendation": (
                    f"1. Visualize '{top_outlier['column']}' distribution and flag the extreme records\n"
                    f"2. Review the {count} outlier records for common patterns (same date, region, user)\n"
                    f"3. Determine if outliers are errors (fix at source) or real signals (create alert rules)\n"
                    f"4. Set operational threshold: flag when outlier rate exceeds {max(0.03, pct/2):.1%}"
                ),
                "confidence": 0.84,
                "data_refs": [str(top_outlier["column"])],
                "deep_analysis": (
                    f"An outlier rate of {pct:.1%} in '{top_outlier['column']}' is statistically significant. "
                    f"Using the IQR method (1.5× IQR boundary), the expected range is [{lb:.1f} – {ub:.1f}]. "
                    f"Records outside this range should be reviewed for root cause — common causes include "
                    f"manual overrides, system errors, or genuine operational extremes."
                ),
            })

        # Duplicate rows
        dup_pct = float(overview.get("duplicate_pct", 0.0))
        if dup_pct > 0.02:
            dup_count = overview.get("duplicate_rows", 0)
            insights.append({
                "id": str(uuid4()), "severity": "WARNING", "insight_type": "data_quality",
                "category": "data_governance",
                "title": f"Duplicate records detected ({dup_pct:.1%} of dataset)",
                "explanation": (
                    f"{dup_count:,} duplicate rows ({dup_pct:.1%}) were found. "
                    f"Duplicates inflate counts, distort averages, and can double-count KPIs like revenue or events. "
                    f"This is a critical data governance issue that should be resolved before publishing reports."
                ),
                "recommendation": (
                    "1. Identify which columns define a unique record for this dataset\n"
                    "2. Apply deduplication using those composite keys\n"
                    "3. Investigate whether duplicates stem from ETL re-runs or source system issues\n"
                    "4. Add uniqueness constraints to the data pipeline"
                ),
                "confidence": 0.93,
                "data_refs": [],
                "deep_analysis": (
                    f"The {dup_pct:.1%} duplicate rate ({dup_count:,} rows) is a data governance red flag. "
                    f"In {domain} analytics, duplicates typically originate from ETL pipeline re-runs, "
                    f"multiple source system merges, or distributed insert operations without idempotency."
                ),
            })

        # Summary if nothing detected
        if not insights:
            # Pick the column with highest CV for a distribution-based insight
            best_cv_col = None
            best_cv = 0.0
            for nc in numeric_profile:
                mean = nc.get("mean", 0)
                std = nc.get("std", 0)
                if mean and std:
                    cv = std / abs(mean)
                    if cv > best_cv:
                        best_cv = cv
                        best_cv_col = nc["column"]

            col_mention = f" Consider inspecting '{best_cv_col}' which shows {best_cv:.1%} variability." if best_cv_col else ""
            insights.append({
                "id": str(uuid4()), "severity": "INFO", "insight_type": "positive",
                "category": "summary",
                "title": "Dataset quality baseline meets acceptable standards",
                "explanation": (
                    f"The {domain} dataset shows no critical data quality issues. "
                    f"{overview.get('rows', 0):,} rows and {overview.get('columns', 0)} columns were processed successfully."
                    f"{col_mention}"
                ),
                "recommendation": (
                    "1. Define business KPIs and segment the data by your most important dimension\n"
                    "2. Set up monitoring for data quality metrics\n"
                    "3. Build trend charts for the primary outcome variable"
                ),
                "confidence": 0.62,
                "data_refs": [best_cv_col] if best_cv_col else [],
                "deep_analysis": (
                    f"The overall data profile for this {domain} dataset is stable. "
                    f"No major anomalies, missingness, or duplicates exceed acceptable thresholds. "
                    f"The next step is deepening domain-specific KPI analysis rather than data remediation."
                ),
            })

        return insights[:8]

    def build_executive_storyline(self, *args, **kwargs) -> list[dict]:
        """Kept for backward compatibility — returns empty list (removed from UI)."""
        return []
