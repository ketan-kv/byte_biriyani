from __future__ import annotations
from agents.intent_agent import IntentAgent
from agents.research_agent import ResearchAgent
import json
import queue
from pathlib import Path

import pandas as pd

from agents.analysis_agent import AnalysisAgent
from agents.insight_agent import InsightAgent
from agents.structuring_agent import StructuringAgent
from pipelines.ingestion.ingestion_router import ingest
from storage.db import insert_many
from storage.parquet_store import append_sensor_data
from utils.logger import get_logger


logger = get_logger("amdais.orchestrator")


class OrchestratorAgent:
    def __init__(self, config: dict):
        self.config = config
        paths = config.get("paths", {})
        self.db_path = paths.get("sqlite_path", "data/structured/mineral_db.sqlite")
        self.sensor_parquet_path = paths.get("sensor_parquet_path", "data/structured/sensor_data.parquet")
        self.insights_path = Path(paths.get("insights_path", "data/insights/latest_insights.json"))

        self.event_queue: queue.Queue[dict] = queue.Queue()
        self.agents = {
            "structuring": StructuringAgent(),
            "analysis": AnalysisAgent(self.db_path, self.sensor_parquet_path),
            "insight": InsightAgent(),
            "intent": IntentAgent(),
            "research": ResearchAgent(),
        }

    def on_new_file(self, filepath: str, file_type: str) -> dict:
        logger.info("Processing file %s (%s)", filepath, file_type)
        raw_data = ingest(filepath, file_type)
        structured = self.agents["structuring"].run(raw_data, file_type)
        stored = self._store_structured(structured)
        self.event_queue.put({"event": "new_data", "type": file_type, "path": filepath})
        return {"stored": stored, "file_type": file_type, "path": filepath}

    def _store_structured(self, payload: dict) -> dict:
        result = {"db_rows": 0, "sensor_rows": 0}
        records = payload.get("records", [])
        table = payload.get("table")
        if table and records:
            result["db_rows"] = insert_many(table, records, db_path=self.db_path)

        sensor_df = payload.get("sensor_df")
        if sensor_df is not None and len(sensor_df) > 0:
            result["sensor_rows"] = append_sensor_data(sensor_df, parquet_path=self.sensor_parquet_path)
            self.agents["analysis"].update_anomaly_flag()

        return result

    def run_pipeline(self) -> dict:
        logger.info("Running full analytics pipeline")
        results = self.agents["analysis"].run_all()
        insights = self.agents["insight"].generate(results)
        self._write_insights(insights)
        return {"analysis": results, "insights": insights}

    def run_domain_pipeline(self, df: pd.DataFrame, user_preferences: dict | None = None) -> dict:
        # Domain-adaptive pipeline for any uploaded CSV/Excel.
        prefs = user_preferences or {}
        pipeline_logs: list[dict] = []

        pipeline_logs.append(
            {
                "step": "input",
                "agent": "orchestrator",
                "status": "ok",
                "details": {
                    "rows": int(df.shape[0]),
                    "columns": int(df.shape[1]),
                    "user_preferences": prefs,
                },
                "editable_controls": [
                    {
                        "name": "missing_strategy",
                        "type": "select",
                        "options": ["none", "mean", "median", "zero", "drop"],
                        "current": str(prefs.get("missing_strategy", "none")),
                    }
                ],
            }
        )

        # Step 1: detect domain
        domain, confidence = self.agents["intent"].detect_with_confidence(df)
        logger.info("Domain detected: %s (confidence: %.2f)", domain, confidence)
        pipeline_logs.append(
            {
                "step": "intent_detection",
                "agent": "intent_agent",
                "status": "ok",
                "details": {"domain": domain, "confidence": round(confidence, 2)},
            }
        )

        # Step 2: research domain knowledge
        from_cache = domain in self.agents["research"]._cache
        knowledge = self.agents["research"].research(domain, list(df.columns))
        knowledge_source = "cache" if from_cache else "llm"
        pipeline_logs.append(
            {
                "step": "domain_research",
                "agent": "research_agent",
                "status": "ok",
                "details": {
                    "knowledge_source": knowledge_source,
                    "kpis": len(knowledge.get("kpis", [])),
                    "analysis_priorities": len(knowledge.get("analysis_priorities", [])),
                },
            }
        )

        # Step 3: run analysis directly on uploaded dataset, then enrich context
        results = self.agents["analysis"].run_uploaded_dataset_analysis(df, user_preferences=prefs)
        results["domain_kpis"] = knowledge.get("kpis", [])
        results["domain_thresholds"] = knowledge.get("anomaly_thresholds", {})
        results["analysis_priorities"] = knowledge.get("analysis_priorities", [])
        pipeline_logs.append(
            {
                "step": "analysis",
                "agent": "analysis_agent",
                "status": "ok",
                "details": {
                    "overview": results.get("descriptive", {}).get("overview", {}),
                    "missing_strategy": results.get("descriptive", {}).get("data_prep", {}).get("missing_strategy"),
                },
            }
        )

        # Step 4: generate LLM insights
        vocab = knowledge.get("vocabulary", [])
        insights = self.agents["insight"].generate_with_llm(results, vocab, domain)
        pipeline_logs.append(
            {
                "step": "insight_generation",
                "agent": "insight_agent",
                "status": "ok",
                "details": {"insight_count": len(insights)},
            }
        )

        executive_storyline = self.agents["insight"].build_executive_storyline(results, insights, domain)
        pipeline_logs.append(
            {
                "step": "storyline",
                "agent": "insight_agent",
                "status": "ok",
                "details": {"storyline_items": len(executive_storyline)},
            }
        )

        # Step 5: save insights
        self._write_insights(insights)

        return {
            "domain": domain,
            "confidence": round(confidence, 2),
            "knowledge_source": knowledge_source,
            "kpis": knowledge.get("kpis", []),
            "analysis_priorities": knowledge.get("analysis_priorities", []),
            "analysis": results,
            "insights": insights,
            "executive_storyline": executive_storyline,
            "pipeline_logs": pipeline_logs,
            "applied_user_preferences": prefs,
        }

    def stream_domain_pipeline(
        self, df: pd.DataFrame, user_preferences: dict | None = None
    ):
        """Generator that yields SSE-ready event dicts for each pipeline stage.

        Designed to run in a background thread while the FastAPI SSE endpoint
        streams events to the browser via an asyncio.Queue bridge.
        """
        prefs = user_preferences or {}
        pipeline_logs: list[dict] = []

        yield {
            "stage": "input", "status": "processing",
            "details": {"rows": int(df.shape[0]), "columns": int(df.shape[1])},
            "summary": ["Reading uploaded dataset into memory..."],
        }

        # ── Step 1: domain detection ──────────────────────────────────────────
        domain, confidence = "unknown", 0.5
        try:
            domain, confidence = self.agents["intent"].detect_with_confidence(df)
            logger.info("Stream: domain=%s conf=%.2f", domain, confidence)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Stream: intent detection failed: %s", exc)
            yield {"stage": "intent_detection", "status": "error", "details": {"error": str(exc)}}

        domain_override = (prefs.get("domain_override") or "").strip().lower()
        if domain_override:
            domain = domain_override
            logger.info("Stream: domain overridden to '%s'", domain)

        pipeline_logs.append({
            "step": "intent_detection", "agent": "intent_agent", "status": "ok",
            "details": {"domain": domain, "confidence": round(confidence, 2)},
        })
        
        num_cols = len(df.select_dtypes(include="number").columns)
        cat_cols = len(df.select_dtypes(include="object").columns)
        yield {
            "stage": "intent_detection", "status": "ok",
            "details": {"domain": domain, "confidence": round(confidence, 2)},
            "summary": [
                f"Domain identified as {domain.title()} with {confidence:.0%} confidence.",
                f"{df.shape[1]} columns recognized: {num_cols} numeric, {cat_cols} categorical.",
            ]
        }

        # ── Step 2: domain research ───────────────────────────────────────────
        knowledge: dict = {}
        knowledge_source = "fallback"
        try:
            from_cache = domain in self.agents["research"]._cache
            knowledge = self.agents["research"].research(domain, list(df.columns))
            knowledge_source = "cache" if from_cache else "llm"
        except Exception as exc:  # noqa: BLE001
            logger.warning("Stream: research failed: %s", exc)

        pipeline_logs.append({
            "step": "domain_research", "agent": "research_agent", "status": "ok",
            "details": {"source": knowledge_source, "kpis": len(knowledge.get("kpis", []))},
        })
        
        kpis_count = len(knowledge.get("kpis", []))
        rules_count = len(knowledge.get("decision_rules", []))
        yield {
            "stage": "domain_research", "status": "ok",
            "details": {"source": knowledge_source, "kpis": kpis_count},
            "summary": [
                f"Retrieved expert knowledge pack for {domain.title()} ({knowledge_source}).",
                f"Loaded {kpis_count} industry KPIs and {rules_count} decision rules.",
                f"Loaded {len(knowledge.get('correlation_hypotheses', []))} correlation hypotheses.",
            ]
        }

        # ── Step 3: analysis ─────────────────────────────────────────────────
        results: dict = {"descriptive": {}, "diagnostic": {}, "predictive": {}}
        try:
            results = self.agents["analysis"].run_uploaded_dataset_analysis(df, user_preferences=prefs)
            results["domain_kpis"] = knowledge.get("kpis", [])
            results["domain_thresholds"] = knowledge.get("anomaly_thresholds", {})
            results["analysis_priorities"] = knowledge.get("analysis_priorities", [])
        except Exception as exc:  # noqa: BLE001
            logger.warning("Stream: analysis failed: %s", exc)

        pipeline_logs.append({
            "step": "analysis", "agent": "analysis_agent", "status": "ok",
            "details": {"overview": results.get("descriptive", {}).get("overview", {})},
        })
        
        prep = results.get("descriptive", {}).get("data_prep", {})
        missing_strat = prep.get("missing_strategy", "none")
        diag = results.get("diagnostic", {})
        top_corr = diag.get("correlation_top", [])
        corr_msg = f"Strongest correlation: {top_corr[0]['left']} ↔ {top_corr[0]['right']} (r={top_corr[0]['corr']:.2f})" if top_corr else "No strong correlations found."
        
        outliers = diag.get("outlier_scan", [])
        outlier_msg = f"Flagged {outliers[0]['outlier_count']:,} outliers in {outliers[0]['column']} ({outliers[0]['outlier_pct']:.1%})" if outliers else "No significant outliers detected."

        yield {
            "stage": "analysis", "status": "ok",
            "details": {"overview": results.get("descriptive", {}).get("overview", {})},
            "summary": [
                f"Missing-value strategy '{missing_strat}' applied.",
                outlier_msg,
                corr_msg,
                f"Processed {results.get('descriptive', {}).get('overview', {}).get('duplicate_rows', 0):,} duplicate rows.",
            ]
        }

        # ── Step 4: insight generation ────────────────────────────────────────
        vocab = knowledge.get("vocabulary", [])
        insight_count = int(prefs.get("insight_count", 6))
        min_severity = str(prefs.get("min_severity", "INFO")).upper()
        insights: list[dict] = []
        try:
            insights = self.agents["insight"].generate_with_llm(
                results, vocab, domain, count=insight_count
            )
            # Filter by minimum severity
            sev_order = {"INFO": 0, "WARNING": 1, "CRITICAL": 2}
            min_sev_val = sev_order.get(min_severity, 0)
            if min_sev_val > 0:
                insights = [
                    i for i in insights
                    if sev_order.get(str(i.get("severity", "INFO")).upper(), 0) >= min_sev_val
                ]
        except Exception as exc:  # noqa: BLE001
            logger.warning("Stream: insight generation failed: %s", exc)

        pipeline_logs.append({
            "step": "insight_generation", "agent": "insight_agent", "status": "ok",
            "details": {"count": len(insights)},
        })
        
        pos_count = len([i for i in insights if i.get("insight_type") in ["positive", "opportunity"]])
        crit_count = len([i for i in insights if i.get("severity", "").upper() == "CRITICAL"])
        warn_count = len([i for i in insights if i.get("severity", "").upper() == "WARNING"])
        
        yield {
            "stage": "insight_generation", "status": "ok",
            "details": {"count": len(insights)},
            "summary": [
                f"{len(insights)} actionable insights generated.",
                f"Found {pos_count} positive/opportunity signals.",
                f"Flagged {crit_count} critical risks and {warn_count} warnings.",
            ]
        }

        # ── Step 5: persist insights ──────────────────────────────────────────
        try:
            self._write_insights(insights)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Stream: failed to persist insights: %s", exc)

        # ── Final done event ──────────────────────────────────────────────────
        yield {
            "stage": "done",
            "status": "ok",
            "result": {
                "domain": domain,
                "confidence": round(confidence, 2),
                "knowledge_source": knowledge_source,
                "kpis": knowledge.get("kpis", []),
                "analysis_priorities": knowledge.get("analysis_priorities", []),
                "analysis": results,
                "insights": insights,
                "pipeline_logs": pipeline_logs,
                "rows": int(df.shape[0]),
            },
        }

    def watch_sensor_anomaly(self) -> list[dict]:
        flag = self.agents["analysis"].check_anomaly_flag()
        if not flag:
            return []
        urgent = self.agents["insight"].generate_urgent(flag)
        if urgent:
            self._write_insights(urgent, append=True)
        return urgent

    def _write_insights(self, insights: list[dict], append: bool = False) -> None:
        self.insights_path.parent.mkdir(parents=True, exist_ok=True)
        if append and self.insights_path.exists():
            old = json.loads(self.insights_path.read_text(encoding="utf-8"))
            data = old + insights
        else:
            data = insights
        self.insights_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
