from __future__ import annotations

import re
import threading
from collections import deque
from dataclasses import dataclass, field
from difflib import get_close_matches
from datetime import date, datetime
import os
from typing import Any

import ollama
import pandas as pd
import plotly.express as px


INTENT_DATA_QUERY = "DATA_QUERY"
INTENT_CHART_REQUEST = "CHART_REQUEST"
INTENT_INSIGHT_EXPLAIN = "INSIGHT_EXPLAIN"
INTENT_RECOMMENDATION = "RECOMMENDATION"
INTENT_PROOF = "PROOF"
INTENT_SIMULATION = "SIMULATION"
INTENT_GENERAL_QA = "GENERAL_QA"


def simulate_change(dataframe: pd.DataFrame, params: dict) -> pd.DataFrame:
    """Placeholder simulation hook: applies a percentage change on a target numeric column."""
    df = dataframe.copy()
    target_col = params.get("target_column")
    change_pct = float(params.get("change_pct", 0.0))
    if not target_col or target_col not in df.columns:
        return df
    if not pd.api.types.is_numeric_dtype(df[target_col]):
        return df
    df[target_col] = df[target_col].astype(float) * (1.0 + (change_pct / 100.0))
    return df


@dataclass
class SessionState:
    history: deque[dict] = field(default_factory=lambda: deque(maxlen=12))
    last_intent: str | None = None
    last_focus: dict[str, Any] = field(default_factory=dict)
    last_result: dict[str, Any] = field(default_factory=dict)


class DecisionCopilot:
    def __init__(self, max_context_rows: int = 200000) -> None:
        self.max_context_rows = max_context_rows
        self._lock = threading.RLock()
        self._latest_df: pd.DataFrame = pd.DataFrame()
        self._latest_analysis: dict[str, Any] = {}
        self._latest_insights: list[dict[str, Any]] = []
        self._latest_domain: str = "unknown"
        self._sessions: dict[str, SessionState] = {}

    def update_context(self, dataframe: pd.DataFrame, pipeline_result: dict | None = None) -> None:
        frame = dataframe.copy()
        if len(frame) > self.max_context_rows:
            frame = frame.head(self.max_context_rows).copy()
        frame.columns = [str(c).strip() for c in frame.columns]

        payload = pipeline_result or {}
        analysis = payload.get("analysis", {})
        insights = payload.get("insights", [])
        domain = str(payload.get("domain", "unknown"))

        with self._lock:
            self._latest_df = frame
            self._latest_analysis = analysis if isinstance(analysis, dict) else {}
            self._latest_insights = insights if isinstance(insights, list) else []
            self._latest_domain = domain

    def handle_message(self, message: str, session_id: str | None = None) -> dict[str, Any]:
        clean_message = str(message or "").strip()
        sid = str(session_id or "default")
        if not clean_message:
            return self._response(
                rtype="text",
                message="Please send a question to start the analysis chat.",
                intent=None,
                session_id=sid,
            )

        with self._lock:
            state = self._sessions.setdefault(sid, SessionState())
            df = self._latest_df
            analysis = self._latest_analysis
            insights = self._latest_insights

        if df.empty:
            return self._response(
                rtype="text",
                message="No dataset context is available yet. Run an analysis first, then ask questions.",
                intent=None,
                session_id=sid,
            )

        intent = self.classify_intent(clean_message, state)
        handlers = {
            INTENT_DATA_QUERY: self.handle_data_query,
            INTENT_CHART_REQUEST: self.generate_chart,
            INTENT_INSIGHT_EXPLAIN: self.explain_insight,
            INTENT_RECOMMENDATION: self.get_recommendations,
            INTENT_PROOF: self.generate_proof,
            INTENT_SIMULATION: self.handle_simulation,
            INTENT_GENERAL_QA: self.handle_general_qa,
        }
        handler = handlers.get(intent, self.handle_general_qa)
        result = handler(clean_message, df, analysis, insights, state)

        with self._lock:
            state.last_intent = intent
            state.last_result = result
            state.history.append({"role": "user", "message": clean_message, "intent": intent})
            state.history.append({"role": "assistant", "message": result.get("message", "")})

        result["intent"] = intent
        result["session_id"] = sid
        return result

    def classify_intent(self, message: str, state: SessionState) -> str:
        text = message.lower().strip()

        if any(k in text for k in ["what if", "simulate", "scenario", "if "]) and any(
            k in text for k in ["drop", "increase", "decrease", "%", "percent", "change"]
        ):
            return INTENT_SIMULATION

        if any(k in text for k in ["prove", "proof", "evidence", "show proof"]):
            return INTENT_PROOF

        if text in {"why that", "why this", "why?"}:
            return INTENT_INSIGHT_EXPLAIN

        # Removed generic why/explain fallback since the LLM can handle it better
        if "explain insight" in text:
            return INTENT_INSIGHT_EXPLAIN

        if any(k in text for k in ["recommend", "what next", "next step", "action"]):
            return INTENT_RECOMMENDATION

        chart_terms = ["chart", "plot", "graph", "visualize", "show trend", "show a trend", "draw"]
        if any(k in text for k in chart_terms):
            return INTENT_CHART_REQUEST

        if text in {"show chart for this", "chart for this", "plot this"}:
            return INTENT_CHART_REQUEST

        if any(
            k in text
            for k in [
                "top",
                "average",
                "mean",
                "sum",
                "total",
                "count",
                "how many",
                "number of",
                "above",
                "below",
                "greater than",
                "less than",
                "at least",
                "at most",
                "by ",
            ]
        ):
            return INTENT_DATA_QUERY

        return INTENT_GENERAL_QA

    def handle_general_qa(
        self,
        message: str,
        dataframe: pd.DataFrame,
        analysis: dict[str, Any],
        insights: list[dict[str, Any]],
        state: SessionState,
    ) -> dict[str, Any]:
        # Prioritize deterministic data answers over LLM guesses for quantitative prompts.
        if self._looks_like_data_question(message):
            return self.handle_data_query(message, dataframe, analysis, insights, state)

        model = os.getenv("OLLAMA_MODEL", "llama3:latest")
        host = os.getenv("OLLAMA_HOST")
        api_key = os.getenv("OLLAMA_API_KEY")
        
        client_kwargs = {"host": host} if host else {}
        if api_key:
            client_kwargs["headers"] = {"Authorization": f"Bearer {api_key}"}
        
        try:
            client = ollama.Client(**client_kwargs)
            history_msgs = []
            for item in list(state.history)[-4:]: # Limit history to last 4 to save context
                history_msgs.append({"role": item["role"], "content": item["message"]})
                
            cols = list(dataframe.columns) if not dataframe.empty else "No dataset available"
            
            system_prompt = (
                "You are AMDAIS Assistant, a helpful AI powered by Llama 3. "
                "You can answer general questions as well as questions about the loaded dataset. "
                f"Data Columns available: {cols}\n"
                "If the user asks a general question, answer it. "
                "If they ask about data, use the columns above as context and do not fabricate counts. "
                "Never use placeholders like [insert number] or [insert percentage]. "
                "Keep your answers concise and useful."
            )
            
            messages = [{"role": "system", "content": system_prompt}] + history_msgs + [{"role": "user", "content": message}]
            
            response = client.chat(model=model, messages=messages)
            llm_text = response.get("message", {}).get("content", "Sorry, I could not generate a response.")
            
            return self._response(
                rtype="text",
                message=llm_text
            )
        except Exception as e:
            return self._response(rtype="text", message=f"Ollama Llama3 Error: {str(e)}")

    def handle_data_query(
        self,
        message: str,
        dataframe: pd.DataFrame,
        _analysis: dict[str, Any],
        _insights: list[dict[str, Any]],
        state: SessionState,
    ) -> dict[str, Any]:
        text = message.lower()
        row_count = int(len(dataframe))
        wants_total = any(k in text for k in ["total", "how many", "count", "number of", "all matches", "all rows"])

        threshold_match = re.search(
            r"(>=|<=|>|<|above|below|greater than|less than|more than|under|at least|at most)\s*(-?\d+(?:\.\d+)?)",
            text,
        )
        if threshold_match:
            operator = threshold_match.group(1)
            threshold = float(threshold_match.group(2))

            prefix = text[: threshold_match.start()]
            metric_col = self._match_column(prefix, dataframe, numeric_only=True) or self._match_column(
                text, dataframe, numeric_only=True
            )
            if not metric_col:
                metric_col = self._default_numeric_column(dataframe)

            if metric_col:
                values = pd.to_numeric(dataframe[metric_col], errors="coerce")
                valid = values.notna()
                if operator in {">", "above", "greater than", "more than"}:
                    mask = valid & (values > threshold)
                    op_label = ">"
                elif operator in {">=", "at least"}:
                    mask = valid & (values >= threshold)
                    op_label = ">="
                elif operator in {"<", "below", "less than", "under"}:
                    mask = valid & (values < threshold)
                    op_label = "<"
                else:
                    mask = valid & (values <= threshold)
                    op_label = "<="

                match_count = int(mask.sum())
                pct_all = (match_count / row_count * 100.0) if row_count else 0.0
                valid_count = int(valid.sum())
                pct_valid = (match_count / valid_count * 100.0) if valid_count else 0.0

                if wants_total:
                    msg = (
                        f"There are {row_count} total rows/matches. "
                        f"{match_count} rows have {metric_col} {op_label} {threshold:g} "
                        f"({pct_all:.2f}% of all rows; {pct_valid:.2f}% of rows with valid {metric_col})."
                    )
                else:
                    msg = (
                        f"{match_count} rows have {metric_col} {op_label} {threshold:g} "
                        f"({pct_all:.2f}% of all rows)."
                    )

                data = {
                    "row_count": row_count,
                    "metric": metric_col,
                    "operator": op_label,
                    "threshold": threshold,
                    "match_count": match_count,
                    "match_pct_of_all": round(pct_all, 4),
                    "match_pct_of_valid_metric_rows": round(pct_valid, 4),
                    "valid_metric_rows": valid_count,
                }
                state.last_focus = {"summary": data}
                return self._response(rtype="text", message=msg, data=data)

        if wants_total and not threshold_match:
            summary = {
                "row_count": row_count,
                "column_count": int(dataframe.shape[1]),
            }
            state.last_focus = {"summary": summary}
            return self._response(
                rtype="text",
                message=f"The loaded dataset contains {row_count} total rows/matches.",
                data=summary,
            )

        numeric_cols = [c for c in dataframe.columns if pd.api.types.is_numeric_dtype(dataframe[c])]
        if not numeric_cols:
            return self._response(
                rtype="text",
                message="I could not find numeric columns for advanced aggregations, but I can still answer row counts.",
                data={"row_count": row_count, "column_count": int(dataframe.shape[1])},
            )

        avg_match = re.search(r"(?:average|mean)\s+(.+?)\s+by\s+(.+)", text)
        if avg_match:
            metric_col = self._match_column(avg_match.group(1), dataframe, numeric_only=True)
            group_col = self._match_column(avg_match.group(2), dataframe, categorical_only=True)
            if metric_col and group_col:
                table = (
                    dataframe[[group_col, metric_col]]
                    .dropna(subset=[metric_col])
                    .groupby(group_col, dropna=False)[metric_col]
                    .mean()
                    .reset_index(name=f"avg_{metric_col}")
                    .sort_values(f"avg_{metric_col}", ascending=False)
                )
                rows = table.head(20).to_dict(orient="records")
                state.last_focus = {"metric": metric_col, "group": group_col, "query_rows": rows}
                return self._response(
                    rtype="text",
                    message=f"Average {metric_col} by {group_col} computed from {len(table)} groups.",
                    data={"columns": list(table.columns), "rows": rows},
                )

        count_match = re.search(r"count\s+by\s+(.+)", text)
        if count_match:
            group_col = self._match_column(count_match.group(1), dataframe)
            if group_col:
                table = (
                    dataframe[group_col]
                    .fillna("UNKNOWN")
                    .astype(str)
                    .value_counts()
                    .reset_index()
                )
                table.columns = [group_col, "count"]
                rows = table.head(20).to_dict(orient="records")
                state.last_focus = {"group": group_col, "query_rows": rows}
                return self._response(
                    rtype="text",
                    message=f"Count by {group_col} ready.",
                    data={"columns": list(table.columns), "rows": rows},
                )

        top_match = re.search(r"top\s+(\d+)", text)
        if top_match:
            top_n = int(top_match.group(1))
            risk_metric = self._select_risk_metric(dataframe)
            group_hint = self._extract_group_hint(text)
            group_col = self._match_column(group_hint, dataframe, categorical_only=True) if group_hint else None
            if group_col and risk_metric:
                table = (
                    dataframe[[group_col, risk_metric]]
                    .dropna(subset=[risk_metric])
                    .groupby(group_col, dropna=False)[risk_metric]
                    .mean()
                    .reset_index(name=f"risk_score_{risk_metric}")
                    .sort_values(f"risk_score_{risk_metric}", ascending=False)
                    .head(top_n)
                )
                rows = table.to_dict(orient="records")
                state.last_focus = {
                    "metric": risk_metric,
                    "group": group_col,
                    "query_rows": rows,
                    "top_n": top_n,
                }
                return self._response(
                    rtype="text",
                    message=f"Top {top_n} risky {group_col} groups ranked by {risk_metric}.",
                    data={"columns": list(table.columns), "rows": rows},
                )

        summary_metric = numeric_cols[0]
        summary = {
            "row_count": int(len(dataframe)),
            "column_count": int(dataframe.shape[1]),
            "primary_metric": summary_metric,
            "primary_metric_mean": float(dataframe[summary_metric].dropna().mean()),
            "primary_metric_std": float(dataframe[summary_metric].dropna().std(ddof=0) or 0.0),
        }
        state.last_focus = {"summary": summary}
        return self._response(
            rtype="text",
            message="I used a generic summary because I could not map the query to a specific aggregation.",
            data=summary,
        )

    def _looks_like_data_question(self, message: str) -> bool:
        text = str(message or "").lower()
        data_tokens = [
            "dataset",
            "data",
            "rows",
            "row",
            "records",
            "record",
            "matches",
            "match",
            "count",
            "how many",
            "number of",
            "total",
            "average",
            "mean",
            "sum",
            "above",
            "below",
            "greater than",
            "less than",
            "at least",
            "at most",
            "percent",
            "percentage",
        ]
        return any(token in text for token in data_tokens)

    def generate_chart(
        self,
        message: str,
        dataframe: pd.DataFrame,
        _analysis: dict[str, Any],
        _insights: list[dict[str, Any]],
        state: SessionState,
    ) -> dict[str, Any]:
        text = message.lower()
        chart_type = self._infer_chart_type(text)

        if "chart for this" in text or "plot this" in text:
            prior_rows = state.last_focus.get("query_rows", []) if state.last_focus else []
            if prior_rows:
                chart = self._chart_from_rows(prior_rows)
                if chart is not None:
                    return self._response(
                        rtype="chart",
                        message="Built a chart from the previous query result.",
                        chart=chart,
                        data={"source": "previous_query"},
                    )

        x_col, y_col = self._infer_xy_columns(text, dataframe)

        try:
            fig = None
            if chart_type == "line":
                date_col = x_col or self._find_date_column(dataframe)
                metric_col = y_col or self._default_numeric_column(dataframe)
                if not date_col or not metric_col:
                    return self._response(
                        rtype="text",
                        message="I need a date-like column and numeric metric to generate a trend chart.",
                    )
                series = dataframe[[date_col, metric_col]].copy()
                series[date_col] = pd.to_datetime(series[date_col], errors="coerce")
                series = series.dropna(subset=[date_col, metric_col])
                if series.empty:
                    return self._response(rtype="text", message="No valid rows were found for the trend chart.")
                series["period"] = series[date_col].dt.to_period("M").astype(str)
                chart_df = series.groupby("period", as_index=False)[metric_col].mean()
                fig = px.line(chart_df, x="period", y=metric_col, markers=True, title=f"Trend of {metric_col}")
            elif chart_type == "scatter":
                if not x_col or not y_col:
                    return self._response(rtype="text", message="Please specify two numeric columns for scatter, e.g. x vs y.")
                plot_df = dataframe[[x_col, y_col]].dropna().head(5000)
                fig = px.scatter(plot_df, x=x_col, y=y_col, title=f"{y_col} vs {x_col}")
            elif chart_type == "histogram":
                metric = y_col or x_col or self._default_numeric_column(dataframe)
                if not metric:
                    return self._response(rtype="text", message="No numeric metric found for histogram.")
                plot_df = dataframe[[metric]].dropna().head(8000)
                fig = px.histogram(plot_df, x=metric, nbins=30, title=f"Distribution of {metric}")
            elif chart_type == "pie":
                group_col = x_col or self._default_categorical_column(dataframe)
                if not group_col:
                    return self._response(rtype="text", message="No categorical column found for pie chart.")
                counts = dataframe[group_col].fillna("UNKNOWN").astype(str).value_counts().head(12).reset_index()
                counts.columns = [group_col, "count"]
                fig = px.pie(counts, names=group_col, values="count", title=f"Share by {group_col}")
            else:
                group_col = x_col or self._default_categorical_column(dataframe)
                metric = y_col or self._default_numeric_column(dataframe)
                if not group_col or not metric:
                    return self._response(
                        rtype="text",
                        message="I need one categorical and one numeric column for a bar chart request.",
                    )
                chart_df = (
                    dataframe[[group_col, metric]]
                    .dropna(subset=[metric])
                    .groupby(group_col, dropna=False)[metric]
                    .mean()
                    .reset_index(name=f"avg_{metric}")
                    .sort_values(f"avg_{metric}", ascending=False)
                    .head(15)
                )
                fig = px.bar(chart_df, x=group_col, y=f"avg_{metric}", title=f"{metric} by {group_col}")

            if fig is None:
                return self._response(rtype="text", message="I could not generate a chart for this request.")

            state.last_focus = {"chart_type": chart_type, "x_col": x_col, "y_col": y_col}
            return self._response(
                rtype="chart",
                message=f"Generated a {chart_type} chart based on your request.",
                chart=fig.to_plotly_json(),
                data={"chart_type": chart_type, "x": x_col, "y": y_col},
            )
        except Exception as exc:
            return self._response(rtype="text", message=f"Chart generation failed: {exc}")

    def explain_insight(
        self,
        message: str,
        dataframe: pd.DataFrame,
        analysis: dict[str, Any],
        insights: list[dict[str, Any]],
        state: SessionState,
    ) -> dict[str, Any]:
        insight = self._pick_insight(message, insights, state)
        if not insight:
            return self._response(
                rtype="text",
                message="No insight is available yet. Run analysis first so I can explain the reasoning.",
            )

        evidence = self._collect_evidence(insight, dataframe, analysis)
        body = {
            "insight": insight.get("title", "Insight"),
            "evidence": evidence,
            "reason": insight.get("explanation", ""),
            "action": insight.get("recommendation", ""),
        }
        state.last_focus = {
            "insight_title": insight.get("title"),
            "data_refs": insight.get("data_refs", []),
        }
        return self._response(
            rtype="text",
            message=(
                f"Insight: {body['insight']}\n"
                f"Evidence: {', '.join(evidence.get('highlights', [])) or 'No strong evidence signals found.'}\n"
                f"Reason: {body['reason']}\n"
                f"Action: {body['action']}"
            ),
            data=body,
        )

    def get_recommendations(
        self,
        _message: str,
        _dataframe: pd.DataFrame,
        analysis: dict[str, Any],
        insights: list[dict[str, Any]],
        _state: SessionState,
    ) -> dict[str, Any]:
        severity_rank = {"CRITICAL": 3, "WARNING": 2, "INFO": 1}
        ranked = sorted(
            [i for i in insights if isinstance(i, dict)],
            key=lambda x: (severity_rank.get(str(x.get("severity", "INFO")).upper(), 1), float(x.get("confidence", 0.0))),
            reverse=True,
        )

        actions: list[dict[str, str]] = []
        for item in ranked:
            if len(actions) >= 3:
                break
            confidence = float(item.get("confidence", 0.0))
            severity = str(item.get("severity", "INFO")).upper()
            impact = "high" if severity == "CRITICAL" or confidence >= 0.8 else "medium" if confidence >= 0.6 else "low"
            actions.append(
                {
                    "description": str(item.get("recommendation", "Review this area.")),
                    "reasoning": str(item.get("explanation", "Recommendation inferred from insight signals.")),
                    "expected_impact": impact,
                }
            )

        predictive_actions = analysis.get("predictive", {}).get("action_plan", [])
        for step in predictive_actions:
            if len(actions) >= 3:
                break
            actions.append(
                {
                    "description": str(step),
                    "reasoning": "Derived from predictive risk signals and profiling outputs.",
                    "expected_impact": "medium",
                }
            )

        if not actions:
            actions = [
                {
                    "description": "Review data quality hotspots before downstream decisions.",
                    "reasoning": "No explicit recommendation objects were available in the latest run.",
                    "expected_impact": "medium",
                }
            ]

        return self._response(
            rtype="text",
            message="Top next actions are ready.",
            actions=actions,
            data={"action_count": len(actions)},
        )

    def generate_proof(
        self,
        message: str,
        dataframe: pd.DataFrame,
        analysis: dict[str, Any],
        insights: list[dict[str, Any]],
        state: SessionState,
    ) -> dict[str, Any]:
        insight = self._pick_insight(message, insights, state)
        if not insight:
            return self._response(rtype="text", message="I could not identify which insight to prove.")

        evidence = self._collect_evidence(insight, dataframe, analysis)
        proof_chart = self._build_evidence_chart(dataframe, insight)
        response_type = "mixed" if proof_chart is not None else "text"
        return self._response(
            rtype=response_type,
            message=f"Proof package generated for: {insight.get('title', 'insight')}",
            data={
                "insight": insight.get("title"),
                "evidence": evidence,
                "supporting_stats": evidence.get("stats", {}),
            },
            chart=proof_chart,
        )

    def handle_simulation(
        self,
        message: str,
        dataframe: pd.DataFrame,
        _analysis: dict[str, Any],
        insights: list[dict[str, Any]],
        state: SessionState,
    ) -> dict[str, Any]:
        params = self._parse_simulation_params(message, dataframe)
        if not params:
            return self._response(
                rtype="text",
                message="I detected a simulation request but could not parse target column or change percentage.",
            )

        before = dataframe[params["target_column"]].dropna().astype(float)
        simulated = simulate_change(dataframe, params)
        after = simulated[params["target_column"]].dropna().astype(float)

        sim_insights = self._simulate_insight_delta(before, after, params, insights)
        comparison = pd.DataFrame(
            {
                "scenario": ["current", "simulated"],
                "mean": [float(before.mean()), float(after.mean())],
                "median": [float(before.median()), float(after.median())],
            }
        )
        fig = px.bar(comparison, x="scenario", y="mean", title=f"Simulation impact on {params['target_column']} (mean)")
        state.last_focus = {"simulation": params, "simulated_summary": comparison.to_dict(orient="records")}

        return self._response(
            rtype="mixed",
            message=(
                f"Simulation complete: {params['target_column']} changed by {params['change_pct']}%. "
                "Updated insight summary generated."
            ),
            data={
                "simulation": params,
                "summary": comparison.to_dict(orient="records"),
                "updated_insights": sim_insights,
            },
            chart=fig.to_plotly_json(),
        )

    def _response(
        self,
        rtype: str,
        message: str,
        data: dict[str, Any] | None = None,
        chart: dict[str, Any] | None = None,
        actions: list[dict[str, Any]] | None = None,
        intent: str | None = None,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        return {
            "type": rtype,
            "message": message,
            "data": self._json_safe(data or {}),
            "chart": self._json_safe(chart) if chart is not None else None,
            "actions": self._json_safe(actions or []),
            "intent": intent,
            "session_id": session_id,
        }

    def _json_safe(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, dict):
            return {str(k): self._json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._json_safe(v) for v in value]

        tolist = getattr(value, "tolist", None)
        if callable(tolist):
            return self._json_safe(tolist())

        item = getattr(value, "item", None)
        if callable(item):
            try:
                return self._json_safe(item())
            except Exception:
                pass

        return str(value)

    def _normalize(self, value: str) -> str:
        return re.sub(r"[^a-z0-9]", "", value.lower())

    def _match_column(
        self,
        phrase: str,
        dataframe: pd.DataFrame,
        numeric_only: bool = False,
        categorical_only: bool = False,
    ) -> str | None:
        cols = list(dataframe.columns)
        if numeric_only:
            cols = [c for c in cols if pd.api.types.is_numeric_dtype(dataframe[c])]
        if categorical_only:
            cols = [c for c in cols if not pd.api.types.is_numeric_dtype(dataframe[c])]
        if not cols:
            return None

        norm_phrase = self._normalize(phrase)
        if not norm_phrase:
            return None

        exact = {self._normalize(c): c for c in cols}
        if norm_phrase in exact:
            return exact[norm_phrase]

        contains = [c for c in cols if self._normalize(c) in norm_phrase or norm_phrase in self._normalize(c)]
        if contains:
            return sorted(contains, key=lambda c: len(self._normalize(c)), reverse=True)[0]

        choices = list(exact.keys())
        close = get_close_matches(norm_phrase, choices, n=1, cutoff=0.55)
        if close:
            return exact[close[0]]

        for token in re.findall(r"[a-zA-Z0-9_]+", phrase):
            token_norm = self._normalize(token)
            if token_norm in exact:
                return exact[token_norm]
        return None

    def _select_risk_metric(self, dataframe: pd.DataFrame) -> str | None:
        numeric_cols = [c for c in dataframe.columns if pd.api.types.is_numeric_dtype(dataframe[c])]
        if not numeric_cols:
            return None
        priority_keywords = ["risk", "anomaly", "failure", "downtime", "loss", "defect", "error", "delay"]
        for col in numeric_cols:
            norm = col.lower()
            if any(k in norm for k in priority_keywords):
                return col
        return numeric_cols[0]

    def _extract_group_hint(self, text: str) -> str | None:
        cleaned = text
        for word in ["top", "risky", "risk", "regions", "region", "areas", "area"]:
            cleaned = cleaned.replace(word, " ")
        tokens = [t for t in re.findall(r"[a-zA-Z0-9_]+", cleaned) if not t.isdigit()]
        if not tokens:
            return None
        return " ".join(tokens[-2:]) if len(tokens) >= 2 else tokens[0]

    def _infer_chart_type(self, text: str) -> str:
        if any(k in text for k in ["trend", "line"]):
            return "line"
        if "scatter" in text or " vs " in text:
            return "scatter"
        if any(k in text for k in ["hist", "distribution"]):
            return "histogram"
        if "pie" in text:
            return "pie"
        return "bar"

    def _infer_xy_columns(self, text: str, dataframe: pd.DataFrame) -> tuple[str | None, str | None]:
        scatter = re.search(r"(.+?)\s+vs\s+(.+)", text)
        if scatter:
            left = self._match_column(scatter.group(1), dataframe, numeric_only=True)
            right = self._match_column(scatter.group(2), dataframe, numeric_only=True)
            return left, right

        by_clause = re.search(r"(.+?)\s+by\s+(.+)", text)
        if by_clause:
            y_col = self._match_column(by_clause.group(1), dataframe, numeric_only=True)
            x_col = self._match_column(by_clause.group(2), dataframe)
            return x_col, y_col

        return None, None

    def _find_date_column(self, dataframe: pd.DataFrame) -> str | None:
        for col in dataframe.columns:
            series = dataframe[col]
            if pd.api.types.is_datetime64_any_dtype(series):
                return col
            if series.dtype == "object":
                sample = series.dropna().astype(str).head(150)
                if sample.empty:
                    continue
                looks_date = sample.str.contains(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", regex=True).mean()
                if float(looks_date) >= 0.65:
                    return col
        return None

    def _default_numeric_column(self, dataframe: pd.DataFrame) -> str | None:
        cols = [c for c in dataframe.columns if pd.api.types.is_numeric_dtype(dataframe[c])]
        return cols[0] if cols else None

    def _default_categorical_column(self, dataframe: pd.DataFrame) -> str | None:
        cols = [c for c in dataframe.columns if not pd.api.types.is_numeric_dtype(dataframe[c])]
        return cols[0] if cols else None

    def _chart_from_rows(self, rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not rows:
            return None
        frame = pd.DataFrame(rows)
        if frame.empty or frame.shape[1] < 2:
            return None
        x_col = frame.columns[0]
        y_candidates = [c for c in frame.columns[1:] if pd.api.types.is_numeric_dtype(frame[c])]
        if not y_candidates:
            return None
        y_col = y_candidates[0]
        fig = px.bar(frame.head(20), x=x_col, y=y_col, title="Chart from previous result")
        return fig.to_plotly_json()

    def _pick_insight(
        self,
        message: str,
        insights: list[dict[str, Any]],
        state: SessionState,
    ) -> dict[str, Any] | None:
        if not insights:
            return None
        text = message.lower()

        if any(k in text for k in ["that", "this", "same"]) and state.last_focus.get("insight_title"):
            target = state.last_focus.get("insight_title")
            for insight in insights:
                if str(insight.get("title")) == target:
                    return insight

        for insight in insights:
            title = str(insight.get("title", "")).lower()
            refs = " ".join([str(r).lower() for r in insight.get("data_refs", [])])
            if any(token in title or token in refs for token in re.findall(r"[a-z0-9_]+", text)):
                return insight

        severity_rank = {"CRITICAL": 3, "WARNING": 2, "INFO": 1}
        return sorted(
            insights,
            key=lambda i: (
                severity_rank.get(str(i.get("severity", "INFO")).upper(), 1),
                float(i.get("confidence", 0.0)),
            ),
            reverse=True,
        )[0]

    def _collect_evidence(
        self,
        insight: dict[str, Any],
        dataframe: pd.DataFrame,
        analysis: dict[str, Any],
    ) -> dict[str, Any]:
        refs = [str(r) for r in insight.get("data_refs", [])]
        highlights: list[str] = []
        stats: dict[str, Any] = {}

        missing = analysis.get("diagnostic", {}).get("missingness", [])
        outliers = analysis.get("diagnostic", {}).get("outlier_scan", [])
        correlations = analysis.get("diagnostic", {}).get("correlation_top", [])

        for ref in refs:
            if ref in dataframe.columns:
                series = dataframe[ref]
                if pd.api.types.is_numeric_dtype(series):
                    clean = series.dropna().astype(float)
                    if not clean.empty:
                        stats[ref] = {
                            "mean": float(clean.mean()),
                            "median": float(clean.median()),
                            "min": float(clean.min()),
                            "max": float(clean.max()),
                        }
                        highlights.append(f"{ref} mean={clean.mean():.3f}, median={clean.median():.3f}")
                else:
                    top = series.fillna("UNKNOWN").astype(str).value_counts().head(3).to_dict()
                    stats[ref] = {"top_values": top}
                    highlights.append(f"{ref} top values include {', '.join(list(top.keys())[:2])}")

        for row in missing:
            col = str(row.get("column", ""))
            if col in refs and float(row.get("missing_pct", 0.0)) > 0:
                highlights.append(f"{col} missing={float(row.get('missing_pct', 0.0)):.1%}")

        for row in outliers:
            col = str(row.get("column", ""))
            if col in refs and float(row.get("outlier_pct", 0.0)) > 0:
                highlights.append(f"{col} outlier share={float(row.get('outlier_pct', 0.0)):.1%}")

        for row in correlations:
            left = str(row.get("left", ""))
            right = str(row.get("right", ""))
            if left in refs or right in refs:
                highlights.append(f"correlation({left}, {right})={float(row.get('corr', 0.0)):.2f}")

        return {"highlights": highlights[:8], "stats": stats}

    def _build_evidence_chart(self, dataframe: pd.DataFrame, insight: dict[str, Any]) -> dict[str, Any] | None:
        refs = [str(r) for r in insight.get("data_refs", []) if str(r) in dataframe.columns]
        numeric_refs = [r for r in refs if pd.api.types.is_numeric_dtype(dataframe[r])]
        categorical_refs = [r for r in refs if r not in numeric_refs]

        if len(numeric_refs) >= 2:
            plot_df = dataframe[[numeric_refs[0], numeric_refs[1]]].dropna().head(2500)
            if plot_df.empty:
                return None
            fig = px.scatter(plot_df, x=numeric_refs[0], y=numeric_refs[1], title="Evidence relationship chart")
            return fig.to_plotly_json()

        if len(numeric_refs) == 1:
            plot_df = dataframe[[numeric_refs[0]]].dropna().head(8000)
            if plot_df.empty:
                return None
            fig = px.histogram(plot_df, x=numeric_refs[0], nbins=30, title="Evidence distribution chart")
            return fig.to_plotly_json()

        if categorical_refs:
            col = categorical_refs[0]
            counts = dataframe[col].fillna("UNKNOWN").astype(str).value_counts().head(12).reset_index()
            counts.columns = [col, "count"]
            fig = px.bar(counts, x=col, y="count", title="Evidence category chart")
            return fig.to_plotly_json()

        return None

    def _parse_simulation_params(self, message: str, dataframe: pd.DataFrame) -> dict[str, Any] | None:
        text = message.lower()
        pct_match = re.search(r"(\d+(?:\.\d+)?)\s*%", text)
        if not pct_match:
            return None
        pct = float(pct_match.group(1))

        is_negative = any(k in text for k in ["drop", "decrease", "down", "reduce"])
        if is_negative:
            pct = -abs(pct)

        target_hint = re.sub(r"what if", "", text)
        target_hint = re.sub(r"(drops?|increase|decrease|by|percent|%)", " ", target_hint)
        target_col = self._match_column(target_hint, dataframe, numeric_only=True)
        if not target_col:
            target_col = self._default_numeric_column(dataframe)
        if not target_col:
            return None

        return {"target_column": target_col, "change_pct": pct}

    def _simulate_insight_delta(
        self,
        before: pd.Series,
        after: pd.Series,
        params: dict[str, Any],
        existing_insights: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if before.empty or after.empty:
            return []
        before_mean = float(before.mean())
        after_mean = float(after.mean())
        delta_pct = 0.0 if before_mean == 0 else ((after_mean - before_mean) / abs(before_mean)) * 100.0

        impact = "WARNING" if abs(delta_pct) > 8 else "INFO"
        direction = "decreased" if delta_pct < 0 else "increased"
        primary = {
            "severity": impact,
            "title": f"Simulation impact on {params['target_column']}",
            "explanation": f"Mean value {direction} by {abs(delta_pct):.2f}% after simulation.",
            "recommendation": "Validate scenario assumptions before decision rollout.",
            "confidence": 0.7,
            "data_refs": [params["target_column"]],
        }

        output = [primary]
        if existing_insights:
            output.append(
                {
                    "severity": "INFO",
                    "title": "Baseline reference",
                    "explanation": str(existing_insights[0].get("title", "Existing top insight")),
                    "recommendation": "Compare baseline and simulated outcomes in decision review.",
                    "confidence": 0.6,
                    "data_refs": [],
                }
            )
        return output