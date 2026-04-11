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

    def run_uploaded_dataset_analysis(self, df: pd.DataFrame, user_preferences: dict | None = None) -> dict:
        prefs = user_preferences or {}
        missing_strategy = str(prefs.get("missing_strategy", "none")).lower()
        frame = df.copy()
        frame.columns = [str(c).strip().lower().replace(" ", "_") for c in frame.columns]

        datetime_cols: list[str] = []
        for col in frame.columns:
            if pd.api.types.is_datetime64_any_dtype(frame[col]):
                datetime_cols.append(col)
                continue
            if frame[col].dtype == "object":
                sample = frame[col].dropna().astype(str).head(200)
                if sample.empty:
                    continue
                looks_like_date = sample.str.contains(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", regex=True).mean()
                if float(looks_like_date) < 0.6:
                    continue
                parsed = pd.to_datetime(frame[col], errors="coerce", utc=True, format="mixed")
                if float(parsed.notna().mean()) >= 0.8:
                    frame[col] = parsed
                    datetime_cols.append(col)

        numeric_cols = frame.select_dtypes(include="number").columns.tolist()
        categorical_cols = [
            c for c in frame.columns if c not in numeric_cols and c not in datetime_cols
        ]

        missing_before = int(frame[numeric_cols].isna().sum().sum()) if numeric_cols else 0
        rows_before = int(len(frame))

        if numeric_cols:
            if missing_strategy == "mean":
                for col in numeric_cols:
                    if frame[col].isna().any():
                        frame[col] = frame[col].fillna(frame[col].mean())
            elif missing_strategy == "median":
                for col in numeric_cols:
                    if frame[col].isna().any():
                        frame[col] = frame[col].fillna(frame[col].median())
            elif missing_strategy == "zero":
                frame[numeric_cols] = frame[numeric_cols].fillna(0)
            elif missing_strategy == "drop":
                frame = frame.dropna(subset=numeric_cols)

        rows_after = int(len(frame))
        missing_after = int(frame[numeric_cols].isna().sum().sum()) if numeric_cols else 0

        numeric_profile: list[dict] = []
        for col in numeric_cols:
            s = frame[col].dropna()
            if s.empty:
                continue
            numeric_profile.append(
                {
                    "column": col,
                    "count": int(len(s)),
                    "min": float(s.min()),
                    "p25": float(s.quantile(0.25)),
                    "median": float(s.median()),
                    "p75": float(s.quantile(0.75)),
                    "max": float(s.max()),
                    "mean": float(s.mean()),
                    "std": float(s.std(ddof=0)) if len(s) > 1 else 0.0,
                }
            )

        # ── Pick the primary metric column ─────────────────────────────────
        # Avoid ID-like columns (high cardinality relative to row count, or named id/key/index)
        _ID_HINTS = {"id", "key", "index", "num", "code", "uuid", "hash", "pk", "fk", "seq", "row"}

        def _is_id_like(col: str, s: pd.Series) -> bool:
            col_lower = col.lower()
            if any(hint in col_lower for hint in _ID_HINTS):
                return True
            # High uniqueness relative to total rows → likely an ID
            if s.nunique() / max(len(s), 1) > 0.92:
                return True
            return False

        def _cv(stat: dict) -> float:
            """Coefficient of variation — higher means more interesting spread."""
            mean = stat.get("mean", 0)
            std = stat.get("std", 0)
            return (std / abs(mean)) if mean else 0.0

        metric_col: str | None = None
        best_cv = -1.0
        for stat in numeric_profile:
            col = stat["column"]
            if _is_id_like(col, frame[col]):
                continue
            cv = _cv(stat)
            if cv > best_cv:
                best_cv = cv
                metric_col = col
        # Fall back to highest-std non-ID column if CV approach gave nothing
        if metric_col is None and numeric_profile:
            candidates = [s for s in numeric_profile if not _is_id_like(s["column"], frame[s["column"]])]
            if candidates:
                metric_col = max(candidates, key=lambda x: x.get("std", 0.0))["column"]

        categorical_profile: list[dict] = []
        for col in categorical_cols[:20]:
            s = frame[col].fillna("UNKNOWN").astype(str)
            vc = s.value_counts(dropna=False)
            top = vc.head(6)
            categorical_profile.append(
                {
                    "column": col,
                    "unique_count": int(s.nunique(dropna=False)),
                    "top_values": [
                        {"value": str(v), "count": int(c)} for v, c in top.items()
                    ],
                }
            )

        missingness = (
            (frame.isna().sum() / max(len(frame), 1))
            .sort_values(ascending=False)
            .round(6)
        )
        missingness_rows = [
            {"column": str(col), "missing_pct": float(pct)} for col, pct in missingness.items()
        ]

        outlier_scan: list[dict] = []
        for col in numeric_cols:
            s = frame[col].dropna()
            if len(s) < 12:
                continue
            q1 = float(s.quantile(0.25))
            q3 = float(s.quantile(0.75))
            iqr = q3 - q1
            if iqr <= 0:
                continue
            low = q1 - 1.5 * iqr
            high = q3 + 1.5 * iqr
            mask = (s < low) | (s > high)
            count = int(mask.sum())
            outlier_scan.append(
                {
                    "column": col,
                    "outlier_count": count,
                    "outlier_pct": float(count / max(len(s), 1)),
                    "lower_bound": float(low),
                    "upper_bound": float(high),
                }
            )
        outlier_scan.sort(key=lambda x: x["outlier_pct"], reverse=True)

        correlation_top: list[dict] = []
        heatmap = {"columns": [], "matrix": []}
        if len(numeric_cols) >= 2:
            corr_cols = numeric_cols[:18]
            corr_matrix = frame[corr_cols].corr(numeric_only=True)
            pairs: list[tuple[str, str, float]] = []
            for i, c1 in enumerate(corr_cols):
                for c2 in corr_cols[i + 1 :]:
                    val = corr_matrix.loc[c1, c2]
                    if pd.notna(val):
                        pairs.append((c1, c2, float(val)))
            pairs.sort(key=lambda x: abs(x[2]), reverse=True)
            correlation_top = [
                {"left": a, "right": b, "corr": v} for a, b, v in pairs[:15]
            ]
            heat_cols = corr_cols[:12]
            heatmap = {
                "columns": heat_cols,
                "matrix": [
                    [float(corr_matrix.loc[r, c]) for c in heat_cols] for r in heat_cols
                ],
            }

        trend_profile = {
            "available": False,
            "date_column": None,
            "metric_column": metric_col,
            "series": [],
            "reason": None,
        }
        if datetime_cols:
            date_col = datetime_cols[0]
            temp = frame[[date_col]].copy()
            if metric_col and metric_col in frame.columns:
                temp[metric_col] = frame[metric_col]

            temp = temp.dropna(subset=[date_col])
            if not temp.empty:
                temp["period"] = temp[date_col].dt.to_period("M").astype(str)
                grouped = temp.groupby("period", dropna=False)
                trend_rows: list[dict] = []
                for period, chunk in grouped:
                    row = {
                        "period": str(period),
                        "records": int(len(chunk)),
                    }
                    if metric_col and metric_col in chunk.columns:
                        vals = chunk[metric_col].dropna()
                        row["metric_mean"] = float(vals.mean()) if not vals.empty else None
                    trend_rows.append(row)

                trend_rows.sort(key=lambda x: x["period"])
                trend_profile = {
                    "available": True,
                    "date_column": date_col,
                    "metric_column": metric_col,
                    "series": trend_rows,
                    "reason": None,
                }
        else:
            trend_profile["reason"] = "No date-like column detected"

        distribution_profile = {
            "available": False,
            "metric_column": metric_col,
            "values_sample": [],
            "summary": {},
            "reason": None,
        }
        if metric_col and metric_col in frame.columns:
            vals = frame[metric_col].dropna()
            if not vals.empty:
                sample_n = min(4000, len(vals))
                sample_vals = vals.sample(sample_n, random_state=42).tolist()
                distribution_profile = {
                    "available": True,
                    "metric_column": metric_col,
                    "values_sample": [float(v) for v in sample_vals],
                    "summary": {
                        "min": float(vals.min()),
                        "p25": float(vals.quantile(0.25)),
                        "median": float(vals.median()),
                        "p75": float(vals.quantile(0.75)),
                        "max": float(vals.max()),
                    },
                    "reason": None,
                }
        if not distribution_profile["available"]:
            distribution_profile["reason"] = "No suitable numeric metric detected"

        segment_pareto = {
            "available": False,
            "segment_column": None,
            "metric_column": metric_col,
            "rows": [],
            "reason": None,
        }
        if metric_col and categorical_cols:
            candidate_col = None
            best_score = -1.0
            for col in categorical_cols:
                filled = frame[col].notna().mean()
                uniq = frame[col].nunique(dropna=True)
                if uniq < 2 or uniq > 80:
                    continue
                score = float(filled) * (1.0 - (uniq / 100.0))
                if score > best_score:
                    best_score = score
                    candidate_col = col

            if candidate_col:
                grouped = (
                    frame[[candidate_col, metric_col]]
                    .dropna(subset=[metric_col])
                    .assign(**{candidate_col: lambda d: d[candidate_col].fillna("UNKNOWN").astype(str)})
                    .groupby(candidate_col, dropna=False)[metric_col]
                    .mean()
                    .reset_index(name="value")
                )
                grouped = grouped.sort_values("value", ascending=False).head(15)
                total_abs = float(grouped["value"].abs().sum()) or 1.0
                cum = 0.0
                rows: list[dict] = []
                for _, r in grouped.iterrows():
                    val = float(r["value"])
                    cum += abs(val)
                    rows.append(
                        {
                            "segment": str(r[candidate_col]),
                            "value": val,
                            "cumulative_pct": float(cum / total_abs),
                        }
                    )

                segment_pareto = {
                    "available": True,
                    "segment_column": candidate_col,
                    "metric_column": metric_col,
                    "rows": rows,
                    "reason": None,
                }
        if not segment_pareto["available"]:
            segment_pareto["reason"] = "No suitable categorical segmentation detected"

        driver_scatter = {
            "available": False,
            "x_col": None,
            "y_col": None,
            "corr": None,
            "points": [],
            "reason": None,
        }
        if correlation_top:
            best = correlation_top[0]
            x_col = best["left"]
            y_col = best["right"]
            pts = frame[[x_col, y_col]].dropna()
            if not pts.empty:
                pts = pts.sample(min(2500, len(pts)), random_state=42)
                points = [
                    {"x": float(x), "y": float(y)}
                    for x, y in pts[[x_col, y_col]].itertuples(index=False, name=None)
                ]
                driver_scatter = {
                    "available": True,
                    "x_col": x_col,
                    "y_col": y_col,
                    "corr": float(best["corr"]),
                    "points": points,
                    "reason": None,
                }
        # ── Top-3 correlated scatter pairs for multi-scatter chart ───────────
        top_scatter_pairs: list[dict] = []
        for pair in correlation_top[:4]:
            x_col = pair["left"]
            y_col = pair["right"]
            pairs_df = frame[[x_col, y_col]].dropna()
            if len(pairs_df) < 10:
                continue
            pts_sample = pairs_df.sample(min(1500, len(pairs_df)), random_state=42)
            top_scatter_pairs.append({
                "x_col": x_col,
                "y_col": y_col,
                "corr": pair["corr"],
                "points": [
                    {"x": float(x), "y": float(y)}
                    for x, y in pts_sample.itertuples(index=False, name=None)
                ],
            })
        if not driver_scatter["available"] and top_scatter_pairs:
            # If the old driver_scatter failed, use the first top pair
            first = top_scatter_pairs[0]
            driver_scatter = {
                "available": True,
                "x_col": first["x_col"],
                "y_col": first["y_col"],
                "corr": first["corr"],
                "points": first["points"],
                "reason": None,
            }
        if not driver_scatter["available"]:
            driver_scatter["reason"] = "Not enough numeric relationships for scatter"

        time_profile = {"date_column": None, "granularity": None, "series": []}
        if datetime_cols:
            date_col = datetime_cols[0]
            d = frame[date_col].dropna()
            if not d.empty:
                monthly = (
                    d.dt.to_period("M")
                    .astype(str)
                    .value_counts()
                    .sort_index()
                )
                time_profile = {
                    "date_column": date_col,
                    "granularity": "month",
                    "series": [
                        {"period": str(period), "count": int(count)}
                        for period, count in monthly.items()
                    ],
                }

        duplicate_rows = int(frame.duplicated().sum())
        duplicate_pct = float(duplicate_rows / max(len(frame), 1))

        risk_signals: list[str] = []
        if missingness_rows and missingness_rows[0]["missing_pct"] > 0.2:
            risk_signals.append(
                f"High missingness in {missingness_rows[0]['column']} ({missingness_rows[0]['missing_pct']:.1%})."
            )
        high_outlier = next((x for x in outlier_scan if x["outlier_pct"] > 0.08), None)
        if high_outlier:
            risk_signals.append(
                f"Outlier-heavy metric {high_outlier['column']} ({high_outlier['outlier_pct']:.1%} flagged points)."
            )
        strong_corr = next((x for x in correlation_top if abs(x["corr"]) > 0.8), None)
        if strong_corr:
            risk_signals.append(
                f"Strong relationship between {strong_corr['left']} and {strong_corr['right']} (corr={strong_corr['corr']:.2f})."
            )
        if duplicate_pct > 0.03:
            risk_signals.append(f"Duplicate records are high ({duplicate_pct:.1%} of rows).")

        if not risk_signals:
            risk_signals.append("Dataset quality is stable; prioritize business drill-down by segment.")

        action_plan = [
            "Validate business-critical columns with high missingness before downstream modeling.",
            "Create KPI views by top segments to identify underperforming groups.",
            "Track flagged high-variance metrics with alert thresholds.",
        ]

        descriptive = {
            "overview": {
                "rows": int(frame.shape[0]),
                "columns": int(frame.shape[1]),
                "numeric_columns": int(len(numeric_cols)),
                "categorical_columns": int(len(categorical_cols)),
                "datetime_columns": int(len(datetime_cols)),
                "duplicate_rows": duplicate_rows,
                "duplicate_pct": duplicate_pct,
            },
            "data_prep": {
                "missing_strategy": missing_strategy,
                "numeric_missing_before": missing_before,
                "numeric_missing_after": missing_after,
                "rows_before": rows_before,
                "rows_after": rows_after,
            },
            "numeric_profile": numeric_profile,
            "categorical_profile": categorical_profile,
            "time_profile": time_profile,
            "trend_profile": trend_profile,
            "distribution_profile": distribution_profile,
            "segment_pareto": segment_pareto,
        }
        diagnostic = {
            "missingness": missingness_rows,
            "outlier_scan": outlier_scan,
            "correlation_top": correlation_top,
            "correlation_heatmap": heatmap,
            "driver_scatter": driver_scatter,
            "top_scatter_pairs": top_scatter_pairs,
        }
        predictive = {
            "risk_signals": risk_signals,
            "action_plan": action_plan,
            "note": "Generated from full dataset profiling and domain heuristics.",
        }
        return {
            "descriptive": descriptive,
            "diagnostic": diagnostic,
            "predictive": predictive,
        }

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
