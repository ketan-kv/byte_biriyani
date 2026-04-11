from __future__ import annotations

import sqlite3

import pandas as pd


def _read_extraction_logs(conn: sqlite3.Connection) -> pd.DataFrame:
    try:
        return pd.read_sql("SELECT * FROM extraction_logs", conn)
    except Exception:
        return pd.DataFrame()


def downtime_summary(conn: sqlite3.Connection) -> dict:
    df = _read_extraction_logs(conn)
    if df.empty:
        return {}
    return (
        df.groupby("zone_id", dropna=False)["downtime_hours"]
        .sum(min_count=1)
        .fillna(0)
        .to_dict()
    )


def production_trend(conn: sqlite3.Connection) -> dict:
    df = _read_extraction_logs(conn)
    if df.empty:
        return {
            "daily_yield": {},
            "by_mineral": {},
            "efficiency_trend": {},
            "downtime_analysis": {},
            "top_zones": {},
        }

    if "log_date" in df.columns:
        df["log_date"] = pd.to_datetime(df["log_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    result = {
        "daily_yield": df.groupby("log_date", dropna=False)["yield_tonnes"].sum(min_count=1).fillna(0).to_dict(),
        "by_mineral": df.groupby("mineral_type", dropna=False)["yield_tonnes"].sum(min_count=1).fillna(0).to_dict(),
        "efficiency_trend": df.groupby("log_date", dropna=False)["efficiency_pct"].mean().fillna(0).to_dict(),
        "downtime_analysis": downtime_summary(conn),
        "top_zones": (
            df.groupby("zone_id", dropna=False)["yield_tonnes"]
            .sum(min_count=1)
            .fillna(0)
            .nlargest(5)
            .to_dict()
        ),
    }
    return result


def mineral_distribution(conn: sqlite3.Connection) -> list[dict]:
    try:
        df = pd.read_sql(
            """
            SELECT mineral_type, zone_id,
                   AVG(grade_value) AS avg_grade,
                   COUNT(*) AS sample_count
            FROM geological_records
            GROUP BY mineral_type, zone_id
            """,
            conn,
        )
    except Exception:
        return []

    if df.empty:
        return []
    return df.fillna("unknown").to_dict(orient="records")
