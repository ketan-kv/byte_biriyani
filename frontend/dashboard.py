from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import sqlite3
import streamlit as st

from agents.analysis_agent import AnalysisAgent
from frontend.pages import insights as insights_page
from frontend.pages import overview as overview_page
from frontend.pages import raw_vs_structured as raw_vs_structured_page
from frontend.pages import sensor_monitor as sensor_monitor_page
from utils.config_loader import load_config


st.set_page_config(page_title="AMDAIS Dashboard", page_icon="AI", layout="wide")


@st.cache_data(ttl=30)
def load_insights(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    return json.loads(p.read_text(encoding="utf-8"))


@st.cache_data(ttl=30)
def load_sensor_df(parquet_path: str) -> pd.DataFrame:
    p = Path(parquet_path)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)


@st.cache_data(ttl=30)
def run_analysis(db_path: str, sensor_path: str) -> dict:
    agent = AnalysisAgent(db_path, sensor_path)
    return agent.run_all()


def main() -> None:
    config = load_config()
    paths = config.get("paths", {})
    db_path = paths.get("sqlite_path", "data/structured/mineral_db.sqlite")
    sensor_path = paths.get("sensor_parquet_path", "data/structured/sensor_data.parquet")
    insights_path = paths.get("insights_path", "data/insights/latest_insights.json")

    st.title("AMDAIS Operations Console")
    st.caption("Autonomous Mineral Data Structuring & Analytical Intelligence System")

    page = st.sidebar.radio(
        "Page",
        ["Overview", "Sensor Monitor", "Insights", "Raw vs Structured"],
    )

    analysis = run_analysis(db_path, sensor_path)
    insights = load_insights(insights_path)
    sensor_df = load_sensor_df(sensor_path)

    if page == "Overview":
        overview_page.render(analysis)
    elif page == "Sensor Monitor":
        sensor_monitor_page.render(sensor_df)
    elif page == "Insights":
        insights_page.render(insights)
    else:
        raw_vs_structured_page.render(db_path)


if __name__ == "__main__":
    main()
