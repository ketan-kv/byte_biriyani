from __future__ import annotations

import streamlit as st

from frontend.components.charts import bar_chart_from_mapping, line_chart_from_mapping


def render(analysis: dict) -> None:
    st.subheader("Overview")
    production = analysis.get("descriptive", {}).get("production", {})

    c1, c2, c3 = st.columns(3)
    c1.metric("Tracked Days", len(production.get("daily_yield", {})))
    c2.metric("Mineral Types", len(production.get("by_mineral", {})))
    c3.metric("Top Zones", len(production.get("top_zones", {})))

    line_chart_from_mapping("Daily Yield Trend", production.get("daily_yield", {}))
    bar_chart_from_mapping("Yield by Mineral", production.get("by_mineral", {}))
