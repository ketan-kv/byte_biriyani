# frontend/pages/domain_upload.py
from __future__ import annotations

import requests
import streamlit as st

from frontend.components.insight_cards import render_insight_cards


def render() -> None:
    st.subheader("Domain-Adaptive Analysis")
    st.caption(
        "Upload any dataset — CSV or Excel. "
        "The system detects the industry domain and generates expert insights automatically."
    )

    file = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"])
    if not file:
        return

    if st.button("Run Domain Analysis", type="primary"):
        with st.spinner("Detecting domain and researching knowledge... (~30 seconds)"):
            try:
                response = requests.post(
                    "http://127.0.0.1:8000/run-domain-pipeline",
                    files={"file": (file.name, file.getvalue(), file.type)},
                    timeout=120,
                )
            except Exception as e:
                st.error(f"Could not reach backend: {e}")
                return

        if response.status_code != 200:
            st.error(f"Error from backend: {response.text}")
            return

        data = response.json()

        # Domain banner
        domain = data.get("domain", "unknown").upper()
        conf = data.get("confidence", 0) * 100
        source = data.get("knowledge_source", "llm")
        st.success(f"Domain detected: **{domain}** — confidence {conf:.0f}% — knowledge from: {source}")

        # KPIs
        kpis = data.get("kpis", [])
        if kpis:
            st.divider()
            st.subheader("Domain KPIs")
            cols = st.columns(min(len(kpis), 3))
            for i, kpi in enumerate(kpis[:3]):
                with cols[i]:
                    st.metric(label=kpi.get("name", ""), value=kpi.get("normal_range", ""))
                    st.caption(kpi.get("what_it_measures", ""))

        # Analysis priorities
        priorities = data.get("analysis_priorities", [])
        if priorities:
            st.divider()
            st.subheader("What the system focused on")
            for p in priorities:
                st.markdown(f"- {p}")

        # Insights
        st.divider()
        st.subheader("AI-Generated Insights")
        render_insight_cards(data.get("insights", []))