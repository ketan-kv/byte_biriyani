from __future__ import annotations

import streamlit as st

from frontend.components.insight_cards import render_insight_cards


def render(items: list[dict]) -> None:
    st.subheader("Prioritized Insights")
    render_insight_cards(items)
