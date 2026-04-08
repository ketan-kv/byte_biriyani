from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st


def line_chart_from_mapping(title: str, mapping: dict) -> None:
    if not mapping:
        st.info(f"No data available for {title}.")
        return
    df = pd.DataFrame({"x": list(mapping.keys()), "y": list(mapping.values())})
    fig = px.line(df, x="x", y="y", title=title, markers=True)
    st.plotly_chart(fig, use_container_width=True)


def bar_chart_from_mapping(title: str, mapping: dict) -> None:
    if not mapping:
        st.info(f"No data available for {title}.")
        return
    df = pd.DataFrame({"x": list(mapping.keys()), "y": list(mapping.values())})
    fig = px.bar(df, x="x", y="y", title=title)
    st.plotly_chart(fig, use_container_width=True)
