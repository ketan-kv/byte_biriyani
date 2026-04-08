from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st


def render(sensor_df: pd.DataFrame) -> None:
    st.subheader("Sensor Monitor")
    if sensor_df.empty:
        st.info("No sensor data available.")
        return

    working = sensor_df.copy()
    working["timestamp"] = pd.to_datetime(working["timestamp"], errors="coerce")
    working = working.dropna(subset=["timestamp"])

    equipment_options = sorted(str(x) for x in working["equipment_id"].dropna().unique())
    selected = st.selectbox("Equipment", options=equipment_options)
    filtered = working[working["equipment_id"].astype(str) == selected]

    fig = px.line(filtered, x="timestamp", y="value", color="sensor_type", title=f"Readings for {selected}")
    st.plotly_chart(fig, use_container_width=True)

    if "is_anomaly" in filtered.columns:
        anomalies = filtered[filtered["is_anomaly"] == True]
        st.metric("Anomaly Points", len(anomalies))
