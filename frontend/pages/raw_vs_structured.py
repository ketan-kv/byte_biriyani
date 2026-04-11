from __future__ import annotations

import sqlite3

import pandas as pd
import streamlit as st


def render(db_path: str, sample_raw_path: str | None = None) -> None:
    st.subheader("Raw vs Structured")
    if sample_raw_path:
        st.caption(f"Raw source preview: {sample_raw_path}")

    conn = sqlite3.connect(db_path)
    try:
        geo = pd.read_sql("SELECT * FROM geological_records ORDER BY id DESC LIMIT 10", conn)
        st.write("Structured geological records (latest 10):")
        st.dataframe(geo, use_container_width=True)
    except Exception:
        st.info("No geological structured records yet.")
    finally:
        conn.close()
