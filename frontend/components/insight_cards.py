from __future__ import annotations

import streamlit as st


SEVERITY_COLOR = {
    "CRITICAL": "#8b0000",
    "WARNING": "#8a6d1f",
    "INFO": "#1e4f85",
}


def render_insight_cards(items: list[dict]) -> None:
    if not items:
        st.info("No insights available.")
        return

    for item in items:
        sev = item.get("severity", "INFO")
        color = SEVERITY_COLOR.get(sev, "#444444")
        st.markdown(
            f"""
            <div style="border-left: 6px solid {color}; padding: 12px; margin: 8px 0; background: #f8f9fb;">
                <h4 style="margin: 0; color: {color};">{sev}: {item.get('title', 'Untitled')}</h4>
                <p style="margin: 8px 0 4px 0;"><b>Explanation:</b> {item.get('explanation', '')}</p>
                <p style="margin: 4px 0;"><b>Recommendation:</b> {item.get('recommendation', '')}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
