# agents/intent_agent.py
from __future__ import annotations

import json

import ollama
import pandas as pd


OLLAMA_MODEL = "llama3.2:3b"


class IntentAgent:
    def detect(self, df: pd.DataFrame) -> str:
        domain, _ = self.detect_with_confidence(df)
        return domain

    def detect_with_confidence(self, df: pd.DataFrame) -> tuple[str, float]:
        cols = list(df.columns)
        # Take up to 3 sample rows as strings for context
        sample_rows = df.head(3).astype(str).to_dict(orient="records")

        prompt = (
            f"You are a data analyst. Given a dataset with these columns: {cols}\n"
            f"And these sample rows: {sample_rows}\n\n"
            "Identify which industry domain this dataset belongs to. "
            "Choose ONE domain from: mining, healthcare, ecommerce, finance, manufacturing, logistics, agriculture, energy, unknown.\n"
            "Return ONLY a JSON object with two keys: "
            '"domain" (string, one of the options above) and "confidence" (float between 0.0 and 1.0). '
            "No explanation. JSON only."
        )

        try:
            response = ollama.chat(
                model=OLLAMA_MODEL,
                messages=[{"role": "user", "content": prompt}],
                format="json",
            )
            result = json.loads(response["message"]["content"])
            domain = str(result.get("domain", "unknown")).lower()
            confidence = float(result.get("confidence", 0.5))
            return domain, round(confidence, 2)
        except Exception:
            return "unknown", 0.0