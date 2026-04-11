# agents/research_agent.py
"""Domain Research Agent — builds expert-level knowledge for any domain."""
from __future__ import annotations

import json
import os
from pathlib import Path

import ollama
from dotenv import load_dotenv

load_dotenv()

OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "mistral")
OLLAMA_HOST = os.getenv("OLLAMA_HOST")
OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY")

client_kwargs = {"host": OLLAMA_HOST} if OLLAMA_HOST else {}
if OLLAMA_API_KEY:
    client_kwargs["headers"] = {"Authorization": f"Bearer {OLLAMA_API_KEY}"}
OLLAMA_CLIENT = ollama.Client(**client_kwargs)


_SYSTEM_PROMPT = """You are the world's foremost expert data analyst with 20+ years of experience.
You know the KPIs, benchmarks, statistical patterns, typical data issues, and decision rules for every major business domain.
When given a domain and a set of column names, you produce an exhaustive knowledge pack that a junior analyst can use to conduct expert-level analysis.
Return ONLY valid JSON. No markdown, no explanation outside the JSON."""

_USER_PROMPT = """Domain: {domain}
Dataset columns: {columns}

Produce a comprehensive domain knowledge pack. Return a JSON object with EXACTLY these keys:

"kpis": list of 8 objects, each with:
  - "name": KPI name
  - "what_it_measures": one-sentence explanation
  - "normal_range": industry benchmark range as a string
  - "red_flag": string describing what signals a problem (include specific numbers where possible)
  - "formula_hint": how it is typically calculated

"analysis_priorities": list of 12 strings, ordered by business impact. Each string should describe a specific thing to look for and WHY it matters to this domain.

"vocabulary": list of 15 domain-specific technical terms with one-sentence definitions in a flat list of strings ("term: definition").

"correlation_hypotheses": list of 6 objects, each with:
  - "columns": list of 2 column-name patterns (use the actual provided column names where possible)
  - "expected_direction": "positive" or "negative"
  - "reasoning": why these variables likely correlate in this domain

"seasonal_patterns": list of 4 strings describing typical temporal cycles in this domain (e.g., "Q4 spike in retail sales due to holiday season").

"common_data_issues": list of 5 strings describing data quality problems typical in this domain.

"benchmarks": list of 6 objects, each with:
  - "metric": metric name
  - "good_value": string with a specific number or range
  - "poor_value": string with a specific number or range
  - "unit": string

"decision_rules": list of 5 strings. Each is a concrete if-then rule: "If [metric] exceeds [threshold], then [recommended action]."

"positive_signals": list of 4 strings describing what GOOD performance looks like in this domain (not just problems).

No explanation. No markdown. JSON only."""


class ResearchAgent:
    def __init__(self, knowledge_dir: str = "knowledge/") -> None:
        self._cache: dict[str, dict] = {}
        self.knowledge_dir = Path(knowledge_dir)

    def research(self, domain: str, columns: list[str]) -> dict:
        """Return expert-level domain knowledge. Uses memory cache → JSON file → LLM."""
        # Layer 1: in-memory session cache (don't re-research same domain)
        if domain in self._cache:
            return self._cache[domain]

        # Layer 2: pre-written JSON file (operator-curated knowledge)
        json_path = self.knowledge_dir / f"{domain}.json"
        if json_path.exists():
            try:
                data = json.loads(json_path.read_text(encoding="utf-8"))
                self._cache[domain] = data
                return data
            except Exception:
                pass

        # Layer 3: LLM self-research (expert prompt)
        result = self._llm_research(domain, columns)
        self._cache[domain] = result
        return result

    def _llm_research(self, domain: str, columns: list[str]) -> dict:
        prompt = _USER_PROMPT.format(domain=domain, columns=json.dumps(columns))

        try:
            response = OLLAMA_CLIENT.chat(
                model=OLLAMA_MODEL,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                format="json",
                options={"num_ctx": 2048, "num_predict": 1024, "num_thread": 8},
                keep_alive="1h",
            )
            raw = json.loads(response["message"]["content"])
            # Validate and normalize — keep whatever the LLM returned
            if not isinstance(raw, dict):
                return self._fallback(domain)
            return raw
        except Exception:
            return self._fallback(domain)

    def _fallback(self, domain: str) -> dict:
        """Minimal fallback when LLM is unavailable."""
        return {
            "kpis": [],
            "analysis_priorities": [
                "Identify the primary outcome variable and measure its distribution.",
                "Find which categorical segment drives the most volume.",
                "Detect time-based trends in the key metric.",
                "Locate the strongest correlations to understand root causes.",
                f"Benchmark {domain} KPIs against industry norms.",
                "Flag data quality issues before drawing conclusions.",
                "Identify outlier records that hide process failures.",
                "Compare performance between top vs bottom segments.",
                "Detect seasonality and cyclical patterns.",
                "Build a short-list of leading indicators for future performance.",
                "Assess completeness of critical business columns.",
                "Prioritize recommendations by business value and effort.",
            ],
            "vocabulary": [],
            "correlation_hypotheses": [],
            "seasonal_patterns": [],
            "common_data_issues": [
                "Missing values in key metric columns due to manual entry gaps.",
                "Duplicate records from ETL pipeline restarts.",
                "Outliers from data entry errors (impossible values).",
                "Date format inconsistencies across data sources.",
                "Categorical label drift (same entity with different string representations).",
            ],
            "benchmarks": [],
            "decision_rules": [],
            "positive_signals": [],
        }

    # ── Convenience accessors ──────────────────────────────────────────────────

    def get_kpis(self, domain: str, columns: list[str]) -> list[dict]:
        return self.research(domain, columns).get("kpis", [])

    def get_thresholds(self, domain: str, columns: list[str]) -> dict:
        return self.research(domain, columns).get("anomaly_thresholds", {})

    def get_vocabulary(self, domain: str, columns: list[str]) -> list[str]:
        k = self.research(domain, columns)
        vocab = k.get("vocabulary", [])
        # Support both flat list of strings and old list of {term: definition} objects
        if vocab and isinstance(vocab[0], dict):
            return [f"{v.get('term','')} : {v.get('definition','')}" for v in vocab]
        return [str(v) for v in vocab]

    def get_correlation_hypotheses(self, domain: str, columns: list[str]) -> list[dict]:
        return self.research(domain, columns).get("correlation_hypotheses", [])

    def get_decision_rules(self, domain: str, columns: list[str]) -> list[str]:
        return self.research(domain, columns).get("decision_rules", [])

    def get_positive_signals(self, domain: str, columns: list[str]) -> list[str]:
        return self.research(domain, columns).get("positive_signals", [])