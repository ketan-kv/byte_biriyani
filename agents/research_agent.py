# agents/research_agent.py
from __future__ import annotations

import json
from pathlib import Path

import ollama


OLLAMA_MODEL = "llama3.2:3b"


class ResearchAgent:
    def __init__(self, knowledge_dir: str = "knowledge/") -> None:
        self._cache: dict[str, dict] = {}
        self.knowledge_dir = Path(knowledge_dir)

    def research(self, domain: str, columns: list[str]) -> dict:
        # Layer 1: in-memory session cache
        if domain in self._cache:
            return self._cache[domain]

        # Layer 2: pre-written JSON file
        json_path = self.knowledge_dir / f"{domain}.json"
        if json_path.exists():
            try:
                data = json.loads(json_path.read_text(encoding="utf-8"))
                self._cache[domain] = data
                return data
            except Exception:
                pass

        # Layer 3: LLM self-research
        result = self._llm_research(domain, columns)
        self._cache[domain] = result
        return result

    def _llm_research(self, domain: str, columns: list[str]) -> dict:
        prompt = (
            f"You are a {domain} data analytics expert.\n"
            f"The dataset has these columns: {columns}\n\n"
            "Return ONLY a valid JSON object with these exact keys:\n"
            '- "kpis": list of 5 objects, each with: "name" (string), "what_it_measures" (string), "normal_range" (string)\n'
            '- "anomaly_thresholds": object mapping metric_name to {"min": number, "max": number, "unit": string}\n'
            '- "analysis_priorities": list of 4 strings describing what to look for in this domain\n'
            '- "vocabulary": list of 8 domain-specific terms relevant to this data\n'
            "No explanation. No markdown. JSON only."
        )

        try:
            response = ollama.chat(
                model=OLLAMA_MODEL,
                messages=[{"role": "user", "content": prompt}],
                format="json",
            )
            return json.loads(response["message"]["content"])
        except Exception:
            return {
                "kpis": [],
                "anomaly_thresholds": {},
                "analysis_priorities": [],
                "vocabulary": [],
            }

    def get_kpis(self, domain: str, columns: list[str]) -> list[dict]:
        return self.research(domain, columns).get("kpis", [])

    def get_thresholds(self, domain: str, columns: list[str]) -> dict:
        return self.research(domain, columns).get("anomaly_thresholds", {})

    def get_vocabulary(self, domain: str, columns: list[str]) -> list[str]:
        return self.research(domain, columns).get("vocabulary", [])