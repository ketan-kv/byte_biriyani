from __future__ import annotations

from pipelines.structuring.rule_engine import RuleEngine


class LLMParser:
    """
    Deterministic fallback parser.

    This class is structured to allow plugging an LLM API later,
    while still producing usable records offline.
    """

    def __init__(self) -> None:
        self.rules = RuleEngine()

    def parse_geo(self, text: str, source_file: str = "unknown") -> list[dict]:
        minerals = self.rules.extract_minerals(text)
        depths = self.rules.extract_depths(text)
        grades = self.rules.extract_grades(text)
        zones = self.rules.extract_zones(text)

        if not minerals and not depths and not grades:
            return []

        out: list[dict] = []
        for idx, mineral in enumerate(minerals or [None]):
            depth = depths[idx][0] if idx < len(depths) else None
            grade_val = grades[idx][0] if idx < len(grades) else None
            grade_unit = grades[idx][1] if idx < len(grades) else None
            zone = zones[idx] if idx < len(zones) else None
            out.append(
                {
                    "source_file": source_file,
                    "survey_date": None,
                    "location_name": zone,
                    "latitude": None,
                    "longitude": None,
                    "depth_m": depth,
                    "mineral_type": mineral,
                    "grade_value": grade_val,
                    "grade_unit": grade_unit,
                    "rock_type": None,
                    "zone_id": zone,
                    "confidence": 0.55,
                }
            )
        return out

    def infer_root_cause(self, text: str) -> str:
        lowered = text.lower()
        if "vibration" in lowered:
            return "Likely bearing degradation due to high vibration readings."
        if "temperature" in lowered:
            return "Likely thermal overload from cooling inefficiency."
        if "pressure" in lowered:
            return "Possible pressure instability in hydraulic circuit."
        return "Insufficient explicit evidence; requires manual investigation."
