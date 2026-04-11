from __future__ import annotations

import re


class RuleEngine:
    MINERAL_KEYWORDS = [
        "gold",
        "copper",
        "silver",
        "zinc",
        "iron",
        "lithium",
        "cobalt",
        "nickel",
        "manganese",
        "platinum",
    ]

    DEPTH_PATTERN = re.compile(r"(\d+\.?\d*)\s*(m|meter|metre|metres|ft|feet)", re.I)
    GRADE_PATTERN = re.compile(r"(\d+\.?\d*)\s*(g/t|ppm|%|oz/t)", re.I)
    DATE_PATTERN = re.compile(r"\b(\d{4}[-/]\d{2}[-/]\d{2}|\w+\s+\d{1,2},?\s*\d{4})\b")
    ZONE_PATTERN = re.compile(r"\bZone\s*[A-Z0-9-]+\b", re.I)
    EQUIPMENT_PATTERN = re.compile(r"\b[A-Z]+-\d{2,4}\b")

    def extract_minerals(self, text: str) -> list[str]:
        found: list[str] = []
        for mineral in self.MINERAL_KEYWORDS:
            if re.search(rf"\b{mineral}\b", text, re.I):
                found.append(mineral.capitalize())
        return sorted(set(found))

    def extract_depths(self, text: str) -> list[tuple[float, str]]:
        return [(float(m.group(1)), m.group(2).lower()) for m in self.DEPTH_PATTERN.finditer(text)]

    def extract_grades(self, text: str) -> list[tuple[float, str]]:
        return [(float(m.group(1)), m.group(2).lower()) for m in self.GRADE_PATTERN.finditer(text)]

    def extract_dates(self, text: str) -> list[str]:
        return [m.group(1) for m in self.DATE_PATTERN.finditer(text)]

    def extract_zones(self, text: str) -> list[str]:
        return [m.group(0).strip() for m in self.ZONE_PATTERN.finditer(text)]

    def extract_equipment(self, text: str) -> list[str]:
        return [m.group(0).strip() for m in self.EQUIPMENT_PATTERN.finditer(text)]
