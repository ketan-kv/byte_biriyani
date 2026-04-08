from __future__ import annotations

import re


UNIT_MAP = {
    "meters": "m",
    "metre": "m",
    "meter": "m",
    "metres": "m",
    "feet": "ft",
    "foot": "ft",
    "gram per tonne": "g/t",
    "grams/tonne": "g/t",
    "parts per million": "ppm",
}


def standardize_unit(raw_unit: str | None) -> str | None:
    if raw_unit is None:
        return None
    cleaned = raw_unit.strip().lower()
    return UNIT_MAP.get(cleaned, raw_unit.strip())


def normalize_units(value: str) -> tuple[float | None, str | None]:
    match = re.match(r"([\d.]+)\s*([a-zA-Z/%]+)", value.strip())
    if not match:
        return None, None
    return float(match.group(1)), standardize_unit(match.group(2))
