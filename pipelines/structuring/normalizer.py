from __future__ import annotations

from utils.date_parser import normalize_date
from utils.unit_normalizer import standardize_unit


def normalize_geological_record(record: dict) -> dict:
    out = dict(record)
    out["survey_date"] = normalize_date(out.get("survey_date"))
    if out.get("grade_unit"):
        out["grade_unit"] = standardize_unit(out["grade_unit"])
    return out


def normalize_incident_record(record: dict) -> dict:
    out = dict(record)
    out["incident_date"] = normalize_date(out.get("incident_date"))
    sev = out.get("severity")
    if isinstance(sev, str):
        out["severity"] = sev.upper()
    return out
