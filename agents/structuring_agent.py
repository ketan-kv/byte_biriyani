from __future__ import annotations

import re

import pandas as pd

from pipelines.ingestion.sensor_stream import process_sensor_batch, zscore_anomalies
from pipelines.structuring.llm_parser import LLMParser
from pipelines.structuring.nlp_pipeline import extract_entities, load_nlp
from pipelines.structuring.normalizer import normalize_geological_record, normalize_incident_record
from pipelines.structuring.rule_engine import RuleEngine
from utils.date_parser import normalize_date
from utils.text_cleaner import clean_text


class StructuringAgent:
    def __init__(self) -> None:
        self.nlp = load_nlp()
        self.rule_engine = RuleEngine()
        self.llm_parser = LLMParser()

    def run(self, raw: dict, file_type: str) -> dict:
        if file_type == "geological_report":
            return {"table": "geological_records", "records": self._parse_geo_report(raw["content"], raw["source_path"])}
        if file_type == "sensor_csv":
            sensor_df = self._parse_sensor_csv(raw["content"], raw["source_path"])
            return {"sensor_df": sensor_df}
        if file_type == "incident_report":
            rec = self._parse_incident(raw["content"], raw["source_path"])
            return {"table": "incident_reports", "records": [rec] if rec else []}
        if file_type == "production_log":
            recs = self._parse_production_log(raw["content"], raw["source_path"])
            return {"table": "extraction_logs", "records": recs}
        return {"records": []}

    def _parse_geo_report(self, text: str, source_file: str) -> list[dict]:
        cleaned = clean_text(text)
        minerals = self.rule_engine.extract_minerals(cleaned)
        depths = self.rule_engine.extract_depths(cleaned)
        grades = self.rule_engine.extract_grades(cleaned)
        zones = self.rule_engine.extract_zones(cleaned)
        dates = self.rule_engine.extract_dates(cleaned)

        entities = extract_entities(self.nlp, cleaned)
        locations = [ent for ent, label in entities if label in {"GPE", "LOC", "FAC"}]

        max_len = max(len(minerals), len(depths), len(grades), len(zones), 1)
        records: list[dict] = []
        for idx in range(max_len):
            mineral = minerals[idx] if idx < len(minerals) else None
            depth_m = depths[idx][0] if idx < len(depths) else None
            grade_value = grades[idx][0] if idx < len(grades) else None
            grade_unit = grades[idx][1] if idx < len(grades) else None
            zone = zones[idx] if idx < len(zones) else None
            location_name = locations[idx] if idx < len(locations) else zone
            survey_date = normalize_date(dates[0]) if dates else None

            record = {
                "source_file": source_file,
                "survey_date": survey_date,
                "location_name": location_name,
                "latitude": None,
                "longitude": None,
                "depth_m": depth_m,
                "mineral_type": mineral,
                "grade_value": grade_value,
                "grade_unit": grade_unit,
                "rock_type": None,
                "zone_id": zone,
                "confidence": 0.82 if mineral or grade_value else 0.6,
            }
            records.append(normalize_geological_record(record))

        if not records:
            records = self.llm_parser.parse_geo(cleaned, source_file=source_file)

        return records

    def _parse_sensor_csv(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        working = df.copy()
        working["source_file"] = source_file
        processed = process_sensor_batch(working)
        if "value" in processed.columns:
            processed = zscore_anomalies(processed)
        if "is_anomaly" not in processed.columns:
            processed["is_anomaly"] = False
        return processed

    def _parse_production_log(self, df: pd.DataFrame, source_file: str) -> list[dict]:
        if df.empty:
            return []
        working = df.copy()
        if "log_date" in working.columns:
            working["log_date"] = pd.to_datetime(working["log_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        if "efficiency_pct" not in working.columns and {"yield_tonnes", "ore_processed_t"}.issubset(working.columns):
            denom = working["ore_processed_t"].replace(0, pd.NA)
            working["efficiency_pct"] = (working["yield_tonnes"] / denom) * 100
        working["source_file"] = source_file
        return working.where(pd.notna(working), None).to_dict(orient="records")

    def _parse_incident(self, text: str, source_file: str) -> dict:
        record: dict[str, object] = {
            "incident_date": None,
            "incident_time": None,
            "zone_id": None,
            "equipment_id": None,
            "incident_type": None,
            "severity": None,
            "description": None,
            "root_cause": None,
            "corrective_action": None,
            "reported_by": None,
            "resolved": False,
            "resolution_date": None,
            "source_file": source_file,
        }

        date_match = re.search(r"Date[:\s]+(.+?)[\n\r]", text, re.I)
        if date_match:
            record["incident_date"] = normalize_date(date_match.group(1).strip())

        equipment = self.rule_engine.extract_equipment(text)
        if equipment:
            record["equipment_id"] = equipment[0]

        zone = self.rule_engine.extract_zones(text)
        if zone:
            record["zone_id"] = zone[0]

        desc_match = re.search(r"Description[:\s]+(.+?)(?=\n[A-Z]|\Z)", text, re.I | re.S)
        if desc_match:
            record["description"] = clean_text(desc_match.group(1))

        for level in ["critical", "high", "medium", "low"]:
            if re.search(rf"\b{level}\b", text, re.I):
                record["severity"] = level.upper()
                break

        cause_match = re.search(r"(root cause|cause)[:\s]+(.+?)(?=\n[A-Z]|\Z)", text, re.I | re.S)
        if cause_match:
            record["root_cause"] = clean_text(cause_match.group(2))
        else:
            record["root_cause"] = self.llm_parser.infer_root_cause(text)

        return normalize_incident_record(record)
