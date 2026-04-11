from __future__ import annotations

from pathlib import Path

from pipelines.ingestion.csv_parser import parse_generic_csv, parse_production_csv, parse_sensor_csv
from pipelines.ingestion.log_parser import parse_log
from pipelines.ingestion.pdf_extractor import extract_pdf


def detect_file_type(path: str | Path) -> str:
    file_path = Path(path)
    lower = str(file_path).lower()
    suffix = file_path.suffix.lower()

    if "geological" in lower and suffix == ".pdf":
        return "geological_report"
    if "sensor" in lower and suffix == ".csv":
        return "sensor_csv"
    if "production" in lower and suffix == ".csv":
        return "production_log"
    if "incident" in lower and suffix in {".txt", ".log", ".md"}:
        return "incident_report"
    if suffix == ".pdf":
        return "geological_report"
    if suffix == ".csv":
        return "sensor_csv"
    if suffix in {".txt", ".log", ".md"}:
        return "incident_report"
    return "unknown"


def ingest(filepath: str | Path, file_type: str | None = None) -> dict:
    ftype = file_type or detect_file_type(filepath)
    source = str(filepath)

    if ftype == "geological_report":
        payload = extract_pdf(source)
        return {"type": ftype, "content": payload["text"], "tables": payload["tables"], "source_path": source}
    if ftype == "sensor_csv":
        df = parse_sensor_csv(source)
        return {"type": ftype, "content": df, "source_path": source}
    if ftype == "production_log":
        df = parse_production_csv(source)
        return {"type": ftype, "content": df, "source_path": source}
    if ftype == "incident_report":
        payload = parse_log(source)
        return {"type": ftype, "content": payload["text"], "fields": payload["fields"], "source_path": source}

    df = parse_generic_csv(source)
    return {"type": "unknown", "content": df, "source_path": source}
