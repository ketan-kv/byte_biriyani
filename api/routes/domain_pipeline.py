from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from sqlalchemy import create_engine, text

from api.schemas import DatabaseDomainPipelineRequest


router = APIRouter(tags=["domain_pipeline"])


CSV_FALLBACK_ENCODINGS = ("utf-8", "utf-8-sig", "cp1252", "latin-1")


def _parse_user_preferences(raw: str | None) -> dict:
	if not raw:
		return {}
	try:
		parsed = json.loads(raw)
	except Exception as exc:
		raise HTTPException(status_code=400, detail=f"Invalid user_preferences JSON: {exc}") from exc
	if not isinstance(parsed, dict):
		raise HTTPException(status_code=400, detail="user_preferences must be a JSON object")
	return parsed


def _read_uploaded_dataframe(payload: bytes, suffix: str) -> pd.DataFrame:
	if suffix in {".xlsx", ".xls"}:
		return pd.read_excel(BytesIO(payload))

	last_decode_error: UnicodeDecodeError | None = None
	for encoding in CSV_FALLBACK_ENCODINGS:
		try:
			return pd.read_csv(BytesIO(payload), low_memory=False, encoding=encoding)
		except UnicodeDecodeError as exc:
			last_decode_error = exc

	if last_decode_error is not None:
		raise ValueError(
			"Could not decode CSV with supported encodings: "
			+ ", ".join(CSV_FALLBACK_ENCODINGS)
		) from last_decode_error

	raise ValueError("Could not parse CSV payload")


@router.post("/run-domain-pipeline")
async def run_domain_pipeline(
	request: Request,
	file: UploadFile = File(...),
	user_preferences: str | None = Form(default=None),
) -> dict:
	orchestrator = request.app.state.orchestrator
	prefs = _parse_user_preferences(user_preferences)
	name = file.filename or "uploaded_file"
	suffix = Path(name).suffix.lower()

	if suffix not in {".csv", ".xlsx", ".xls"}:
		raise HTTPException(status_code=400, detail="Only CSV and Excel files are supported")

	payload = await file.read()
	if not payload:
		raise HTTPException(status_code=400, detail="Uploaded file is empty")

	try:
		df = _read_uploaded_dataframe(payload, suffix)
	except Exception as exc:
		raise HTTPException(status_code=400, detail=f"Could not parse file: {exc}") from exc

	if df.empty:
		raise HTTPException(status_code=400, detail="Uploaded dataset has no rows")

	try:
		result = orchestrator.run_domain_pipeline(df, user_preferences=prefs)
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"Domain pipeline failed: {exc}") from exc

	if hasattr(request.app.state, "decision_copilot"):
		request.app.state.decision_copilot.update_context(df, result)

	result["source_type"] = "file"
	result["file_name"] = name
	result["rows"] = int(df.shape[0])
	result["columns"] = [str(c) for c in df.columns]
	return result


@router.post("/run-domain-pipeline-db")
def run_domain_pipeline_db(payload: DatabaseDomainPipelineRequest, request: Request) -> dict:
	orchestrator = request.app.state.orchestrator

	try:
		engine = create_engine(payload.database_url)
	except Exception as exc:
		raise HTTPException(status_code=400, detail=f"Invalid database_url: {exc}") from exc

	try:
		with engine.connect() as conn:
			if payload.query:
				df = pd.read_sql_query(text(payload.query), conn)
			else:
				df = pd.read_sql_table(payload.table_name, conn)
	except Exception as exc:
		raise HTTPException(status_code=400, detail=f"Database read failed: {exc}") from exc

	if df.empty:
		raise HTTPException(status_code=400, detail="Database query returned no rows")

	if len(df) > payload.row_limit:
		df = df.head(payload.row_limit).copy()

	try:
		result = orchestrator.run_domain_pipeline(df, user_preferences=payload.user_preferences)
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"Domain pipeline failed: {exc}") from exc

	if hasattr(request.app.state, "decision_copilot"):
		request.app.state.decision_copilot.update_context(df, result)

	result["source_type"] = "database"
	result["rows"] = int(df.shape[0])
	result["columns"] = [str(c) for c in df.columns]
	return result
