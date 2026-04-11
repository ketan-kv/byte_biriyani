from __future__ import annotations

import asyncio
import json
import threading
from io import BytesIO
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import create_engine, text

from api.schemas import DatabaseDomainPipelineRequest


router = APIRouter(tags=["domain_pipeline"])


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
		if suffix == ".csv":
			df = pd.read_csv(BytesIO(payload), low_memory=False)
		else:
			df = pd.read_excel(BytesIO(payload))
	except Exception as exc:
		raise HTTPException(status_code=400, detail=f"Could not parse file: {exc}") from exc

	if df.empty:
		raise HTTPException(status_code=400, detail="Uploaded dataset has no rows")

	try:
		result = orchestrator.run_domain_pipeline(df, user_preferences=prefs)
	except Exception as exc:
		raise HTTPException(status_code=500, detail=f"Domain pipeline failed: {exc}") from exc

	result["source_type"] = "file"
	result["file_name"] = name
	result["rows"] = int(df.shape[0])
	result["columns"] = [str(c) for c in df.columns]
	return result


@router.post("/run-domain-pipeline-stream")
async def run_domain_pipeline_stream(
	request: Request,
	file: UploadFile = File(...),
	user_preferences: str | None = Form(default=None),
) -> StreamingResponse:
	"""SSE endpoint — streams pipeline stage events as they complete."""
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
		if suffix == ".csv":
			df = pd.read_csv(BytesIO(payload), low_memory=False)
		else:
			df = pd.read_excel(BytesIO(payload))
	except Exception as exc:
		raise HTTPException(status_code=400, detail=f"Could not parse file: {exc}") from exc

	if df.empty:
		raise HTTPException(status_code=400, detail="Uploaded dataset has no rows")

	loop = asyncio.get_event_loop()
	queue: asyncio.Queue = asyncio.Queue()

	def _run():
		try:
			for event in orchestrator.stream_domain_pipeline(df, user_preferences=prefs):
				asyncio.run_coroutine_threadsafe(queue.put(event), loop)
		except Exception as exc:  # noqa: BLE001
			asyncio.run_coroutine_threadsafe(
				queue.put({"stage": "error", "status": "error", "details": {"error": str(exc)}}),
				loop,
			)
		finally:
			asyncio.run_coroutine_threadsafe(queue.put(None), loop)  # sentinel

	threading.Thread(target=_run, daemon=True).start()

	async def event_generator():
		while True:
			try:
				event = await asyncio.wait_for(queue.get(), timeout=180.0)
			except asyncio.TimeoutError:
				yield 'data: {"stage":"error","status":"error","details":{"error":"pipeline timeout"}}\n\n'
				break
			if event is None:
				break
			yield f"data: {json.dumps(event, default=str)}\n\n"
		yield 'data: {"stage":"stream_end"}\n\n'

	return StreamingResponse(
		event_generator(),
		media_type="text/event-stream",
		headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
	)


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

	result["source_type"] = "database"
	result["rows"] = int(df.shape[0])
	result["columns"] = [str(c) for c in df.columns]
	return result
