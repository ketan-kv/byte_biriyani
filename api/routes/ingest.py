from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, Request

from api.schemas import IngestRequest
from pipelines.ingestion.ingestion_router import detect_file_type


router = APIRouter(tags=["ingestion"])


@router.post("/ingest")
def ingest_file(payload: IngestRequest, request: Request) -> dict:
    orchestrator = request.app.state.orchestrator
    path = Path(payload.path)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Input file does not exist")

    file_type = payload.file_type or detect_file_type(payload.path)
    if file_type == "unknown":
        raise HTTPException(status_code=400, detail="Unsupported file type")
    try:
        result = orchestrator.on_new_file(payload.path, file_type)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Ingestion failed: {exc}") from exc
    return {"status": "ok", "result": result}
