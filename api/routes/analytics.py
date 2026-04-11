from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request


router = APIRouter(tags=["analytics"])


@router.get("/analytics/{analysis_type}")
def get_analytics(analysis_type: str, request: Request) -> dict:
    orchestrator = request.app.state.orchestrator
    bundle = orchestrator.agents["analysis"].run_all()
    if analysis_type not in bundle:
        raise HTTPException(status_code=404, detail=f"Unknown analysis type: {analysis_type}")
    return {"type": analysis_type, "data": bundle[analysis_type]}
