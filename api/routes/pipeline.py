from __future__ import annotations

from fastapi import APIRouter, Request


router = APIRouter(tags=["pipeline"])


@router.post("/run-pipeline")
def run_pipeline(request: Request) -> dict:
    orchestrator = request.app.state.orchestrator
    output = orchestrator.run_pipeline()
    return {
        "status": "ok",
        "insights_generated": len(output.get("insights", [])),
        "analysis_keys": list(output.get("analysis", {}).keys()),
    }
