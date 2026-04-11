"""Endpoint for generating deep-dive analysis of a single AI insight."""
from __future__ import annotations

from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel

from utils.logger import get_logger

logger = get_logger("amdais.insight_deep")
router = APIRouter(tags=["insights"])


class InsightDeepRequest(BaseModel):
    insight: dict
    context: dict
    domain: str


@router.post("/insight-deep")
def generate_insight_deep(payload: InsightDeepRequest, request: Request) -> dict:
    insight_agent = request.app.state.orchestrator.agents["insight"]
    
    try:
        deep_analysis = insight_agent.generate_deep_insight(
            insight=payload.insight,
            analysis=payload.context,
            domain=payload.domain,
        )
        return {"status": "ok", "deep_analysis": deep_analysis}
    except Exception as exc:
        logger.error("Deep insight generation failed: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to generate deep insight.")
