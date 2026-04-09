from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from api.schemas import ChatRequest, ChatResponse


router = APIRouter(tags=["chat"])


@router.post("/chat", response_model=ChatResponse)
def chat(payload: ChatRequest, request: Request) -> ChatResponse:
    copilot = getattr(request.app.state, "decision_copilot", None)
    if copilot is None:
        raise HTTPException(status_code=500, detail="Decision copilot is not initialized")

    try:
        result = copilot.handle_message(payload.message, payload.session_id)
        return ChatResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Chat processing failed: {exc}") from exc