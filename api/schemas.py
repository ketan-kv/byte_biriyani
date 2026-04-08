from __future__ import annotations

from pydantic import BaseModel


class IngestRequest(BaseModel):
    path: str
    file_type: str | None = None


class RunPipelineResponse(BaseModel):
    insights_generated: int
    status: str
