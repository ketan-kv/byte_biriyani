from __future__ import annotations

from pydantic import BaseModel, Field, model_validator


class IngestRequest(BaseModel):
    path: str
    file_type: str | None = None


class RunPipelineResponse(BaseModel):
    insights_generated: int
    status: str


class DatabaseDomainPipelineRequest(BaseModel):
    database_url: str
    query: str | None = None
    table_name: str | None = None
    row_limit: int = Field(default=200000, ge=100, le=1000000)
    user_preferences: dict = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_query_or_table(self) -> "DatabaseDomainPipelineRequest":
        has_query = bool(self.query and self.query.strip())
        has_table = bool(self.table_name and self.table_name.strip())
        if has_query == has_table:
            raise ValueError("Provide exactly one of query or table_name")
        return self


class ChatRequest(BaseModel):
    message: str = Field(min_length=1, max_length=2000)
    session_id: str | None = None


class ChatAction(BaseModel):
    description: str
    reasoning: str
    expected_impact: str


class ChatResponse(BaseModel):
    type: str
    message: str
    data: dict = Field(default_factory=dict)
    chart: dict | None = None
    actions: list[ChatAction] = Field(default_factory=list)
    intent: str | None = None
    session_id: str | None = None
