from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class GeologicalRecord(BaseModel):
    source_file: str
    survey_date: str | None = None
    location_name: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    depth_m: float | None = None
    mineral_type: str | None = None
    grade_value: float | None = None
    grade_unit: str | None = None
    rock_type: str | None = None
    zone_id: str | None = None
    confidence: float = 0.5


class Insight(BaseModel):
    id: str
    severity: str
    category: str
    title: str
    explanation: str
    recommendation: str
    confidence: float = Field(ge=0.0, le=1.0)
    data_refs: list[str] = Field(default_factory=list)
    generated_at: datetime


class AnalysisBundle(BaseModel):
    descriptive: dict[str, Any]
    diagnostic: dict[str, Any]
    predictive: dict[str, Any]
