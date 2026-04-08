from __future__ import annotations

from fastapi import FastAPI

from api.routes.analytics import router as analytics_router
from api.routes.ingest import router as ingest_router
from api.routes.insights import router as insights_router
from api.routes.pipeline import router as pipeline_router


def create_app(orchestrator, config: dict) -> FastAPI:
    app = FastAPI(title="AMDAIS API", version="0.1.0")
    app.state.orchestrator = orchestrator
    app.state.config = config

    @app.get("/health", tags=["system"])
    def health() -> dict:
        return {"status": "healthy", "service": "amdais"}

    app.include_router(ingest_router)
    app.include_router(pipeline_router)
    app.include_router(insights_router)
    app.include_router(analytics_router)
    return app
