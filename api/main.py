from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from api.routes.analytics import router as analytics_router
from api.routes.chat import router as chat_router
from api.routes.domain_pipeline import router as domain_pipeline_router
from api.routes.ingest import router as ingest_router
from api.routes.insight_deep import router as insight_deep_router
from api.routes.insights import router as insights_router
from api.routes.pipeline import router as pipeline_router


def create_app(orchestrator, config: dict) -> FastAPI:
    app = FastAPI(title="AMDAIS API", version="0.1.0")
    app.state.orchestrator = orchestrator
    app.state.config = config

    project_root = Path(__file__).resolve().parents[1]
    web_dir = project_root / "webapp"

    @app.get("/", include_in_schema=False)
    def web_root():
        if web_dir.exists():
            return FileResponse(web_dir / "index.html")
        return {
            "status": "ok",
            "message": "Web frontend folder not found. Check /webapp in project root.",
        }

    @app.get("/app", include_in_schema=False)
    def web_app():
        if web_dir.exists() and (web_dir / "app.html").exists():
            return FileResponse(web_dir / "app.html")
        return {
            "status": "ok",
            "message": "App page not found. Check /webapp/app.html.",
        }

    @app.get("/about", include_in_schema=False)
    def web_about():
        if web_dir.exists() and (web_dir / "about.html").exists():
            return FileResponse(web_dir / "about.html")
        return {
            "status": "ok",
            "message": "About page not found. Check /webapp/about.html.",
        }

    react_dir = web_dir / "react"
    if react_dir.exists():
        app.mount("/react", StaticFiles(directory=react_dir, html=True), name="react")

    if web_dir.exists():
        app.mount("/static", StaticFiles(directory=web_dir), name="static")

    @app.get("/health", tags=["system"])
    def health() -> dict:
        return {"status": "healthy", "service": "amdais"}

    app.include_router(ingest_router)
    app.include_router(pipeline_router)
    app.include_router(insights_router)
    app.include_router(insight_deep_router)
    app.include_router(analytics_router)
    app.include_router(domain_pipeline_router)
    app.include_router(chat_router)
    return app

