from __future__ import annotations

from pathlib import Path

import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler

from agents.orchestrator_agent import OrchestratorAgent
from api.main import create_app
from storage.db import init_db
from utils.config_loader import load_config
from utils.file_watcher import start_file_watcher
from utils.logger import get_logger


logger = get_logger("amdais.main")


def build_runtime() -> tuple[dict, OrchestratorAgent, BackgroundScheduler, object]:
    config = load_config()
    paths = config.get("paths", {})

    sqlite_path = Path(paths.get("sqlite_path", "data/structured/mineral_db.sqlite"))
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    init_db(sqlite_path)

    orchestrator = OrchestratorAgent(config)

    scheduler = BackgroundScheduler()
    for hour in config.get("scheduler", {}).get("run_hours", [6, 18]):
        scheduler.add_job(orchestrator.run_pipeline, "cron", hour=hour)
    scheduler.add_job(
        orchestrator.watch_sensor_anomaly,
        "interval",
        seconds=config.get("scheduler", {}).get("anomaly_poll_seconds", 30),
    )
    scheduler.start()

    watch_paths = [
        paths.get("geological_raw", "data/raw/geological"),
        paths.get("sensor_raw", "data/raw/sensor"),
        paths.get("production_raw", "data/raw/production"),
        paths.get("incidents_raw", "data/raw/incidents"),
    ]
    observer = start_file_watcher(watch_paths, orchestrator.on_new_file)
    return config, orchestrator, scheduler, observer


def run() -> None:
    config, orchestrator, scheduler, observer = build_runtime()
    app = create_app(orchestrator, config)

    try:
        logger.info("Starting FastAPI server on 127.0.0.1:8000")
        uvicorn.run(app, host="127.0.0.1", port=8000)
    finally:
        observer.stop()
        observer.join(timeout=5)
        scheduler.shutdown(wait=False)
        logger.info("AMDAIS shutdown complete")


if __name__ == "__main__":
    run()
