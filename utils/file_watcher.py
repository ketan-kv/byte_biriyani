from __future__ import annotations

from pathlib import Path
from typing import Callable

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from pipelines.ingestion.ingestion_router import detect_file_type
from utils.logger import get_logger


logger = get_logger("amdais.file_watcher")


class DataDropHandler(FileSystemEventHandler):
    def __init__(self, callback: Callable[[str, str], None]) -> None:
        super().__init__()
        self.callback = callback

    def on_created(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        src = str(event.src_path)
        file_type = detect_file_type(src)
        logger.info("Detected new file: %s (%s)", src, file_type)
        self.callback(src, file_type)


def start_file_watcher(watch_paths: list[str], callback: Callable[[str, str], None]) -> Observer:
    observer = Observer()
    handler = DataDropHandler(callback)

    for raw in watch_paths:
        path = Path(raw)
        path.mkdir(parents=True, exist_ok=True)
        observer.schedule(handler, str(path), recursive=False)

    observer.start()
    logger.info("File watcher started for %d paths", len(watch_paths))
    return observer
