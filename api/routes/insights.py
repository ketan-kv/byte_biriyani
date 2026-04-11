from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, Request


router = APIRouter(tags=["insights"])


@router.get("/insights")
def get_insights(request: Request) -> dict:
    config = request.app.state.config
    path = Path(config["paths"]["insights_path"])
    if not path.exists():
        return {"count": 0, "items": []}
    items = json.loads(path.read_text(encoding="utf-8"))
    return {"count": len(items), "items": items}
