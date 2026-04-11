from __future__ import annotations

import re
from pathlib import Path


KV_PATTERN = re.compile(r"^\s*([A-Za-z_\-\s]+)\s*[:=]\s*(.+?)\s*$")


def parse_log(path: str | Path) -> dict:
    content = Path(path).read_text(encoding="utf-8", errors="ignore")
    lines = content.splitlines()
    extracted: dict[str, str] = {}

    for line in lines:
        match = KV_PATTERN.match(line)
        if not match:
            continue
        key = match.group(1).strip().lower().replace(" ", "_").replace("-", "_")
        extracted[key] = match.group(2).strip()

    return {
        "text": content,
        "fields": extracted,
        "source": str(path),
    }
