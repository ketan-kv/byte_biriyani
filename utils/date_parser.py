from __future__ import annotations

from dateutil import parser


def normalize_date(raw: str | None) -> str | None:
    if not raw:
        return None
    try:
        return parser.parse(raw).strftime("%Y-%m-%d")
    except (TypeError, ValueError, OverflowError):
        return None
