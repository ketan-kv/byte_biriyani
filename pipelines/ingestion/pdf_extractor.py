from __future__ import annotations

from pathlib import Path

import pdfplumber


def extract_pdf(path: str | Path) -> dict:
    source = str(path)
    with pdfplumber.open(source) as pdf:
        text = "\n".join(page.extract_text() or "" for page in pdf.pages)
        tables = [table for page in pdf.pages for table in (page.extract_tables() or [])]
    return {"text": text, "tables": tables, "source": source}
