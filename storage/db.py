from __future__ import annotations

import sqlite3
from pathlib import Path


DEFAULT_DB_PATH = Path("data/structured/mineral_db.sqlite")
MIGRATION_FILE = Path("storage/migrations/001_init_tables.sql")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def get_connection(db_path: str | Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    path = Path(db_path)
    ensure_parent(path)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str | Path = DEFAULT_DB_PATH) -> None:
    conn = get_connection(db_path)
    try:
        sql = MIGRATION_FILE.read_text(encoding="utf-8")
        conn.executescript(sql)
        conn.commit()
    finally:
        conn.close()


def insert_many(table_name: str, records: list[dict], db_path: str | Path = DEFAULT_DB_PATH) -> int:
    if not records:
        return 0

    conn = get_connection(db_path)
    try:
        keys = list(records[0].keys())
        cols = ", ".join(keys)
        placeholders = ", ".join(["?"] * len(keys))
        query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        values = [tuple(record.get(k) for k in keys) for record in records]
        conn.executemany(query, values)
        conn.commit()
        return len(values)
    finally:
        conn.close()
