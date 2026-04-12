from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Optional

import psycopg
from psycopg.rows import dict_row

from config import Settings


class Database:
    def __init__(self, settings: Settings):
        if not settings.database_url:
            raise RuntimeError("DATABASE_URL is not set.")
        self.database_url = settings.database_url

    def connect(self) -> psycopg.Connection:
        return psycopg.connect(self.database_url, row_factory=dict_row)

    def fetch_one(self, query: str, params: Optional[Iterable[Any]] = None) -> Optional[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                row = cur.fetchone()
                return dict(row) if row else None

    def fetch_all(self, query: str, params: Optional[Iterable[Any]] = None) -> list[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                return [dict(row) for row in cur.fetchall()]

    def execute(self, query: str, params: Optional[Iterable[Any]] = None) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or ())
            conn.commit()

    def execute_returning(self, query: str, params: Optional[Iterable[Any]] = None) -> Optional[dict[str, Any]]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None

    def initialize_schema(self) -> None:
        schema_dir = Path(__file__).resolve().parent / "schemas"
        schema_files = [
            schema_dir / "core_schema.sql",
            schema_dir / "trades_schema.sql",
            schema_dir / "bounties_schema.sql",
        ]
        with self.connect() as conn:
            with conn.cursor() as cur:
                for schema_file in schema_files:
                    sql = schema_file.read_text(encoding="utf-8")
                    if sql.strip():
                        cur.execute(sql)
            conn.commit()

    def healthcheck(self) -> dict[str, Any]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT NOW() AS server_time, version() AS server_version")
                row = cur.fetchone()
                return dict(row) if row else {"ok": False}
