from __future__ import annotations

import re
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row

from .config import Settings


SELECT_RE = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)
FORBIDDEN_RE = re.compile(r"\b(insert|update|delete|merge|alter|drop|create|truncate|grant|revoke|vacuum|copy\s+[^()]*\s+from)\b", re.IGNORECASE)


class ReadOnlyDatabase:
    def __init__(self, settings: Settings):
        self.settings = settings

    @contextmanager
    def connect(self) -> Iterator[psycopg.Connection[Any]]:
        if not self.settings.orion_db_password:
            raise RuntimeError("ORION_DB_PASSWORD is not set.")
        conn = psycopg.connect(
            host=self.settings.orion_db_host,
            port=self.settings.orion_db_port,
            dbname=self.settings.orion_db_name,
            user=self.settings.orion_db_user,
            password=self.settings.orion_db_password,
            sslmode=self.settings.orion_db_sslmode,
            connect_timeout=self.settings.orion_db_connect_timeout,
            row_factory=dict_row,
            options="-c default_transaction_read_only=on -c statement_timeout=30000",
        )
        try:
            with conn.transaction():
                yield conn
        finally:
            conn.close()

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self._assert_read_only(sql)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or {})
                rows = cur.fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _assert_read_only(sql: str) -> None:
        if not SELECT_RE.search(sql):
            raise ValueError("Only SELECT/CTE queries are allowed.")
        if FORBIDDEN_RE.search(sql):
            raise ValueError("Query contains a forbidden mutating keyword.")
