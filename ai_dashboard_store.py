"""SQLite persistence for AI-generated dashboards."""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ai_dashboards (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL,
    prompt       TEXT NOT NULL,
    spec_json    TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    created_at   TEXT NOT NULL
);
"""


class AIDashboardStore:
    """Write-through archive of AI-generated dashboard definitions."""

    def __init__(self, path: str) -> None:
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def save(
        self, name: str, prompt: str, spec: dict[str, Any], metrics: list[dict[str, Any]]
    ) -> dict[str, Any]:
        created_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        with self._lock:
            cursor = self._conn.execute(
                "INSERT INTO ai_dashboards (name, prompt, spec_json, metrics_json, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (name, prompt, json.dumps(spec), json.dumps(metrics), created_at),
            )
            self._conn.commit()
            dashboard_id = cursor.lastrowid
        return {
            "id": dashboard_id,
            "name": name,
            "prompt": prompt,
            "spec": spec,
            "metrics": metrics,
            "created_at": created_at,
        }

    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            cursor = self._conn.execute(
                "SELECT id, name, prompt, created_at FROM ai_dashboards ORDER BY id"
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def get(self, dashboard_id: int) -> dict[str, Any] | None:
        with self._lock:
            cursor = self._conn.execute(
                "SELECT * FROM ai_dashboards WHERE id = ?", (dashboard_id,)
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return {
            "id": row["id"],
            "name": row["name"],
            "prompt": row["prompt"],
            "spec": json.loads(row["spec_json"]),
            "metrics": json.loads(row["metrics_json"]),
            "created_at": row["created_at"],
        }

    def delete(self, dashboard_id: int) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM ai_dashboards WHERE id = ?", (dashboard_id,))
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()
