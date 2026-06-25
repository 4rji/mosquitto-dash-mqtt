"""SQLite persistence for MQTT messages, safe under the threaded app."""

from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any

_SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id               INTEGER PRIMARY KEY,
    timestamp        TEXT NOT NULL,
    topic            TEXT NOT NULL,
    payload          TEXT NOT NULL,
    payload_size     INTEGER NOT NULL,
    payload_encoding TEXT NOT NULL,
    is_json          INTEGER NOT NULL,
    json_text        TEXT,
    device           TEXT NOT NULL,
    qos              INTEGER NOT NULL,
    retain           INTEGER NOT NULL
);
"""

_COLUMNS = (
    "id",
    "timestamp",
    "topic",
    "payload",
    "payload_size",
    "payload_encoding",
    "is_json",
    "json_text",
    "device",
    "qos",
    "retain",
)


class MessageStore:
    """Write-through archive of MQTT messages with count-based retention."""

    def __init__(self, path: str, retention: int = 100_000) -> None:
        self._retention = retention
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def append(self, message: dict[str, Any]) -> None:
        """Persist one message and prune anything beyond the retention window."""
        json_value = message.get("json")
        json_text = None if json_value is None else json.dumps(json_value)
        row = (
            message["id"],
            message["timestamp"],
            message["topic"],
            message["payload"],
            message["payload_size"],
            message["payload_encoding"],
            1 if message["is_json"] else 0,
            json_text,
            message["device"],
            message["qos"],
            1 if message["retain"] else 0,
        )
        placeholders = ", ".join("?" for _ in _COLUMNS)
        with self._lock:
            self._conn.execute(
                f"INSERT OR REPLACE INTO messages ({', '.join(_COLUMNS)}) "
                f"VALUES ({placeholders})",
                row,
            )
            self._conn.execute(
                "DELETE FROM messages WHERE id <= "
                "(SELECT MAX(id) FROM messages) - ?",
                (self._retention,),
            )
            self._conn.commit()

    def recent(self, limit: int) -> list[dict[str, Any]]:
        """Return up to ``limit`` newest messages, oldest-first."""
        with self._lock:
            cursor = self._conn.execute(
                "SELECT * FROM messages ORDER BY id DESC LIMIT ?", (limit,)
            )
            rows = cursor.fetchall()
        return [self._row_to_message(row) for row in reversed(rows)]

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @staticmethod
    def _row_to_message(row: sqlite3.Row) -> dict[str, Any]:
        json_text = row["json_text"]
        try:
            json_value = None if json_text is None else json.loads(json_text)
        except (json.JSONDecodeError, ValueError):
            json_value = None
        return {
            "id": row["id"],
            "timestamp": row["timestamp"],
            "topic": row["topic"],
            "payload": row["payload"],
            "payload_size": row["payload_size"],
            "payload_encoding": row["payload_encoding"],
            "json": json_value,
            "is_json": bool(row["is_json"]),
            "device": row["device"],
            "qos": row["qos"],
            "retain": bool(row["retain"]),
        }
