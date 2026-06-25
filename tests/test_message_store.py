from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from message_store import MessageStore


def make_message(message_id: int, **overrides: object) -> dict[str, object]:
    message = {
        "id": message_id,
        "timestamp": f"2026-06-25T17:00:0{message_id}.000+00:00",
        "topic": f"router0{message_id}/system",
        "payload": '{"ok": true}',
        "payload_size": 12,
        "payload_encoding": "utf-8",
        "json": {"ok": True},
        "is_json": True,
        "device": f"router0{message_id}",
        "qos": 0,
        "retain": False,
    }
    message.update(overrides)
    return message


class MessageStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.path = str(Path(self._dir.name) / "test.db")

    def tearDown(self) -> None:
        self._dir.cleanup()

    def test_append_then_recent_returns_oldest_first(self) -> None:
        store = MessageStore(self.path)
        store.append(make_message(1))
        store.append(make_message(2))
        store.append(make_message(3))

        rows = store.recent(10)
        self.assertEqual([row["id"] for row in rows], [1, 2, 3])
        self.assertEqual(rows[0]["topic"], "router01/system")
        self.assertIs(rows[0]["is_json"], True)
        self.assertIs(rows[0]["retain"], False)
        store.close()

    def test_recent_limits_to_newest(self) -> None:
        store = MessageStore(self.path)
        for i in range(1, 6):
            store.append(make_message(i))

        rows = store.recent(2)
        self.assertEqual([row["id"] for row in rows], [4, 5])
        store.close()

    def test_json_round_trips_and_non_json_stores_null(self) -> None:
        store = MessageStore(self.path)
        store.append(make_message(1, json={"a": [1, 2]}, is_json=True))
        store.append(
            make_message(2, json=None, is_json=False, payload="0xff00",
                         payload_encoding="binary (hex)")
        )

        rows = store.recent(10)
        self.assertEqual(rows[0]["json"], {"a": [1, 2]})
        self.assertIsNone(rows[1]["json"])
        self.assertIs(rows[1]["is_json"], False)
        store.close()

    def test_retention_prunes_oldest(self) -> None:
        store = MessageStore(self.path, retention=3)
        for i in range(1, 6):
            store.append(make_message(i))

        rows = store.recent(100)
        self.assertEqual([row["id"] for row in rows], [3, 4, 5])
        store.close()

    def test_persists_across_reopen(self) -> None:
        store = MessageStore(self.path)
        store.append(make_message(1))
        store.append(make_message(2))
        store.close()

        reopened = MessageStore(self.path)
        rows = reopened.recent(100)
        self.assertEqual([row["id"] for row in rows], [1, 2])
        reopened.close()


if __name__ == "__main__":
    unittest.main()
