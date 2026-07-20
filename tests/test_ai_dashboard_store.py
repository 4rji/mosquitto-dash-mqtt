from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ai_dashboard_store import AIDashboardStore

SPEC = {"data": {"name": "table"}, "mark": "line"}
METRICS = [
    {
        "id": "temp",
        "label": "Temperature",
        "topic_filter": "+/temperature",
        "json_path": "temperature",
        "mode": "point",
    }
]


class AIDashboardStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.path = str(Path(self._dir.name) / "test.db")

    def tearDown(self) -> None:
        self._dir.cleanup()

    def test_save_then_get_round_trips_spec_and_metrics(self) -> None:
        store = AIDashboardStore(self.path)
        saved = store.save("Router temps", "chart temperature", SPEC, METRICS)

        fetched = store.get(saved["id"])
        self.assertEqual(fetched["name"], "Router temps")
        self.assertEqual(fetched["prompt"], "chart temperature")
        self.assertEqual(fetched["spec"], SPEC)
        self.assertEqual(fetched["metrics"], METRICS)
        store.close()

    def test_get_returns_none_for_missing_id(self) -> None:
        store = AIDashboardStore(self.path)
        self.assertIsNone(store.get(999))
        store.close()

    def test_list_returns_lightweight_rows_in_creation_order(self) -> None:
        store = AIDashboardStore(self.path)
        first = store.save("First", "prompt one", SPEC, METRICS)
        second = store.save("Second", "prompt two", SPEC, METRICS)

        rows = store.list()

        self.assertEqual([row["id"] for row in rows], [first["id"], second["id"]])
        self.assertNotIn("spec", rows[0])
        self.assertNotIn("metrics", rows[0])
        store.close()

    def test_delete_removes_the_row(self) -> None:
        store = AIDashboardStore(self.path)
        saved = store.save("First", "prompt", SPEC, METRICS)

        store.delete(saved["id"])

        self.assertIsNone(store.get(saved["id"]))
        store.close()

    def test_persists_across_reopen(self) -> None:
        store = AIDashboardStore(self.path)
        saved = store.save("First", "prompt", SPEC, METRICS)
        store.close()

        reopened = AIDashboardStore(self.path)
        fetched = reopened.get(saved["id"])
        self.assertEqual(fetched["name"], "First")
        reopened.close()


if __name__ == "__main__":
    unittest.main()
