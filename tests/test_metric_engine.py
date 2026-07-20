from __future__ import annotations

import unittest

from dashboard_state import decode_payload
from metric_engine import MetricEngine


def make_message(
    topic: str,
    payload: bytes,
    timestamp: str = "2026-07-20T10:00:00.000+00:00",
) -> dict:
    text, encoding, is_json, json_value = decode_payload(payload)
    return {
        "topic": topic,
        "payload": text,
        "payload_encoding": encoding,
        "is_json": is_json,
        "json": json_value,
        "timestamp": timestamp,
    }


POINT_METRIC = {
    "id": "temp",
    "label": "Temperature",
    "topic_filter": "+/temperature",
    "json_path": "temperature",
    "mode": "point",
}

SERIES_METRIC = {
    "id": "load",
    "label": "Load",
    "topic_filter": "+/system",
    "json_path": "load_avg.1min",
    "mode": "series",
}


class MetricEngineTests(unittest.TestCase):
    def test_point_mode_groups_latest_value_by_device(self) -> None:
        engine = MetricEngine()
        engine.register(1, [POINT_METRIC])

        changed = engine.ingest(make_message("router01/temperature", b'{"temperature": 21.5}'))

        self.assertEqual(len(changed), 1)
        self.assertEqual(changed[0]["dashboard_id"], 1)
        self.assertEqual(changed[0]["metric"], "temp")
        self.assertEqual(changed[0]["group"], "router01")
        self.assertEqual(changed[0]["value"], 21.5)
        self.assertEqual(engine.snapshot(1), changed)

        engine.ingest(make_message("router01/temperature", b'{"temperature": 22.0}'))
        snapshot = engine.snapshot(1)
        self.assertEqual(len(snapshot), 1)
        self.assertEqual(snapshot[0]["value"], 22.0)

    def test_series_mode_appends_and_caps_buffer(self) -> None:
        engine = MetricEngine(series_maxlen=2)
        engine.register(1, [SERIES_METRIC])

        for value in (1, 2, 3):
            payload = f'{{"load_avg": {{"1min": {value}}}}}'.encode()
            engine.ingest(make_message("router01/system", payload))

        snapshot = engine.snapshot(1)
        self.assertEqual([row["value"] for row in snapshot], [2, 3])

    def test_non_matching_topic_is_ignored(self) -> None:
        engine = MetricEngine()
        engine.register(1, [POINT_METRIC])

        changed = engine.ingest(make_message("router01/status", b'{"temperature": 21.5}'))

        self.assertEqual(changed, [])
        self.assertEqual(engine.snapshot(1), [])

    def test_missing_json_path_is_skipped_without_error(self) -> None:
        engine = MetricEngine()
        engine.register(1, [POINT_METRIC])

        changed = engine.ingest(make_message("router01/temperature", b'{"other": 1}'))

        self.assertEqual(changed, [])

    def test_unregister_stops_future_updates(self) -> None:
        engine = MetricEngine()
        engine.register(1, [POINT_METRIC])
        engine.ingest(make_message("router01/temperature", b'{"temperature": 21.5}'))

        engine.unregister(1)
        changed = engine.ingest(make_message("router01/temperature", b'{"temperature": 30}'))

        self.assertEqual(changed, [])
        self.assertEqual(engine.snapshot(1), [])


if __name__ == "__main__":
    unittest.main()
