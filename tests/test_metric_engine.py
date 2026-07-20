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
    "value_source": "payload",
    "json_path": "temperature",
    "mode": "point",
}

SERIES_METRIC = {
    "id": "load",
    "label": "Load",
    "topic_filter": "+/system",
    "value_source": "payload",
    "json_path": "load_avg.1min",
    "mode": "series",
}

TOPIC_VALUE_METRIC = {
    "id": "health",
    "label": "Health",
    "topic_filter": "#",
    "value_source": "topic",
    "topic_regex": r"Health (\d+)",
    "mode": "point",
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

    def test_topic_value_source_extracts_from_the_topic_string(self) -> None:
        engine = MetricEngine()
        engine.register(1, [TOPIC_VALUE_METRIC])

        changed = engine.ingest(make_message("Health 100", b"log-status-ok"))

        self.assertEqual(len(changed), 1)
        self.assertEqual(changed[0]["metric"], "health")
        self.assertEqual(changed[0]["value"], 100.0)

        engine.ingest(make_message("Health 0", b"log-status-ok"))
        snapshot = engine.snapshot(1)
        self.assertEqual(len(snapshot), 1)
        self.assertEqual(snapshot[0]["value"], 0.0)

    def test_topic_value_source_skips_non_matching_topics(self) -> None:
        engine = MetricEngine()
        engine.register(1, [TOPIC_VALUE_METRIC])

        changed = engine.ingest(make_message("router01/status", b"ok"))

        self.assertEqual(changed, [])

    def test_topic_value_source_ignores_an_incorrect_topic_filter(self) -> None:
        # An LLM-authored topic_filter like "Health #" is not valid MQTT wildcard
        # syntax for a single-segment topic ("#" only matches whole segments after
        # a "/"). topic_regex alone must still decide relevance in "topic" mode.
        metric = dict(TOPIC_VALUE_METRIC, topic_filter="Health #")
        engine = MetricEngine()
        engine.register(1, [metric])

        changed = engine.ingest(make_message("Health 100", b"log-status-ok"))

        self.assertEqual(len(changed), 1)
        self.assertEqual(changed[0]["value"], 100.0)

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
