from __future__ import annotations

import unittest

from dashboard_state import DashboardState, decode_payload, infer_device


class DashboardStateTests(unittest.TestCase):
    def test_infers_first_segment_only_for_nested_topics(self) -> None:
        self.assertEqual(infer_device("router01/status"), "router01")
        self.assertEqual(infer_device("/router01/interfaces/wan"), "router01")
        self.assertEqual(infer_device("status"), "Unknown")

    def test_decodes_json_and_binary_without_data_loss(self) -> None:
        text, encoding, is_json, value = decode_payload(b'{"ok":true}')
        self.assertEqual(text, '{"ok":true}')
        self.assertEqual(encoding, "utf-8")
        self.assertTrue(is_json)
        self.assertEqual(value, {"ok": True})

        text, encoding, is_json, value = decode_payload(b"\xff\x00")
        self.assertEqual(text, "0xff00")
        self.assertEqual(encoding, "binary (hex)")
        self.assertFalse(is_json)
        self.assertIsNone(value)

        text, encoding, is_json, value = decode_payload(b"null")
        self.assertEqual(text, "null")
        self.assertEqual(encoding, "utf-8")
        self.assertTrue(is_json)
        self.assertIsNone(value)

    def test_system_topic_populates_system_snapshot(self) -> None:
        state = DashboardState(system_topic_suffix="system")
        payload = (
            b'{"load_avg":{"1min":"0.35","5min":"0.40","15min":"0.51"},'
            b'"disk_usage":{"/opt":null,"ram":"10"}}'
        )
        state.record_message("router01/system", payload)

        snapshot = state.snapshot()
        system = snapshot["system"]
        self.assertEqual(len(system), 1)
        entry = system[0]
        self.assertEqual(entry["device"], "router01")
        self.assertEqual(
            entry["metrics"]["load_avg"],
            {"1min": 0.35, "5min": 0.40, "15min": 0.51},
        )
        self.assertEqual(entry["metrics"]["ram"], 10.0)
        self.assertEqual(
            entry["metrics"]["disks"], [{"mount": "/opt", "value": None}]
        )
        self.assertTrue(entry["online"])
        self.assertNotIn("last_seen_monotonic", entry)
        self.assertEqual(state.system_snapshot(), system)

    def test_non_system_topic_creates_no_system_entry(self) -> None:
        state = DashboardState(system_topic_suffix="system")
        state.record_message("router01/status", b'{"ok":true}')
        self.assertEqual(state.snapshot()["system"], [])
        self.assertEqual(state.system_snapshot(), [])

    def test_restore_rebuilds_aggregates_and_continues_ids(self) -> None:
        state = DashboardState(system_topic_suffix="system")
        restored = [
            {
                "id": 7,
                "timestamp": "2026-06-25T17:00:00.000+00:00",
                "topic": "router01/status",
                "payload": "one",
                "payload_size": 3,
                "payload_encoding": "utf-8",
                "json": None,
                "is_json": False,
                "device": "router01",
                "qos": 0,
                "retain": False,
            },
            {
                "id": 8,
                "timestamp": "2026-06-25T17:00:01.000+00:00",
                "topic": "router01/system",
                "payload": '{"load_avg": {"1min": "0.5"}}',
                "payload_size": 29,
                "payload_encoding": "utf-8",
                "json": {"load_avg": {"1min": "0.5"}},
                "is_json": True,
                "device": "router01",
                "qos": 0,
                "retain": False,
            },
        ]
        state.restore(restored)

        snapshot = state.snapshot()
        self.assertEqual([m["id"] for m in snapshot["messages"]], [8, 7])
        self.assertEqual(
            snapshot["messages"][0]["timestamp"], "2026-06-25T17:00:01.000+00:00"
        )
        self.assertEqual(snapshot["stats"]["unique_topics"], 2)
        self.assertEqual(len(snapshot["devices"]), 1)
        self.assertEqual(len(snapshot["system"]), 1)
        self.assertEqual(snapshot["system"][0]["device"], "router01")
        self.assertEqual(
            snapshot["system"][0]["metrics"]["load_avg"]["1min"], 0.5
        )

        new_message = state.record_message("router01/status", b"two")
        self.assertEqual(new_message["id"], 9)

    def test_bounds_history_and_keeps_aggregates(self) -> None:
        state = DashboardState(message_limit=2)
        state.record_message("router01/status", b"one")
        state.record_message("router01/status", b"two")
        state.record_message("router02/status", b'{"up": true}')

        snapshot = state.snapshot()
        self.assertEqual(len(snapshot["messages"]), 2)
        self.assertEqual(snapshot["stats"]["total_messages"], 3)
        self.assertEqual(snapshot["stats"]["unique_topics"], 2)
        self.assertEqual(snapshot["stats"]["inferred_devices"], 2)
        self.assertEqual(snapshot["messages"][0]["topic"], "router02/status")


if __name__ == "__main__":
    unittest.main()
