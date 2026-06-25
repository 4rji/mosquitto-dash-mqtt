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
