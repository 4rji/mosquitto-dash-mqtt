from __future__ import annotations

import json
import unittest

from system_metrics import is_system_topic, normalize_metrics, system_device_name


REAL_PAYLOAD = {
    "load_avg": {"1min": "0.35", "5min": "0.40", "15min": "0.51"},
    "disk_usage": {"/opt": None, "/etc/config:": None, "ram": "10"},
}


class IsSystemTopicTests(unittest.TestCase):
    def test_matches_when_last_segment_equals_suffix(self) -> None:
        self.assertTrue(is_system_topic("router01/system", "system"))
        self.assertTrue(is_system_topic("router01/system/", "system"))
        self.assertTrue(is_system_topic("system", "system"))

    def test_rejects_non_matching_or_empty(self) -> None:
        self.assertFalse(is_system_topic("router01/status", "system"))
        self.assertFalse(is_system_topic("router01/system/wan", "system"))
        self.assertFalse(is_system_topic("", "system"))


class NormalizeMetricsTests(unittest.TestCase):
    def test_normalizes_the_real_world_payload(self) -> None:
        result = normalize_metrics(REAL_PAYLOAD)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(
            result["load_avg"], {"1min": 0.35, "5min": 0.40, "15min": 0.51}
        )
        self.assertEqual(result["ram"], 10.0)
        self.assertEqual(
            result["disks"],
            [
                {"mount": "/opt", "value": None},
                {"mount": "/etc/config:", "value": None},
            ],
        )

    def test_coerces_numeric_strings_and_keeps_null_as_none(self) -> None:
        payload = {
            "load_avg": {"1min": "1.5", "5min": "bad", "15min": None},
            "disk_usage": {"/opt": "50", "/var": None, "ram": "bad"},
        }
        result = normalize_metrics(payload)
        assert result is not None
        self.assertEqual(result["load_avg"], {"1min": 1.5, "5min": None, "15min": None})
        self.assertIsNone(result["ram"])
        self.assertEqual(
            result["disks"],
            [{"mount": "/opt", "value": 50.0}, {"mount": "/var", "value": None}],
        )

    def test_missing_load_avg_keys_become_none(self) -> None:
        result = normalize_metrics({"load_avg": {"1min": "0.1"}})
        assert result is not None
        self.assertEqual(
            result["load_avg"], {"1min": 0.1, "5min": None, "15min": None}
        )
        self.assertIsNone(result["ram"])
        self.assertEqual(result["disks"], [])

    def test_only_disk_usage_present(self) -> None:
        result = normalize_metrics({"disk_usage": {"/opt": "10", "ram": "20"}})
        assert result is not None
        self.assertEqual(
            result["load_avg"], {"1min": None, "5min": None, "15min": None}
        )
        self.assertEqual(result["ram"], 20.0)
        self.assertEqual(result["disks"], [{"mount": "/opt", "value": 10.0}])

    def test_returns_none_for_non_telemetry(self) -> None:
        self.assertIsNone(normalize_metrics("hello"))
        self.assertIsNone(normalize_metrics(None))
        self.assertIsNone(normalize_metrics([1, 2, 3]))
        self.assertIsNone(normalize_metrics({"foo": "bar"}))

    def test_handles_double_encoded_json_string(self) -> None:
        # Some routers publish a JSON string that itself contains JSON.
        inner = json.dumps(REAL_PAYLOAD)
        result = normalize_metrics(inner)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["ram"], 10.0)
        self.assertEqual(result["load_avg"]["1min"], 0.35)

    def test_plain_string_is_still_rejected(self) -> None:
        self.assertIsNone(normalize_metrics("just text"))
        self.assertIsNone(normalize_metrics('"a bare json string"'))


class SystemDeviceNameTests(unittest.TestCase):
    def test_uses_segment_before_the_suffix(self) -> None:
        self.assertEqual(
            system_device_name("event/router/WR64-003536/system", "system"),
            "WR64-003536",
        )
        self.assertEqual(system_device_name("router01/system", "system"), "router01")

    def test_falls_back_to_unknown_without_a_segment_before(self) -> None:
        self.assertEqual(system_device_name("system", "system"), "Unknown")
        self.assertEqual(system_device_name("", "system"), "Unknown")


if __name__ == "__main__":
    unittest.main()
