from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ai_dashboard import (
    AIDashboardError,
    AIProviderError,
    build_context,
    generate_dashboard,
    validate_response,
)
from dashboard_state import DashboardState
from message_store import MessageStore


class FakeOpenAIClient:
    def __init__(self, responses: list[str]) -> None:
        self._responses = responses
        self.calls: list[tuple[str, str]] = []

    def generate(self, system_prompt: str, user_prompt: str) -> str:
        self.calls.append((system_prompt, user_prompt))
        return self._responses[len(self.calls) - 1]


class RaisingClient:
    def generate(self, system_prompt: str, user_prompt: str) -> str:
        raise RuntimeError("connection refused")


VALID_RESPONSE = json.dumps(
    {
        "spec": {"data": {"name": "table"}, "mark": "line"},
        "metrics": [
            {
                "id": "temp_router01",
                "label": "Temperature",
                "topic_filter": "+/temperature",
                "json_path": "temperature",
                "mode": "point",
            }
        ],
    }
)


class BuildContextTests(unittest.TestCase):
    def test_dedupes_by_topic_keeping_latest_payload(self) -> None:
        state = DashboardState()
        state.record_message("router01/temperature", b'{"temperature": 20}')
        state.record_message("router01/temperature", b'{"temperature": 21}')
        state.record_message("router02/temperature", b'{"temperature": 30}')

        context = build_context(None, state)

        self.assertIn('router01/temperature -> {"temperature": 21}', context)
        self.assertNotIn('"temperature": 20', context)
        self.assertIn('router02/temperature -> {"temperature": 30}', context)

    def test_caps_distinct_topics(self) -> None:
        state = DashboardState()
        for i in range(5):
            state.record_message(f"router{i}/temperature", b'{"temperature": 1}')

        context = build_context(None, state, max_topics=2)

        self.assertEqual(len(context.splitlines()), 2)

    def test_uses_message_store_when_provided(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(str(Path(tmp) / "test.db"))
            store.append(
                {
                    "id": 1,
                    "timestamp": "2026-07-20T10:00:00.000+00:00",
                    "topic": "router01/temperature",
                    "payload": '{"temperature": 22}',
                    "payload_size": 20,
                    "payload_encoding": "utf-8",
                    "json": {"temperature": 22},
                    "is_json": True,
                    "device": "router01",
                    "qos": 0,
                    "retain": False,
                }
            )
            state = DashboardState()

            context = build_context(store, state)

            self.assertIn('router01/temperature -> {"temperature": 22}', context)
            store.close()


class ValidateResponseTests(unittest.TestCase):
    def test_accepts_a_well_formed_response(self) -> None:
        payload = json.loads(VALID_RESPONSE)
        result = validate_response(payload)
        self.assertEqual(result, payload)

    def test_rejects_missing_table_data_name(self) -> None:
        payload = json.loads(VALID_RESPONSE)
        payload["spec"]["data"]["name"] = "wrong"
        with self.assertRaises(AIDashboardError):
            validate_response(payload)

    def test_rejects_duplicate_metric_ids(self) -> None:
        payload = json.loads(VALID_RESPONSE)
        payload["metrics"].append(dict(payload["metrics"][0]))
        with self.assertRaises(AIDashboardError):
            validate_response(payload)

    def test_rejects_invalid_mode(self) -> None:
        payload = json.loads(VALID_RESPONSE)
        payload["metrics"][0]["mode"] = "average"
        with self.assertRaises(AIDashboardError):
            validate_response(payload)

    def test_rejects_empty_metrics(self) -> None:
        payload = json.loads(VALID_RESPONSE)
        payload["metrics"] = []
        with self.assertRaises(AIDashboardError):
            validate_response(payload)


class GenerateDashboardTests(unittest.TestCase):
    def test_returns_validated_payload_on_first_try(self) -> None:
        client = FakeOpenAIClient([VALID_RESPONSE])
        result = generate_dashboard("chart it", "router01/temperature -> 20", client)
        self.assertEqual(result["metrics"][0]["id"], "temp_router01")
        self.assertEqual(len(client.calls), 1)

    def test_retries_once_then_succeeds(self) -> None:
        client = FakeOpenAIClient(["not json", VALID_RESPONSE])
        result = generate_dashboard("chart it", "context", client)
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(result["metrics"][0]["id"], "temp_router01")

    def test_raises_ai_dashboard_error_after_second_failure(self) -> None:
        client = FakeOpenAIClient(["not json", "still not json"])
        with self.assertRaises(AIDashboardError):
            generate_dashboard("chart it", "context", client)
        self.assertEqual(len(client.calls), 2)

    def test_wraps_client_errors_as_provider_error(self) -> None:
        with self.assertRaises(AIProviderError):
            generate_dashboard("chart it", "context", RaisingClient())


if __name__ == "__main__":
    unittest.main()
