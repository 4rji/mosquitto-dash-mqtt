from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from app import create_app
from config import Config

_VALID_RESPONSE = json.dumps(
    {
        "spec": {"data": {"name": "table"}, "mark": "line"},
        "metrics": [
            {
                "id": "temp_router01",
                "label": "Temperature",
                "topic_filter": "+/temperature",
                "value_source": "payload",
                "json_path": "temperature",
                "mode": "point",
            }
        ],
    }
)


class _FakeOpenAIClient:
    def __init__(self, responses: list[str]) -> None:
        self._responses = responses
        self.calls = 0

    def generate(self, system_prompt: str, user_prompt: str) -> str:
        response = self._responses[self.calls]
        self.calls += 1
        return response


class AppIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        db_path = str(Path(self._dir.name) / "test.db")
        self.app, self.socketio = create_app(
            Config(MQTT_ENABLED=False, SOCKET_BATCH_INTERVAL=0.01, LOG_DB_PATH=db_path),
            start_mqtt=False,
        )
        self.app.config["TESTING"] = True

    def tearDown(self) -> None:
        self.app.extensions["event_batcher"].stop()
        self._dir.cleanup()

    def test_index_and_websocket_snapshot(self) -> None:
        response = self.app.test_client().get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Digi MQTT Monitor", response.data)
        self.assertIn(b'data-panel="dashboardsPanel"', response.data)
        self.assertIn(b'id="triDashboardRoot"', response.data)
        self.assertIn(b"tri-dashboards.js", response.data)

        client = self.socketio.test_client(self.app)
        events = client.get_received()
        snapshot_events = [event for event in events if event["name"] == "snapshot"]
        self.assertEqual(len(snapshot_events), 1)
        self.assertEqual(snapshot_events[0]["args"][0]["messages"], [])
        client.disconnect()

    def test_message_is_delivered_in_a_batch(self) -> None:
        client = self.socketio.test_client(self.app)
        client.get_received()

        state = self.app.extensions["dashboard_state"]
        batcher = self.app.extensions["event_batcher"]
        message = state.record_message("router01/status", b'{"online": true}')
        batcher.push(message)
        self.socketio.sleep(0.03)

        events = client.get_received()
        batches = [event for event in events if event["name"] == "mqtt_messages"]
        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0]["args"][0][0]["device"], "router01")
        self.assertTrue(batches[0]["args"][0][0]["is_json"])
        client.disconnect()


class AIDashboardRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        db_path = str(Path(self._dir.name) / "test.db")
        self.fake_client = _FakeOpenAIClient([_VALID_RESPONSE])
        self.app, self.socketio = create_app(
            Config(MQTT_ENABLED=False, SOCKET_BATCH_INTERVAL=0.01, LOG_DB_PATH=db_path),
            start_mqtt=False,
            openai_client=self.fake_client,
        )
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.app.extensions["event_batcher"].stop()
        self._dir.cleanup()

    def test_create_list_get_delete_round_trip(self) -> None:
        response = self.client.post(
            "/api/ai-dashboards", json={"name": "Temps", "prompt": "chart temperature"}
        )
        self.assertEqual(response.status_code, 201)
        dashboard_id = response.get_json()["id"]

        listed = self.client.get("/api/ai-dashboards").get_json()
        self.assertEqual([d["id"] for d in listed], [dashboard_id])

        fetched = self.client.get(f"/api/ai-dashboards/{dashboard_id}").get_json()
        self.assertEqual(fetched["metrics"][0]["id"], "temp_router01")
        self.assertEqual(fetched["initial_data"], [])

        deleted = self.client.delete(f"/api/ai-dashboards/{dashboard_id}")
        self.assertEqual(deleted.status_code, 204)
        self.assertEqual(
            self.client.get(f"/api/ai-dashboards/{dashboard_id}").status_code, 404
        )

    def test_missing_fields_return_400(self) -> None:
        response = self.client.post("/api/ai-dashboards", json={"name": "", "prompt": ""})
        self.assertEqual(response.status_code, 400)

    def test_live_mqtt_message_pushes_ai_metrics_event(self) -> None:
        self.client.post(
            "/api/ai-dashboards", json={"name": "Temps", "prompt": "chart temperature"}
        )
        ws_client = self.socketio.test_client(self.app)
        ws_client.get_received()

        state = self.app.extensions["dashboard_state"]
        engine = self.app.extensions["metric_engine"]
        batcher = self.app.extensions["event_batcher"]

        message = state.record_message("router01/temperature", b'{"temperature": 21.5}')
        rows = engine.ingest(message)
        batcher.push_ai_metrics(rows)
        self.socketio.sleep(0.03)

        events = ws_client.get_received()
        ai_events = [event for event in events if event["name"] == "ai_metrics"]
        self.assertEqual(len(ai_events), 1)
        self.assertEqual(ai_events[0]["args"][0][0]["value"], 21.5)
        ws_client.disconnect()


class AIDashboardWithoutClientTests(unittest.TestCase):
    def test_returns_503_without_a_configured_client(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app, socketio = create_app(
                Config(
                    MQTT_ENABLED=False,
                    SOCKET_BATCH_INTERVAL=0.01,
                    LOG_DB_PATH=str(Path(tmp) / "t.db"),
                    OPENAI_API_KEY="",
                ),
                start_mqtt=False,
                openai_client=None,
            )
            response = app.test_client().post(
                "/api/ai-dashboards", json={"name": "Temps", "prompt": "chart it"}
            )
            self.assertEqual(response.status_code, 503)
            app.extensions["event_batcher"].stop()


if __name__ == "__main__":
    unittest.main()
