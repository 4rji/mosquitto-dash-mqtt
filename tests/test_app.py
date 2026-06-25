from __future__ import annotations

import unittest

from app import create_app
from config import Config


class AppIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app, self.socketio = create_app(
            Config(MQTT_ENABLED=False, SOCKET_BATCH_INTERVAL=0.01),
            start_mqtt=False,
        )
        self.app.config["TESTING"] = True

    def tearDown(self) -> None:
        self.app.extensions["event_batcher"].stop()

    def test_index_and_websocket_snapshot(self) -> None:
        response = self.app.test_client().get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Digi MQTT Monitor", response.data)

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


if __name__ == "__main__":
    unittest.main()

