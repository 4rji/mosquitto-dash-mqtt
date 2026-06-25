"""Flask entry point for the Digi MQTT dashboard."""

from __future__ import annotations

import logging
import threading
from collections import deque
from typing import Any

from flask import Flask, render_template
from flask_socketio import SocketIO, emit

from config import Config
from dashboard_state import DashboardState
from mqtt_client import MQTTClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


class SocketEventBatcher:
    """Coalesce MQTT events to reduce WebSocket and DOM overhead."""

    def __init__(
        self,
        socketio: SocketIO,
        state: DashboardState,
        interval: float = 0.05,
    ) -> None:
        self._socketio = socketio
        self._state = state
        self._interval = interval
        self._pending: deque[dict[str, Any]] = deque()
        self._lock = threading.Lock()
        self._running = False
        self._task: Any = None

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = self._socketio.start_background_task(self._run)

    def stop(self) -> None:
        self._running = False

    def push(self, message: dict[str, Any]) -> None:
        with self._lock:
            self._pending.append(message)

    def _drain(self) -> list[dict[str, Any]]:
        with self._lock:
            messages = list(self._pending)
            self._pending.clear()
            return messages

    def _run(self) -> None:
        stats_elapsed = 0.0
        while self._running:
            self._socketio.sleep(self._interval)
            messages = self._drain()
            if messages:
                self._socketio.emit("mqtt_messages", messages)

            stats_elapsed += self._interval
            if stats_elapsed >= 1.0:
                stats_elapsed = 0.0
                self._socketio.emit("stats", self._state.stats())


def create_app(
    config: Config | None = None,
    *,
    start_mqtt: bool = True,
) -> tuple[Flask, SocketIO]:
    """Create the web application and its owned services."""
    settings = config or Config()
    app = Flask(__name__)
    app.config["SECRET_KEY"] = settings.SECRET_KEY

    socketio = SocketIO(
        app,
        async_mode="threading",
        cors_allowed_origins=None,
        logger=False,
        engineio_logger=False,
    )
    state = DashboardState(
        message_limit=settings.MESSAGE_LIMIT,
        online_seconds=settings.DEVICE_ONLINE_SECONDS,
    )
    batcher = SocketEventBatcher(socketio, state, settings.SOCKET_BATCH_INTERVAL)

    def handle_status(connected: bool, detail: str) -> None:
        status = state.set_mqtt_status(connected, detail)
        socketio.emit("mqtt_status", status)

    def handle_message(topic: str, payload: bytes, qos: int, retain: bool) -> None:
        message = state.record_message(
            topic,
            payload,
            qos=qos,
            retain=retain,
        )
        batcher.push(message)

    mqtt_client = MQTTClient(settings, handle_message, handle_status)
    app.extensions["dashboard_state"] = state
    app.extensions["event_batcher"] = batcher
    app.extensions["mqtt_client"] = mqtt_client

    @app.get("/")
    def index() -> str:
        return render_template(
            "index.html",
            mqtt_host=settings.MQTT_HOST,
            mqtt_port=settings.MQTT_PORT,
            message_limit=settings.MESSAGE_LIMIT,
            device_online_seconds=settings.DEVICE_ONLINE_SECONDS,
        )

    @socketio.on("connect")
    def socket_connect() -> None:
        emit("snapshot", state.snapshot())

    batcher.start()
    if start_mqtt and settings.MQTT_ENABLED:
        mqtt_client.start()
    elif not settings.MQTT_ENABLED:
        handle_status(False, "MQTT disabled by configuration")

    return app, socketio


def main() -> None:
    settings = Config()
    app, socketio = create_app(settings)
    mqtt_client: MQTTClient = app.extensions["mqtt_client"]
    batcher: SocketEventBatcher = app.extensions["event_batcher"]

    try:
        socketio.run(
            app,
            host=settings.APP_HOST,
            port=settings.APP_PORT,
            debug=settings.APP_DEBUG,
            use_reloader=False,
            allow_unsafe_werkzeug=True,
        )
    finally:
        mqtt_client.stop()
        batcher.stop()


if __name__ == "__main__":
    main()
