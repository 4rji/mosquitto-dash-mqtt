"""Flask entry point for the Digi MQTT dashboard."""

from __future__ import annotations

import logging
import threading
from collections import deque
from typing import Any

from flask import Flask, jsonify, render_template, request
from flask_socketio import SocketIO, emit

from ai_dashboard import (
    AIDashboardError,
    AIProviderError,
    OpenAIChatClient,
    OpenAIClient,
    build_context,
    generate_dashboard,
)
from ai_dashboard_store import AIDashboardStore
from config import Config
from dashboard_state import DashboardState
from message_store import MessageStore
from metric_engine import MetricEngine
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
        self._ai_pending: deque[dict[str, Any]] = deque()
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

    def push_ai_metrics(self, rows: list[dict[str, Any]]) -> None:
        with self._lock:
            self._ai_pending.extend(rows)

    def _drain(self) -> list[dict[str, Any]]:
        with self._lock:
            messages = list(self._pending)
            self._pending.clear()
            return messages

    def _drain_ai(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = list(self._ai_pending)
            self._ai_pending.clear()
            return rows

    def _run(self) -> None:
        stats_elapsed = 0.0
        while self._running:
            self._socketio.sleep(self._interval)
            messages = self._drain()
            if messages:
                self._socketio.emit("mqtt_messages", messages)

            ai_rows = self._drain_ai()
            if ai_rows:
                self._socketio.emit("ai_metrics", ai_rows)

            stats_elapsed += self._interval
            if stats_elapsed >= 1.0:
                stats_elapsed = 0.0
                self._socketio.emit("stats", self._state.stats())
                self._socketio.emit("system", self._state.system_snapshot())


def create_app(
    config: Config | None = None,
    *,
    start_mqtt: bool = True,
    openai_client: OpenAIClient | None = None,
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
        system_topic_suffix=settings.SYSTEM_TOPIC_SUFFIX,
    )
    batcher = SocketEventBatcher(socketio, state, settings.SOCKET_BATCH_INTERVAL)

    store: MessageStore | None = None
    if settings.LOG_PERSISTENCE_ENABLED:
        store = MessageStore(settings.LOG_DB_PATH, settings.LOG_RETENTION)
        restored = store.recent(settings.MESSAGE_LIMIT)
        state.restore(restored)
        logger.info("Restored %d messages from %s", len(restored), settings.LOG_DB_PATH)

    ai_store = AIDashboardStore(settings.LOG_DB_PATH)
    metric_engine = MetricEngine(series_maxlen=settings.AI_METRIC_SERIES_MAXLEN)
    for row in ai_store.list():
        saved = ai_store.get(row["id"])
        if saved is not None:
            metric_engine.register(saved["id"], saved["metrics"])

    resolved_openai_client = openai_client
    if resolved_openai_client is None and settings.OPENAI_API_KEY:
        resolved_openai_client = OpenAIChatClient(settings.OPENAI_API_KEY, settings.OPENAI_MODEL)

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
        if store is not None:
            store.append(message)
        batcher.push(message)
        ai_rows = metric_engine.ingest(message)
        if ai_rows:
            batcher.push_ai_metrics(ai_rows)

    mqtt_client = MQTTClient(settings, handle_message, handle_status)
    app.extensions["dashboard_state"] = state
    app.extensions["event_batcher"] = batcher
    app.extensions["mqtt_client"] = mqtt_client
    app.extensions["message_store"] = store
    app.extensions["ai_dashboard_store"] = ai_store
    app.extensions["metric_engine"] = metric_engine
    app.extensions["openai_client"] = resolved_openai_client

    @app.get("/")
    def index() -> str:
        return render_template(
            "index.html",
            mqtt_host=settings.MQTT_HOST,
            mqtt_port=settings.MQTT_PORT,
            message_limit=settings.MESSAGE_LIMIT,
            device_online_seconds=settings.DEVICE_ONLINE_SECONDS,
        )

    @app.post("/api/ai-dashboards")
    def create_ai_dashboard():
        if resolved_openai_client is None:
            return jsonify({"error": "AI dashboard generation not configured"}), 503

        body = request.get_json(silent=True) or {}
        name = (body.get("name") or "").strip()
        prompt = (body.get("prompt") or "").strip()
        if not name or not prompt:
            return jsonify({"error": "name and prompt are required"}), 400

        context = build_context(
            store, state, settings.AI_DASHBOARD_SAMPLE_SIZE, settings.AI_DASHBOARD_MAX_TOPICS
        )
        try:
            result = generate_dashboard(prompt, context, resolved_openai_client)
        except AIProviderError as error:
            return jsonify({"error": f"OpenAI request failed: {error}"}), 502
        except AIDashboardError as error:
            return jsonify({"error": str(error)}), 422

        saved = ai_store.save(name, prompt, result["spec"], result["metrics"])
        metric_engine.register(saved["id"], saved["metrics"])
        return jsonify(saved), 201

    @app.get("/api/ai-dashboards")
    def list_ai_dashboards():
        return jsonify(ai_store.list())

    @app.get("/api/ai-dashboards/<int:dashboard_id>")
    def get_ai_dashboard(dashboard_id: int):
        dashboard = ai_store.get(dashboard_id)
        if dashboard is None:
            return jsonify({"error": "not found"}), 404
        dashboard["initial_data"] = metric_engine.snapshot(dashboard_id)
        return jsonify(dashboard)

    @app.delete("/api/ai-dashboards/<int:dashboard_id>")
    def delete_ai_dashboard(dashboard_id: int):
        ai_store.delete(dashboard_id)
        metric_engine.unregister(dashboard_id)
        return "", 204

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
    store: MessageStore | None = app.extensions["message_store"]
    ai_store: AIDashboardStore = app.extensions["ai_dashboard_store"]

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
        if store is not None:
            store.close()
        ai_store.close()


if __name__ == "__main__":
    main()
