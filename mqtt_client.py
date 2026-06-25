"""Mosquitto client isolated from Flask request handling."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Any

import paho.mqtt.client as mqtt

from config import Config

logger = logging.getLogger(__name__)

MessageCallback = Callable[[str, bytes, int, bool], None]
StatusCallback = Callable[[bool, str], None]


class MQTTClient:
    """Lifecycle wrapper around paho-mqtt with automatic reconnection."""

    def __init__(
        self,
        config: Config,
        on_message: MessageCallback,
        on_status: StatusCallback,
    ) -> None:
        self._config = config
        self._on_message_callback = on_message
        self._on_status_callback = on_status
        self._started = False
        self._lock = threading.Lock()

        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id="",
            protocol=mqtt.MQTTv311,
        )
        if config.MQTT_USERNAME:
            self._client.username_pw_set(
                config.MQTT_USERNAME,
                config.MQTT_PASSWORD or None,
            )

        self._client.reconnect_delay_set(min_delay=1, max_delay=30)
        self._client.on_connect = self._on_connect
        self._client.on_connect_fail = self._on_connect_fail
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            detail = f"Connecting to {self._config.MQTT_HOST}:{self._config.MQTT_PORT}"
            self._on_status_callback(False, detail)
            self._client.connect_async(
                self._config.MQTT_HOST,
                self._config.MQTT_PORT,
                self._config.MQTT_KEEPALIVE,
            )
            self._client.loop_start()

    def stop(self) -> None:
        with self._lock:
            if not self._started:
                return
            self._started = False
            try:
                self._client.disconnect()
            finally:
                self._client.loop_stop()
                self._on_status_callback(False, "MQTT client stopped")

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del userdata, flags, properties
        if reason_code.is_failure:
            detail = f"Connection rejected: {reason_code}"
            logger.warning(detail)
            self._on_status_callback(False, detail)
            return

        result, _mid = client.subscribe(self._config.MQTT_TOPIC)
        if result != mqtt.MQTT_ERR_SUCCESS:
            detail = f"Connected, but subscription failed ({result})"
            logger.error(detail)
            self._on_status_callback(False, detail)
            return

        detail = (
            f"Connected to {self._config.MQTT_HOST}:{self._config.MQTT_PORT} "
            f"· subscribed to {self._config.MQTT_TOPIC}"
        )
        logger.info(detail)
        self._on_status_callback(True, detail)

    def _on_connect_fail(self, client: mqtt.Client, userdata: Any) -> None:
        del client, userdata
        detail = f"Unable to reach {self._config.MQTT_HOST}:{self._config.MQTT_PORT}"
        logger.warning(detail)
        self._on_status_callback(False, detail)

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del client, userdata, disconnect_flags, properties
        if self._started:
            detail = f"Disconnected ({reason_code}); reconnecting automatically"
        else:
            detail = "MQTT client stopped"
        logger.warning(detail)
        self._on_status_callback(False, detail)

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: Any,
        message: mqtt.MQTTMessage,
    ) -> None:
        del client, userdata
        self._on_message_callback(
            message.topic,
            bytes(message.payload),
            message.qos,
            message.retain,
        )
