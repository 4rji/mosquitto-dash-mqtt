"""Thread-safe in-memory state for the MQTT dashboard."""

from __future__ import annotations

import json
import threading
import time
from collections import deque
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from system_metrics import (
    is_system_topic,
    normalize_metrics,
    system_device_name,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"Unsupported JSON constant: {value}")


def infer_device(topic: str) -> str:
    """Infer a device from the first segment of a multi-segment topic."""
    segments = [segment for segment in topic.split("/") if segment]
    return segments[0] if len(segments) >= 2 else "Unknown"


def decode_payload(payload: bytes) -> tuple[str, str, bool, Any | None]:
    """Return a lossless display value, encoding label, and parsed JSON value."""
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        return f"0x{payload.hex()}", "binary (hex)", False, None

    try:
        parsed = json.loads(text, parse_constant=_reject_json_constant)
    except (json.JSONDecodeError, ValueError):
        return text, "utf-8", False, None
    return text, "utf-8", True, parsed


class DashboardState:
    """Owns bounded message history and topic/device aggregates."""

    def __init__(
        self,
        message_limit: int = 1000,
        online_seconds: int = 60,
        system_topic_suffix: str = "system",
    ) -> None:
        self._lock = threading.RLock()
        self._messages: deque[dict[str, Any]] = deque(maxlen=message_limit)
        self._message_times: deque[float] = deque()
        self._topics: dict[str, dict[str, Any]] = {}
        self._devices: dict[str, dict[str, Any]] = {}
        self._system: dict[str, dict[str, Any]] = {}
        self._system_topic_suffix = system_topic_suffix
        self._total_messages = 0
        self._next_id = 1
        self._mqtt_connected = False
        self._mqtt_status_detail = "Waiting for MQTT client"
        self._started_at = time.monotonic()
        self._online_seconds = online_seconds

    def set_mqtt_status(self, connected: bool, detail: str) -> dict[str, Any]:
        with self._lock:
            self._mqtt_connected = connected
            self._mqtt_status_detail = detail
            return self._status_unlocked()

    def record_message(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: int = 0,
        retain: bool = False,
    ) -> dict[str, Any]:
        """Record one broker message and return its browser-safe representation."""
        timestamp = _utc_now()
        received_monotonic = time.monotonic()
        payload_text, encoding, is_json, json_value = decode_payload(payload)
        device = infer_device(topic)

        with self._lock:
            message_id = self._next_id
            self._next_id += 1
            self._total_messages += 1
            self._message_times.append(received_monotonic)
            self._prune_rate_window_unlocked(received_monotonic)

            message = {
                "id": message_id,
                "timestamp": timestamp,
                "topic": topic,
                "payload": payload_text,
                "payload_size": len(payload),
                "payload_encoding": encoding,
                "json": json_value,
                "is_json": is_json,
                "device": device,
                "qos": qos,
                "retain": retain,
            }
            self._apply_message_unlocked(message, received_monotonic)
            return deepcopy(message)

    def restore(self, messages: list[dict[str, Any]]) -> None:
        """Rebuild in-memory aggregates from persisted messages (oldest-first).

        Preserves each message's id and timestamp, and resumes id generation
        after the highest restored id. Does not touch the live rate window or
        the session message counter.
        """
        with self._lock:
            now = time.monotonic()
            max_id = 0
            for message in messages:
                self._apply_message_unlocked(message, now)
                max_id = max(max_id, message["id"])
            if max_id:
                self._next_id = max_id + 1

    def _apply_message_unlocked(
        self, message: dict[str, Any], received_monotonic: float
    ) -> None:
        """Fold one message into the feed, topic, device, and system aggregates."""
        topic = message["topic"]
        timestamp = message["timestamp"]
        device = message["device"]
        payload_text = message["payload"]
        encoding = message["payload_encoding"]
        is_json = message["is_json"]
        json_value = message["json"]

        self._messages.append(message)

        topic_entry = self._topics.setdefault(
            topic,
            {
                "topic": topic,
                "message_count": 0,
                "last_updated": timestamp,
                "last_payload": "",
                "payload_size": 0,
                "payload_encoding": "utf-8",
                "json": None,
                "is_json": False,
            },
        )
        topic_entry.update(
            {
                "message_count": topic_entry["message_count"] + 1,
                "last_updated": timestamp,
                "last_payload": payload_text,
                "payload_size": message["payload_size"],
                "payload_encoding": encoding,
                "json": json_value,
                "is_json": is_json,
            }
        )

        device_entry = self._devices.setdefault(
            device,
            {
                "name": device,
                "last_seen": timestamp,
                "last_seen_monotonic": received_monotonic,
                "total_messages": 0,
                "last_topic": topic,
                "last_payload": payload_text,
            },
        )
        device_entry.update(
            {
                "last_seen": timestamp,
                "last_seen_monotonic": received_monotonic,
                "total_messages": device_entry["total_messages"] + 1,
                "last_topic": topic,
                "last_payload": payload_text,
            }
        )

        if is_system_topic(topic, self._system_topic_suffix):
            metrics = normalize_metrics(json_value)
            if metrics is not None:
                sys_device = system_device_name(topic, self._system_topic_suffix)
                self._system[sys_device] = {
                    "device": sys_device,
                    "metrics": metrics,
                    "last_seen": timestamp,
                    "last_seen_monotonic": received_monotonic,
                    "topic": topic,
                }

    def stats(self) -> dict[str, Any]:
        with self._lock:
            self._prune_rate_window_unlocked(time.monotonic())
            return self._stats_unlocked()

    def snapshot(self) -> dict[str, Any]:
        """Return a consistent initial state for a newly connected browser."""
        with self._lock:
            now = time.monotonic()
            self._prune_rate_window_unlocked(now)
            devices = [
                self._public_device_unlocked(device, now)
                for device in self._devices.values()
            ]
            return {
                "messages": list(reversed(deepcopy(self._messages))),
                "topics": deepcopy(list(self._topics.values())),
                "devices": deepcopy(devices),
                "system": self._system_snapshot_unlocked(now),
                "stats": self._stats_unlocked(),
                "status": self._status_unlocked(),
            }

    def system_snapshot(self) -> list[dict[str, Any]]:
        """Return the latest normalized metrics per system device."""
        with self._lock:
            return self._system_snapshot_unlocked(time.monotonic())

    def _system_snapshot_unlocked(self, now: float) -> list[dict[str, Any]]:
        snapshot = []
        for entry in self._system.values():
            public = {
                key: value
                for key, value in entry.items()
                if key != "last_seen_monotonic"
            }
            public["online"] = (
                now - entry["last_seen_monotonic"] <= self._online_seconds
            )
            snapshot.append(deepcopy(public))
        return snapshot

    def _prune_rate_window_unlocked(self, now: float) -> None:
        cutoff = now - 1.0
        while self._message_times and self._message_times[0] < cutoff:
            self._message_times.popleft()

    def _stats_unlocked(self) -> dict[str, Any]:
        return {
            "mqtt_connected": self._mqtt_connected,
            "total_messages": self._total_messages,
            "messages_per_second": len(self._message_times),
            "unique_topics": len(self._topics),
            "inferred_devices": sum(name != "Unknown" for name in self._devices),
            "last_message_timestamp": (
                self._messages[-1]["timestamp"] if self._messages else None
            ),
            "uptime_seconds": int(time.monotonic() - self._started_at),
        }

    def _status_unlocked(self) -> dict[str, Any]:
        return {
            "connected": self._mqtt_connected,
            "detail": self._mqtt_status_detail,
        }

    def _public_device_unlocked(
        self, device: dict[str, Any], now: float
    ) -> dict[str, Any]:
        public = {key: value for key, value in device.items() if key != "last_seen_monotonic"}
        public["online"] = now - device["last_seen_monotonic"] <= self._online_seconds
        return public
