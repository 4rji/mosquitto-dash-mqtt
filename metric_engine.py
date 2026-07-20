"""Live metric extraction for AI-generated dashboards."""

from __future__ import annotations

import re
import threading
from collections import deque
from typing import Any

from paho.mqtt.client import topic_matches_sub

from dashboard_state import infer_device
from system_metrics import coerce_number


class MetricEngine:
    """Thread-safe registry of metric extractors and their rolling buffers."""

    def __init__(self, series_maxlen: int = 200) -> None:
        self._lock = threading.RLock()
        self._series_maxlen = series_maxlen
        self._metrics: dict[int, list[dict[str, Any]]] = {}
        self._point_values: dict[tuple[int, str, str], dict[str, Any]] = {}
        self._series_values: dict[tuple[int, str], deque[dict[str, Any]]] = {}

    def register(self, dashboard_id: int, metrics: list[dict[str, Any]]) -> None:
        with self._lock:
            self._metrics[dashboard_id] = metrics

    def unregister(self, dashboard_id: int) -> None:
        with self._lock:
            self._metrics.pop(dashboard_id, None)
            for key in [k for k in self._point_values if k[0] == dashboard_id]:
                del self._point_values[key]
            for key in [k for k in self._series_values if k[0] == dashboard_id]:
                del self._series_values[key]

    def ingest(self, message: dict[str, Any]) -> list[dict[str, Any]]:
        topic = message["topic"]
        json_value = message.get("json")
        group = infer_device(topic)
        changed: list[dict[str, Any]] = []

        with self._lock:
            for dashboard_id, metrics in self._metrics.items():
                for metric in metrics:
                    is_topic_sourced = metric.get("value_source") == "topic"
                    # topic_regex already does full-string matching against the topic,
                    # so it alone decides relevance here — an LLM-authored topic_filter
                    # is easy to get syntactically wrong for single-segment topics
                    # (MQTT wildcards match whole '/'-separated segments, not substrings)
                    # and would otherwise silently mask an otherwise-correct topic_regex.
                    if not is_topic_sourced and not topic_matches_sub(metric["topic_filter"], topic):
                        continue
                    if is_topic_sourced:
                        raw_value = _extract_from_topic(topic, metric["topic_regex"])
                    else:
                        raw_value = _extract(json_value, metric["json_path"])
                    value = coerce_number(raw_value)
                    if value is None:
                        continue
                    row = {
                        "dashboard_id": dashboard_id,
                        "metric": metric["id"],
                        "group": group,
                        "value": value,
                        "ts": message["timestamp"],
                    }
                    if metric["mode"] == "point":
                        self._point_values[(dashboard_id, metric["id"], group)] = row
                    else:
                        key = (dashboard_id, metric["id"])
                        buffer = self._series_values.setdefault(
                            key, deque(maxlen=self._series_maxlen)
                        )
                        buffer.append(row)
                    changed.append(row)
        return changed

    def snapshot(self, dashboard_id: int) -> list[dict[str, Any]]:
        with self._lock:
            rows = [
                row for key, row in self._point_values.items() if key[0] == dashboard_id
            ]
            for key, buffer in self._series_values.items():
                if key[0] == dashboard_id:
                    rows.extend(buffer)
            return rows


def _extract(value: Any, path: str) -> Any:
    """Dot-path lookup. Returns None if any segment is missing or the value
    at that point isn't a dict — treated as "no value" by the caller, not
    an error (payloads matching a topic_filter are not all shaped alike)."""
    for segment in path.split("."):
        if not isinstance(value, dict) or segment not in value:
            return None
        value = value[segment]
    return value


def _extract_from_topic(topic: str, pattern: str) -> Any:
    """Regex lookup against the topic string itself, for devices that embed
    a value in the topic rather than the payload (e.g. topic "Health 100").
    Returns the first capturing group, or None if the pattern doesn't match."""
    match = re.search(pattern, topic)
    return match.group(1) if match else None
