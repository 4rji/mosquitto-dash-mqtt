"""Pure helpers for normalizing Digi router system-telemetry payloads."""

from __future__ import annotations

import json
from typing import Any

_LOAD_KEYS = ("1min", "5min", "15min")
_RAM_KEY = "ram"
_MAX_UNWRAP = 3


def is_system_topic(topic: str, suffix: str) -> bool:
    """True when the last non-empty topic segment equals ``suffix``."""
    segments = [segment for segment in topic.split("/") if segment]
    return bool(segments) and segments[-1] == suffix


def system_device_name(topic: str, suffix: str) -> str:
    """Device label for a system topic: the segment before the suffix.

    e.g. ``event/router/WR64-003536/system`` -> ``WR64-003536``.
    """
    segments = [segment for segment in topic.split("/") if segment]
    if len(segments) >= 2 and segments[-1] == suffix:
        return segments[-2]
    return "Unknown"


def _maybe_unwrap_json(value: Any) -> Any:
    """Decode JSON strings that themselves contain JSON (double-encoded)."""
    for _ in range(_MAX_UNWRAP):
        if not isinstance(value, str):
            return value
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return value
    return value


def coerce_number(value: Any) -> float | None:
    """Coerce a numeric string to ``float``; anything else becomes ``None``."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def normalize_metrics(json_value: Any) -> dict[str, Any] | None:
    """Normalize a raw system payload, or ``None`` if it is not telemetry."""
    json_value = _maybe_unwrap_json(json_value)
    if not isinstance(json_value, dict):
        return None

    raw_load = json_value.get("load_avg")
    raw_disk = json_value.get("disk_usage")
    if not isinstance(raw_load, dict) and not isinstance(raw_disk, dict):
        return None

    load_source = raw_load if isinstance(raw_load, dict) else {}
    load_avg = {key: coerce_number(load_source.get(key)) for key in _LOAD_KEYS}

    disk_source = raw_disk if isinstance(raw_disk, dict) else {}
    ram = coerce_number(disk_source.get(_RAM_KEY)) if _RAM_KEY in disk_source else None
    disks = [
        {"mount": mount, "value": coerce_number(value)}
        for mount, value in disk_source.items()
        if mount != _RAM_KEY
    ]

    return {"load_avg": load_avg, "ram": ram, "disks": disks}
