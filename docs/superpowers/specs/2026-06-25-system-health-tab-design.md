# System Health Tab — Design

**Date:** 2026-06-25
**Status:** Approved (pending spec review)

## Goal

Add a fourth dashboard tab, **System health**, that renders the latest
system-telemetry payload published by Digi routers on a dedicated topic.
The payload looks like:

```json
{
  "load_avg": {"1min": "0.35", "5min": "0.40", "15min": "0.51"},
  "disk_usage": {"/opt": null, "/etc/config:": null, "ram": "10"}
}
```

Only **current values** are shown — no time series, no charts, no extra storage.
The tab reuses the last-value-per-device aggregation pattern the dashboard
already applies to topics and inferred devices.

## Non-goals (YAGNI)

- No historical trends or charts.
- No per-metric alerting/thresholds.
- No persistence beyond the existing in-memory state.

## Architecture & data flow

```
MQTT  router01/system
   -> MQTTClient.on_message
   -> app.handle_message
   -> DashboardState.record_message
        - existing topic/device aggregates (unchanged)
        - if is_system_topic(topic, suffix) and normalize_metrics(json) is not None:
              _system[device] = { device, metrics, last_seen, last_seen_monotonic, topic }
   -> SocketEventBatcher (1/sec block) emits "system" alongside "stats"
   -> dashboard.js renders cards in the System health tab
```

Identification strategy: **dedicated topic** (chosen). A message is system
telemetry when the topic's last segment equals the configured suffix
(default `system`, e.g. `router01/system`). Device name is derived with the
existing `infer_device(topic)` (first topic segment).

## Components

### 1. `system_metrics.py` (new module, pure functions)

Pure and side-effect free so it is trivially unit-testable under strict TDD.

```python
def is_system_topic(topic: str, suffix: str) -> bool:
    """True when the last non-empty topic segment equals suffix."""

def normalize_metrics(json_value: Any) -> dict | None:
    """Normalize a raw system payload, or None if it is not system telemetry."""
```

Normalized output shape:

```python
{
    "load_avg": {"1min": 0.35, "5min": 0.40, "15min": 0.51},  # float or None
    "ram": 10.0,                                               # float or None
    "disks": [                                                 # dynamic, ordered
        {"mount": "/opt", "value": None},
        {"mount": "/etc/config:", "value": None},
    ],
}
```

Normalization rules:

- Numeric strings (`"0.35"`, `"10"`) are coerced to `float`. Unparseable
  values and JSON `null` become `None`.
- `ram` is extracted out of `disk_usage` (the source payload nests it there
  even though it is not a disk). If absent, `ram` is `None`.
- Every remaining key in `disk_usage` becomes a `disks` entry, preserving
  insertion order. Mount names are dynamic — never hardcoded.
- `load_avg` keys (`1min`/`5min`/`15min`) are each coerced; missing keys map
  to `None`.
- Returns `None` when `json_value` is not a dict, or has neither `load_avg`
  nor `disk_usage` — so non-telemetry traffic on the suffix is ignored safely.

### 2. `DashboardState` changes

- `__init__(... , system_topic_suffix: str = "system")`: store the suffix and
  initialize `self._system: dict[str, dict] = {}`.
- `record_message`: after the existing topic/device aggregate updates, when
  `is_system_topic(topic, suffix)` and `normalize_metrics(json_value)` returns
  a value, upsert `self._system[device]` with the normalized metrics plus
  `last_seen`, `last_seen_monotonic`, and `topic`. Mirrors the existing
  `_devices` upsert pattern.
- `snapshot()`: add `"system": self._system_snapshot_unlocked(now)`.
- New `system_snapshot()` (locked) + `_system_snapshot_unlocked(now)` returning
  a list of public entries (metrics + `online` flag computed from
  `last_seen_monotonic` and `_online_seconds`, `last_seen_monotonic` stripped).

### 3. `app.py` changes

- Pass `settings.SYSTEM_TOPIC_SUFFIX` into `DashboardState`.
- In `SocketEventBatcher._run`, inside the existing 1-second block, also
  `self._socketio.emit("system", self._state.system_snapshot())`. Low rate,
  no per-message overhead; load average changes slowly so 1/sec is ample.

### 4. `config.py` change

- `SYSTEM_TOPIC_SUFFIX: str = os.getenv("SYSTEM_TOPIC_SUFFIX", "system")`.

### 5. Frontend

`templates/index.html`:

- Add a fourth `.view-tab` button (last position) with
  `data-panel="systemPanel"` and a `#systemCountBadge` count.
- Add `<div id="systemPanel" class="dashboard-panel">` containing a
  `#systemGrid` and an empty state. The existing tab-toggle handler in
  `dashboard.js` already activates any `.view-tab` — no change to that logic.

`static/js/dashboard.js`:

- `state.system = new Map()` keyed by device name.
- `snapshot` handler: populate `state.system` from `snapshot.system`.
- New `socket.on("system", ...)`: merge into `state.system`, call
  `renderSystem()`.
- `renderSystem()`: one card per device — header (name + online badge,
  reusing `.device-card` styles), load average (1/5/15 min), RAM, and a list
  of disks (`mount -> value`, `null` shown as `—`).
- Update `#systemCountBadge` in `updateBadges()`.

`static/css/dashboard.css`:

- Minimal additions for the metric card layout, reusing existing
  `.device-card`/`.device-grid` tokens.

## Error handling

- Malformed or non-telemetry payloads on the suffix topic: `normalize_metrics`
  returns `None` and the message is treated as ordinary traffic (no system
  entry created). No exceptions surface to the MQTT callback.
- `null` and unparseable values render as `—` in the UI rather than failing.
- Binary/non-JSON payloads never reach normalization (`json_value` is `None`).

## Testing (strict TDD — tests first)

`tests/test_system_metrics.py` (new):

- `is_system_topic`: matches when last segment equals suffix; rejects
  non-matching and single-segment topics.
- `normalize_metrics`:
  - Coerces numeric strings to floats for `load_avg`.
  - Maps `null` and unparseable strings to `None`.
  - Extracts `ram` from `disk_usage`; remaining keys become ordered `disks`.
  - Returns `None` for non-dict input and for dicts lacking both keys.
  - Handles the exact real-world payload above end-to-end.

`tests/test_dashboard_state.py` (additions):

- A message on a system topic populates `snapshot()["system"]` with normalized
  metrics and an `online` flag.
- A message on a non-system topic does not create a system entry.

## Configuration summary

| Variable | Default | Purpose |
| --- | --- | --- |
| `SYSTEM_TOPIC_SUFFIX` | `system` | Last topic segment identifying system telemetry |
