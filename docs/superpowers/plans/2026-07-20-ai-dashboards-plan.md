# AI-Generated Dashboards Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a user describe a chart in a prompt and have OpenAI turn it into a live Vega-Lite dashboard, driven by custom metrics extracted from the existing MQTT message stream.

**Architecture:** A new `ai_dashboard.py` module samples recent MQTT traffic and calls OpenAI for a `{spec, metrics}` JSON payload; a new `MetricEngine` evaluates each metric's MQTT-wildcard `topic_filter` + JSON dot-path against every live message (same pipeline `DashboardState` already sees) and produces rows the existing `SocketEventBatcher` emits as a new `ai_metrics` socket event; a new `AIDashboardStore` persists generated dashboards to the same SQLite file `MessageStore` already uses, in a new table.

**Tech Stack:** Flask / Flask-SocketIO (existing), `openai` Python SDK (new dependency), `paho-mqtt`'s `topic_matches_sub` (existing dependency, new use), Vega-Lite + vega-embed via CDN (new frontend dependency, no build step).

## Global Constraints

- Python 3.12+, four-space indentation, type hints, module docstrings, `snake_case`/`PascalCase` — match `AGENTS.md` / `CLAUDE.md` conventions already in the repo.
- No formatter or linter is configured — match surrounding style.
- Tests never hit a live external service. Existing tests keep MQTT out via `Config(MQTT_ENABLED=False)`; the same rule now applies to OpenAI — always inject a fake/stub client, never the real `OpenAIChatClient`, in tests.
- `unittest`, file `test_<module>.py`, class `*Tests`, method `test_<behavior>`.
- The LLM's output is declarative JSON only (a Vega-Lite spec + metric definitions) — never executed as code, client- or server-side.
- AI dashboards read only from custom metric extractors (`topic_filter` + `json_path` against live message JSON) — never from the existing fixed aggregates (`devices`/`topics`/`stats`/`system`).
- `json_path` is dot-notation only, no array indexing (e.g. `load_avg.1min`, not `items[0].value`).
- Frontend has no build step and no framework — vanilla JS IIFE modules, matching `static/js/dashboard.js`.

---

### Task 1: Dependencies and configuration

**Files:**
- Modify: `requirements.txt`
- Modify: `config.py:44-46`
- Modify: `.env.example:13-14`
- Modify: `README.md` (configuration table)

**Interfaces:**
- Produces: `Config.OPENAI_API_KEY: str`, `Config.OPENAI_MODEL: str`, `Config.AI_DASHBOARD_SAMPLE_SIZE: int`, `Config.AI_DASHBOARD_MAX_TOPICS: int`, `Config.AI_METRIC_SERIES_MAXLEN: int` — consumed by Tasks 2, 3, and 5.

- [ ] **Step 1: Add the `openai` dependency**

In `requirements.txt`, insert alphabetically between `Flask-SocketIO` and `paho-mqtt`:

```
Flask>=3.0,<4
Flask-SocketIO>=5.3,<6
openai>=2.0,<3
paho-mqtt>=2.1,<3
python-dotenv>=1.0,<2
simple-websocket>=1.0,<2
```

- [ ] **Step 2: Install it**

Run: `pip install -r requirements.txt`
Expected: `openai` installs alongside the existing dependencies with no conflicts.

- [ ] **Step 3: Add the new config fields**

In `config.py`, the file currently ends with:

```python
    LOG_PERSISTENCE_ENABLED: bool = _env_bool("LOG_PERSISTENCE_ENABLED", True)
    LOG_DB_PATH: str = os.getenv("LOG_DB_PATH", "mqtt_dashboard.db")
    LOG_RETENTION: int = int(os.getenv("LOG_RETENTION", "100000"))
```

Add immediately after `LOG_RETENTION`:

```python
    LOG_PERSISTENCE_ENABLED: bool = _env_bool("LOG_PERSISTENCE_ENABLED", True)
    LOG_DB_PATH: str = os.getenv("LOG_DB_PATH", "mqtt_dashboard.db")
    LOG_RETENTION: int = int(os.getenv("LOG_RETENTION", "100000"))

    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    AI_DASHBOARD_SAMPLE_SIZE: int = int(os.getenv("AI_DASHBOARD_SAMPLE_SIZE", "50"))
    AI_DASHBOARD_MAX_TOPICS: int = int(os.getenv("AI_DASHBOARD_MAX_TOPICS", "30"))
    AI_METRIC_SERIES_MAXLEN: int = int(os.getenv("AI_METRIC_SERIES_MAXLEN", "200"))
```

- [ ] **Step 4: Verify the config loads with the new defaults**

Run: `python3 -c "from config import Config; c = Config(); print(c.OPENAI_MODEL, c.AI_DASHBOARD_SAMPLE_SIZE, c.AI_METRIC_SERIES_MAXLEN)"`
Expected: `gpt-4o-mini 50 200`

- [ ] **Step 5: Document the new variables**

In `.env.example`, the file currently ends with:

```
MESSAGE_LIMIT=1000
DEVICE_ONLINE_SECONDS=60
```

Append:

```
MESSAGE_LIMIT=1000
DEVICE_ONLINE_SECONDS=60

OPENAI_API_KEY=
OPENAI_MODEL=gpt-4o-mini
AI_DASHBOARD_SAMPLE_SIZE=50
AI_DASHBOARD_MAX_TOPICS=30
AI_METRIC_SERIES_MAXLEN=200
```

In `README.md`, the configuration table currently has this row order ending in `LOG_RETENTION` before `APP_HOST`:

```
| `LOG_RETENTION` | `100000` | Maximum messages retained in SQLite (oldest pruned) |
| `APP_HOST` | `0.0.0.0` | Web server bind address |
```

Insert new rows between them:

```
| `LOG_RETENTION` | `100000` | Maximum messages retained in SQLite (oldest pruned) |
| `OPENAI_API_KEY` | empty | OpenAI API key; AI dashboard generation is disabled (503) when unset |
| `OPENAI_MODEL` | `gpt-4o-mini` | Model used for AI dashboard generation |
| `AI_DASHBOARD_SAMPLE_SIZE` | `50` | Recent messages sampled for AI dashboard prompt context |
| `AI_DASHBOARD_MAX_TOPICS` | `30` | Max distinct topics included in AI dashboard prompt context |
| `AI_METRIC_SERIES_MAXLEN` | `200` | Max points kept per `series`-mode AI dashboard metric |
| `APP_HOST` | `0.0.0.0` | Web server bind address |
```

- [ ] **Step 6: Commit**

```bash
git add requirements.txt config.py .env.example README.md
git commit -m "Add OpenAI dependency and AI dashboard configuration"
```

---

### Task 2: `ai_dashboard.py` — context sampling and OpenAI generation

**Files:**
- Create: `ai_dashboard.py`
- Test: `tests/test_ai_dashboard.py`

**Interfaces:**
- Consumes: `DashboardState.snapshot()["messages"]` (`dashboard_state.py`), `MessageStore.recent(limit: int) -> list[dict]` (`message_store.py`).
- Produces: `build_context(store, state, sample_size=50, max_topics=30) -> str`, `generate_dashboard(prompt: str, context: str, client) -> dict`, `validate_response(payload: Any) -> dict`, `class AIDashboardError(Exception)`, `class AIProviderError(Exception)`, `class OpenAIChatClient` (real client, constructed with `api_key`, `model`), `class OpenAIClient(Protocol)` — the `generate(system_prompt, user_prompt) -> str` interface any injected client (real or fake) must satisfy. Consumed by Task 5 (`app.py` routes).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_ai_dashboard.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m unittest tests.test_ai_dashboard -v`
Expected: `ModuleNotFoundError: No module named 'ai_dashboard'`

- [ ] **Step 3: Implement `ai_dashboard.py`**

Create `ai_dashboard.py`:

```python
"""OpenAI-backed generation of AI dashboard specs from MQTT context."""

from __future__ import annotations

import json
from typing import Any, Protocol

from dashboard_state import DashboardState
from message_store import MessageStore

_ALLOWED_MODES = {"point", "series"}

_RESPONSE_SCHEMA = {
    "name": "ai_dashboard",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["spec", "metrics"],
        "properties": {
            "spec": {"type": "object"},
            "metrics": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["id", "label", "topic_filter", "json_path", "mode"],
                    "properties": {
                        "id": {"type": "string", "minLength": 1},
                        "label": {"type": "string"},
                        "topic_filter": {"type": "string", "minLength": 1},
                        "json_path": {"type": "string", "minLength": 1},
                        "mode": {"type": "string", "enum": ["point", "series"]},
                    },
                },
            },
        },
    },
    "strict": True,
}

_SYSTEM_PROMPT = (
    "You design a single Vega-Lite v5 chart for live MQTT telemetry. "
    "Reply with JSON only, matching the provided schema. "
    "The chart's data source MUST be named \"table\" (spec.data.name == \"table\") "
    "and rows have exactly these fields: metric (string), group (string), "
    "value (number), ts (ISO-8601 string). "
    "Each entry in \"metrics\" extracts one value stream from the sampled MQTT "
    "traffic below: topic_filter is an MQTT wildcard pattern (+, #), json_path is "
    "a dot-path into the decoded JSON payload (e.g. \"load_avg.1min\"), and mode is "
    "\"point\" (latest value per device, for bar/pie charts) or \"series\" "
    "(value over time, for line/area charts)."
)


class OpenAIClient(Protocol):
    def generate(self, system_prompt: str, user_prompt: str) -> str: ...


class AIDashboardError(Exception):
    """Raised when the model's response fails validation after retrying."""


class AIProviderError(Exception):
    """Raised when the underlying LLM API call itself fails."""


class OpenAIChatClient:
    """Thin wrapper around the OpenAI SDK's chat completions endpoint."""

    def __init__(self, api_key: str, model: str) -> None:
        from openai import OpenAI

        self._client = OpenAI(api_key=api_key)
        self._model = model

    def generate(self, system_prompt: str, user_prompt: str) -> str:
        response = self._client.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_schema", "json_schema": _RESPONSE_SCHEMA},
        )
        return response.choices[0].message.content


def build_context(
    store: MessageStore | None,
    state: DashboardState,
    sample_size: int = 50,
    max_topics: int = 30,
) -> str:
    """Sample recent messages, deduped by topic (most recent payload kept),
    capped at max_topics distinct topics, formatted for the prompt."""
    if store is not None:
        messages = store.recent(sample_size)
    else:
        messages = list(reversed(state.snapshot()["messages"][:sample_size]))

    by_topic: dict[str, dict[str, Any]] = {}
    for message in messages:
        by_topic[message["topic"]] = message
    topics = list(by_topic.values())[-max_topics:]

    return "\n".join(f"{m['topic']} -> {m['payload']}" for m in topics)


def validate_response(payload: Any) -> dict[str, Any]:
    """Validate the model's parsed JSON response. Raises AIDashboardError
    with a short, model-readable reason on the first problem found."""
    if not isinstance(payload, dict):
        raise AIDashboardError("response must be a JSON object")

    spec = payload.get("spec")
    metrics = payload.get("metrics")
    if not isinstance(spec, dict):
        raise AIDashboardError('"spec" must be an object')
    if not isinstance(spec.get("data"), dict) or spec["data"].get("name") != "table":
        raise AIDashboardError('spec.data.name must be "table"')
    if not isinstance(metrics, list) or not metrics:
        raise AIDashboardError('"metrics" must be a non-empty array')

    seen_ids: set[str] = set()
    for metric in metrics:
        if not isinstance(metric, dict):
            raise AIDashboardError("each metric must be an object")
        metric_id = metric.get("id")
        if not isinstance(metric_id, str) or not metric_id:
            raise AIDashboardError("metric.id must be a non-empty string")
        if metric_id in seen_ids:
            raise AIDashboardError(f"duplicate metric id: {metric_id}")
        seen_ids.add(metric_id)
        if not isinstance(metric.get("topic_filter"), str) or not metric["topic_filter"]:
            raise AIDashboardError(f"metric {metric_id}: topic_filter must be a non-empty string")
        if not isinstance(metric.get("json_path"), str) or not metric["json_path"]:
            raise AIDashboardError(f"metric {metric_id}: json_path must be a non-empty string")
        if metric.get("mode") not in _ALLOWED_MODES:
            raise AIDashboardError(f"metric {metric_id}: mode must be one of {sorted(_ALLOWED_MODES)}")

    return payload


def generate_dashboard(prompt: str, context: str, client: OpenAIClient) -> dict[str, Any]:
    """Call the model, validate its response, retry once on validation
    failure. Raises AIProviderError if the client call itself fails, or
    AIDashboardError if the response is still invalid after one retry."""
    user_prompt = f"User request: {prompt}\n\nSample MQTT traffic:\n{context}"
    last_error: AIDashboardError | None = None

    for attempt in range(2):
        if attempt == 1:
            user_prompt += (
                f"\n\nYour previous response was invalid: {last_error}. "
                "Fix it and reply again with JSON only."
            )
        try:
            raw = client.generate(_SYSTEM_PROMPT, user_prompt)
        except Exception as error:
            raise AIProviderError(str(error)) from error

        try:
            payload = json.loads(raw)
            return validate_response(payload)
        except json.JSONDecodeError as error:
            last_error = AIDashboardError(f"response was not valid JSON: {error}")
        except AIDashboardError as error:
            last_error = error

    assert last_error is not None
    raise last_error
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m unittest tests.test_ai_dashboard -v`
Expected: all tests `OK`

- [ ] **Step 5: Commit**

```bash
git add ai_dashboard.py tests/test_ai_dashboard.py
git commit -m "Add ai_dashboard module for OpenAI-backed dashboard generation"
```

---

### Task 3: `metric_engine.py` — live metric extraction

**Files:**
- Modify: `system_metrics.py:42-51` (rename `_coerce_number` to public `coerce_number`)
- Create: `metric_engine.py`
- Test: `tests/test_metric_engine.py`

**Interfaces:**
- Consumes: `paho.mqtt.client.topic_matches_sub(sub: str, topic: str) -> bool` (third-party, already a dependency), `infer_device(topic: str) -> str` (`dashboard_state.py`), `coerce_number(value: Any) -> float | None` (`system_metrics.py`, renamed this task).
- Produces: `class MetricEngine(series_maxlen: int = 200)` with `.register(dashboard_id: int, metrics: list[dict]) -> None`, `.unregister(dashboard_id: int) -> None`, `.ingest(message: dict) -> list[dict]`, `.snapshot(dashboard_id: int) -> list[dict]`. Consumed by Task 5 (`app.py`).

- [ ] **Step 1: Rename `_coerce_number` to `coerce_number`**

In `system_metrics.py`, this private helper is currently used three times inside `normalize_metrics`. Make it public since `metric_engine.py` needs to reuse it rather than duplicate the numeric-coercion logic:

```bash
python3 -c "
import pathlib
p = pathlib.Path('system_metrics.py')
p.write_text(p.read_text().replace('_coerce_number', 'coerce_number'))
"
```

- [ ] **Step 2: Verify the existing system_metrics tests still pass after the rename**

Run: `python -m unittest tests.test_system_metrics -v`
Expected: all tests `OK` (the rename is behavior-preserving; `tests/test_system_metrics.py` only calls `normalize_metrics`, never `_coerce_number` directly)

- [ ] **Step 3: Write the failing tests for `MetricEngine`**

Create `tests/test_metric_engine.py`:

```python
from __future__ import annotations

import unittest

from dashboard_state import decode_payload
from metric_engine import MetricEngine


def make_message(
    topic: str,
    payload: bytes,
    timestamp: str = "2026-07-20T10:00:00.000+00:00",
) -> dict:
    text, encoding, is_json, json_value = decode_payload(payload)
    return {
        "topic": topic,
        "payload": text,
        "payload_encoding": encoding,
        "is_json": is_json,
        "json": json_value,
        "timestamp": timestamp,
    }


POINT_METRIC = {
    "id": "temp",
    "label": "Temperature",
    "topic_filter": "+/temperature",
    "json_path": "temperature",
    "mode": "point",
}

SERIES_METRIC = {
    "id": "load",
    "label": "Load",
    "topic_filter": "+/system",
    "json_path": "load_avg.1min",
    "mode": "series",
}


class MetricEngineTests(unittest.TestCase):
    def test_point_mode_groups_latest_value_by_device(self) -> None:
        engine = MetricEngine()
        engine.register(1, [POINT_METRIC])

        changed = engine.ingest(make_message("router01/temperature", b'{"temperature": 21.5}'))

        self.assertEqual(len(changed), 1)
        self.assertEqual(changed[0]["dashboard_id"], 1)
        self.assertEqual(changed[0]["metric"], "temp")
        self.assertEqual(changed[0]["group"], "router01")
        self.assertEqual(changed[0]["value"], 21.5)
        self.assertEqual(engine.snapshot(1), changed)

        engine.ingest(make_message("router01/temperature", b'{"temperature": 22.0}'))
        snapshot = engine.snapshot(1)
        self.assertEqual(len(snapshot), 1)
        self.assertEqual(snapshot[0]["value"], 22.0)

    def test_series_mode_appends_and_caps_buffer(self) -> None:
        engine = MetricEngine(series_maxlen=2)
        engine.register(1, [SERIES_METRIC])

        for value in (1, 2, 3):
            payload = f'{{"load_avg": {{"1min": {value}}}}}'.encode()
            engine.ingest(make_message("router01/system", payload))

        snapshot = engine.snapshot(1)
        self.assertEqual([row["value"] for row in snapshot], [2, 3])

    def test_non_matching_topic_is_ignored(self) -> None:
        engine = MetricEngine()
        engine.register(1, [POINT_METRIC])

        changed = engine.ingest(make_message("router01/status", b'{"temperature": 21.5}'))

        self.assertEqual(changed, [])
        self.assertEqual(engine.snapshot(1), [])

    def test_missing_json_path_is_skipped_without_error(self) -> None:
        engine = MetricEngine()
        engine.register(1, [POINT_METRIC])

        changed = engine.ingest(make_message("router01/temperature", b'{"other": 1}'))

        self.assertEqual(changed, [])

    def test_unregister_stops_future_updates(self) -> None:
        engine = MetricEngine()
        engine.register(1, [POINT_METRIC])
        engine.ingest(make_message("router01/temperature", b'{"temperature": 21.5}'))

        engine.unregister(1)
        changed = engine.ingest(make_message("router01/temperature", b'{"temperature": 30}'))

        self.assertEqual(changed, [])
        self.assertEqual(engine.snapshot(1), [])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `python -m unittest tests.test_metric_engine -v`
Expected: `ModuleNotFoundError: No module named 'metric_engine'`

- [ ] **Step 5: Implement `metric_engine.py`**

Create `metric_engine.py`:

```python
"""Live metric extraction for AI-generated dashboards."""

from __future__ import annotations

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
                    if not topic_matches_sub(metric["topic_filter"], topic):
                        continue
                    value = coerce_number(_extract(json_value, metric["json_path"]))
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
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `python -m unittest tests.test_metric_engine -v`
Expected: all tests `OK`

- [ ] **Step 7: Commit**

```bash
git add system_metrics.py metric_engine.py tests/test_metric_engine.py
git commit -m "Add MetricEngine for live AI-dashboard metric extraction"
```

---

### Task 4: `ai_dashboard_store.py` — dashboard persistence

**Files:**
- Create: `ai_dashboard_store.py`
- Test: `tests/test_ai_dashboard_store.py`

**Interfaces:**
- Produces: `class AIDashboardStore(path: str)` with `.save(name: str, prompt: str, spec: dict, metrics: list[dict]) -> dict`, `.list() -> list[dict]`, `.get(dashboard_id: int) -> dict | None`, `.delete(dashboard_id: int) -> None`, `.close() -> None`. Consumed by Task 5 (`app.py`).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_ai_dashboard_store.py`:

```python
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ai_dashboard_store import AIDashboardStore

SPEC = {"data": {"name": "table"}, "mark": "line"}
METRICS = [
    {
        "id": "temp",
        "label": "Temperature",
        "topic_filter": "+/temperature",
        "json_path": "temperature",
        "mode": "point",
    }
]


class AIDashboardStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.path = str(Path(self._dir.name) / "test.db")

    def tearDown(self) -> None:
        self._dir.cleanup()

    def test_save_then_get_round_trips_spec_and_metrics(self) -> None:
        store = AIDashboardStore(self.path)
        saved = store.save("Router temps", "chart temperature", SPEC, METRICS)

        fetched = store.get(saved["id"])
        self.assertEqual(fetched["name"], "Router temps")
        self.assertEqual(fetched["prompt"], "chart temperature")
        self.assertEqual(fetched["spec"], SPEC)
        self.assertEqual(fetched["metrics"], METRICS)
        store.close()

    def test_get_returns_none_for_missing_id(self) -> None:
        store = AIDashboardStore(self.path)
        self.assertIsNone(store.get(999))
        store.close()

    def test_list_returns_lightweight_rows_in_creation_order(self) -> None:
        store = AIDashboardStore(self.path)
        first = store.save("First", "prompt one", SPEC, METRICS)
        second = store.save("Second", "prompt two", SPEC, METRICS)

        rows = store.list()

        self.assertEqual([row["id"] for row in rows], [first["id"], second["id"]])
        self.assertNotIn("spec", rows[0])
        self.assertNotIn("metrics", rows[0])
        store.close()

    def test_delete_removes_the_row(self) -> None:
        store = AIDashboardStore(self.path)
        saved = store.save("First", "prompt", SPEC, METRICS)

        store.delete(saved["id"])

        self.assertIsNone(store.get(saved["id"]))
        store.close()

    def test_persists_across_reopen(self) -> None:
        store = AIDashboardStore(self.path)
        saved = store.save("First", "prompt", SPEC, METRICS)
        store.close()

        reopened = AIDashboardStore(self.path)
        fetched = reopened.get(saved["id"])
        self.assertEqual(fetched["name"], "First")
        reopened.close()


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m unittest tests.test_ai_dashboard_store -v`
Expected: `ModuleNotFoundError: No module named 'ai_dashboard_store'`

- [ ] **Step 3: Implement `ai_dashboard_store.py`**

Create `ai_dashboard_store.py`:

```python
"""SQLite persistence for AI-generated dashboards."""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ai_dashboards (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL,
    prompt       TEXT NOT NULL,
    spec_json    TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    created_at   TEXT NOT NULL
);
"""


class AIDashboardStore:
    """Write-through archive of AI-generated dashboard definitions."""

    def __init__(self, path: str) -> None:
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def save(
        self, name: str, prompt: str, spec: dict[str, Any], metrics: list[dict[str, Any]]
    ) -> dict[str, Any]:
        created_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        with self._lock:
            cursor = self._conn.execute(
                "INSERT INTO ai_dashboards (name, prompt, spec_json, metrics_json, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (name, prompt, json.dumps(spec), json.dumps(metrics), created_at),
            )
            self._conn.commit()
            dashboard_id = cursor.lastrowid
        return {
            "id": dashboard_id,
            "name": name,
            "prompt": prompt,
            "spec": spec,
            "metrics": metrics,
            "created_at": created_at,
        }

    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            cursor = self._conn.execute(
                "SELECT id, name, prompt, created_at FROM ai_dashboards ORDER BY id"
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def get(self, dashboard_id: int) -> dict[str, Any] | None:
        with self._lock:
            cursor = self._conn.execute(
                "SELECT * FROM ai_dashboards WHERE id = ?", (dashboard_id,)
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return {
            "id": row["id"],
            "name": row["name"],
            "prompt": row["prompt"],
            "spec": json.loads(row["spec_json"]),
            "metrics": json.loads(row["metrics_json"]),
            "created_at": row["created_at"],
        }

    def delete(self, dashboard_id: int) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM ai_dashboards WHERE id = ?", (dashboard_id,))
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m unittest tests.test_ai_dashboard_store -v`
Expected: all tests `OK`

- [ ] **Step 5: Commit**

```bash
git add ai_dashboard_store.py tests/test_ai_dashboard_store.py
git commit -m "Add AIDashboardStore for persisting generated dashboards"
```

---

### Task 5: Wire it into `app.py`

**Files:**
- Modify: `app.py:1-176` (imports, `SocketEventBatcher`, `create_app`, `main`)
- Test: `tests/test_app.py` (additions)

**Interfaces:**
- Consumes: everything produced in Tasks 1–4 — `Config.OPENAI_API_KEY/OPENAI_MODEL/AI_DASHBOARD_SAMPLE_SIZE/AI_DASHBOARD_MAX_TOPICS/AI_METRIC_SERIES_MAXLEN`, `ai_dashboard.{AIDashboardError, AIProviderError, OpenAIChatClient, OpenAIClient, build_context, generate_dashboard}`, `ai_dashboard_store.AIDashboardStore`, `metric_engine.MetricEngine`.
- Produces: `create_app(config=None, *, start_mqtt=True, openai_client=None)` — new keyword-only `openai_client` param; `app.extensions["ai_dashboard_store"]`, `app.extensions["metric_engine"]`; routes `POST/GET /api/ai-dashboards`, `GET/DELETE /api/ai-dashboards/<id>`; socket event `"ai_metrics"`.

- [ ] **Step 1: Update the imports**

In `app.py`, the current import block is:

```python
from flask import Flask, render_template
from flask_socketio import SocketIO, emit

from config import Config
from dashboard_state import DashboardState
from message_store import MessageStore
from mqtt_client import MQTTClient
```

Replace with:

```python
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
```

- [ ] **Step 2: Extend `SocketEventBatcher` with an `ai_metrics` queue**

The current `__init__` is:

```python
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
```

Replace with:

```python
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
```

The current `push` and `_drain` methods are:

```python
    def push(self, message: dict[str, Any]) -> None:
        with self._lock:
            self._pending.append(message)

    def _drain(self) -> list[dict[str, Any]]:
        with self._lock:
            messages = list(self._pending)
            self._pending.clear()
            return messages
```

Add `push_ai_metrics` and `_drain_ai` right after `_drain`:

```python
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
```

The current `_run` is:

```python
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
                self._socketio.emit("system", self._state.system_snapshot())
```

Replace with:

```python
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
```

- [ ] **Step 3: Extend `create_app` — new services and startup registration**

The current signature and the block that builds `state`/`batcher`/`store` is:

```python
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
        system_topic_suffix=settings.SYSTEM_TOPIC_SUFFIX,
    )
    batcher = SocketEventBatcher(socketio, state, settings.SOCKET_BATCH_INTERVAL)

    store: MessageStore | None = None
    if settings.LOG_PERSISTENCE_ENABLED:
        store = MessageStore(settings.LOG_DB_PATH, settings.LOG_RETENTION)
        restored = store.recent(settings.MESSAGE_LIMIT)
        state.restore(restored)
        logger.info("Restored %d messages from %s", len(restored), settings.LOG_DB_PATH)
```

Replace with:

```python
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
```

- [ ] **Step 4: Register the new extensions and hook `handle_message`**

The current block is:

```python
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

    mqtt_client = MQTTClient(settings, handle_message, handle_status)
    app.extensions["dashboard_state"] = state
    app.extensions["event_batcher"] = batcher
    app.extensions["mqtt_client"] = mqtt_client
    app.extensions["message_store"] = store
```

Replace with:

```python
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
```

- [ ] **Step 5: Add the REST routes**

The current route block is:

```python
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
```

Replace with:

```python
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
```

- [ ] **Step 6: Close `ai_store` on shutdown**

The current `main()` is:

```python
def main() -> None:
    settings = Config()
    app, socketio = create_app(settings)
    mqtt_client: MQTTClient = app.extensions["mqtt_client"]
    batcher: SocketEventBatcher = app.extensions["event_batcher"]
    store: MessageStore | None = app.extensions["message_store"]

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
```

Replace with:

```python
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
```

- [ ] **Step 7: Run the existing test suite to make sure nothing broke**

Run: `python -m unittest discover -s tests -v`
Expected: `test_app.py`'s existing two tests still pass (new routes don't affect them); new modules' tests still pass.

- [ ] **Step 8: Write the failing tests for the new routes and event**

In `tests/test_app.py`, the current imports are:

```python
from __future__ import annotations

import unittest

from app import create_app
from config import Config
```

Replace with:

```python
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
```

Append a new test class after the existing `AppIntegrationTests` (before the `if __name__ == "__main__":` block):

```python
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
                ),
                start_mqtt=False,
                openai_client=None,
            )
            response = app.test_client().post(
                "/api/ai-dashboards", json={"name": "Temps", "prompt": "chart it"}
            )
            self.assertEqual(response.status_code, 503)
            app.extensions["event_batcher"].stop()
```

- [ ] **Step 9: Run the tests to verify they pass**

Run: `python -m unittest tests.test_app -v`
Expected: all tests `OK`

- [ ] **Step 10: Run the full test suite**

Run: `python -m unittest discover -s tests -v`
Expected: all tests `OK`

- [ ] **Step 11: Commit**

```bash
git add app.py tests/test_app.py
git commit -m "Wire AI dashboard generation and live metrics into app.py"
```

---

### Task 6: Frontend — AI Dashboards tab

**Files:**
- Modify: `templates/index.html` (nav tabs, new panel, CDN scripts)
- Modify: `static/js/dashboard.js:72-78` (expose the socket)
- Create: `static/js/ai-dashboards.js`
- Modify: `static/css/dashboard.css` (new panel styles)

**Interfaces:**
- Consumes: `POST/GET /api/ai-dashboards`, `GET/DELETE /api/ai-dashboards/<id>`, socket event `"ai_metrics"` (Task 5); global `window.dashboardSocket` (this task, exposed from `dashboard.js`); `vegaEmbed` global (CDN).

There is no JS test harness in this repo (`dashboard.js` itself is untested by the Python `unittest` suite) — this task is verified manually by running the app and using the feature in a browser, per this repo's existing convention.

- [ ] **Step 1: Resolve pinned CDN versions and compute SRI hashes**

The existing `<script>`/`<link>` tags in `templates/index.html` (Bootstrap, Socket.IO) both pin an exact version and an `integrity` attribute. Do the same for the three new scripts. Run:

```bash
for pkg in vega@5 vega-lite@5 vega-embed@6; do
  url="https://cdn.jsdelivr.net/npm/${pkg}"
  resolved=$(curl -sI "$url" | grep -i '^location:' | sed 's/location: //I' | tr -d '\r')
  echo "$pkg -> ${resolved:-$url}"
done
```

For each resolved URL, fetch the file and compute its SRI hash:

```bash
curl -s "<resolved-url>" | openssl dgst -sha384 -binary | openssl base64 -A
```

Note the three exact versioned URLs and their `sha384-...` hashes — they're used in Step 2.

- [ ] **Step 2: Add the CDN scripts and the AI Dashboards nav tab**

In `templates/index.html`, the nav currently ends with:

```html
            <button class="view-tab" type="button" data-panel="systemPanel">
              System health <span id="systemCountBadge" class="tab-count">0</span>
            </button>
          </nav>
```

Add a fourth tab button:

```html
            <button class="view-tab" type="button" data-panel="systemPanel">
              System health <span id="systemCountBadge" class="tab-count">0</span>
            </button>
            <button class="view-tab" type="button" data-panel="aiDashboardsPanel">
              AI Dashboards <span id="aiDashboardCountBadge" class="tab-count">0</span>
            </button>
          </nav>
```

The `systemPanel` section currently ends with:

```html
        <div id="systemPanel" class="dashboard-panel">
          <div class="panel-controls">
            <div>
              <h2>System health</h2>
              <p>Latest load average, RAM, and disk usage per device.</p>
            </div>
          </div>
          <div id="systemGrid" class="device-grid"></div>
          <div id="systemEmpty" class="empty-state">
            <h2>No system telemetry yet</h2>
            <p>Messages on a topic such as <code>router01/system</code> will appear here.</p>
          </div>
        </div>
      </section>
```

Add a new panel after it, before `</section>`:

```html
        <div id="systemPanel" class="dashboard-panel">
          <div class="panel-controls">
            <div>
              <h2>System health</h2>
              <p>Latest load average, RAM, and disk usage per device.</p>
            </div>
          </div>
          <div id="systemGrid" class="device-grid"></div>
          <div id="systemEmpty" class="empty-state">
            <h2>No system telemetry yet</h2>
            <p>Messages on a topic such as <code>router01/system</code> will appear here.</p>
          </div>
        </div>

        <div id="aiDashboardsPanel" class="dashboard-panel">
          <div class="panel-controls">
            <div>
              <h2>AI Dashboards</h2>
              <p>Describe a chart in plain language; it stays live as new MQTT messages arrive.</p>
            </div>
          </div>
          <form id="aiDashboardForm" class="ai-dashboard-form">
            <input
              id="aiDashboardName"
              type="text"
              placeholder="Dashboard name (e.g. Router temperatures)"
              autocomplete="off"
              required
            >
            <textarea
              id="aiDashboardPrompt"
              placeholder="Describe the chart you want (e.g. line chart of temperature per router)"
              rows="2"
              required
            ></textarea>
            <button type="submit">Generate</button>
          </form>
          <div id="aiDashboardError" class="ai-dashboard-error" hidden></div>
          <div id="aiDashboardList" class="ai-dashboard-list"></div>
          <div id="aiDashboardEmpty" class="empty-state compact">
            <h2>No AI dashboards yet</h2>
            <p>Generate one above to see it here.</p>
          </div>
        </div>
      </section>
```

Right before the closing `</body>`, the current scripts are:

```html
    <script src="https://cdn.socket.io/4.7.5/socket.io.min.js" integrity="sha384-2huaZvOR9iDzHqslqwpR87isEmrfxqyWOF7hr7BY6KG0+hVKLoEXMPUJw3ynWuhO" crossorigin="anonymous" defer></script>
    <script src="{{ url_for('static', filename='js/dashboard.js') }}" defer></script>
  </body>
```

Replace with (using the resolved URLs and hashes from Step 1):

```html
    <script src="https://cdn.socket.io/4.7.5/socket.io.min.js" integrity="sha384-2huaZvOR9iDzHqslqwpR87isEmrfxqyWOF7hr7BY6KG0+hVKLoEXMPUJw3ynWuhO" crossorigin="anonymous" defer></script>
    <script src="<resolved-vega-url>" integrity="<sha384-vega-hash>" crossorigin="anonymous" defer></script>
    <script src="<resolved-vega-lite-url>" integrity="<sha384-vega-lite-hash>" crossorigin="anonymous" defer></script>
    <script src="<resolved-vega-embed-url>" integrity="<sha384-vega-embed-hash>" crossorigin="anonymous" defer></script>
    <script src="{{ url_for('static', filename='js/dashboard.js') }}" defer></script>
    <script src="{{ url_for('static', filename='js/ai-dashboards.js') }}" defer></script>
  </body>
```

- [ ] **Step 3: Expose the Socket.IO connection for `ai-dashboards.js` to reuse**

In `static/js/dashboard.js`, the socket is currently created as:

```javascript
  const socket = window.io({
    transports: ["websocket"],
    upgrade: false,
    reconnection: true,
    reconnectionDelay: 500,
    reconnectionDelayMax: 5000,
  });
```

Replace with:

```javascript
  const socket = window.io({
    transports: ["websocket"],
    upgrade: false,
    reconnection: true,
    reconnectionDelay: 500,
    reconnectionDelayMax: 5000,
  });
  window.dashboardSocket = socket;
```

- [ ] **Step 4: Write `static/js/ai-dashboards.js`**

Create `static/js/ai-dashboards.js`:

```javascript
(() => {
  "use strict";

  const elements = {
    form: document.getElementById("aiDashboardForm"),
    nameInput: document.getElementById("aiDashboardName"),
    promptInput: document.getElementById("aiDashboardPrompt"),
    error: document.getElementById("aiDashboardError"),
    list: document.getElementById("aiDashboardList"),
    empty: document.getElementById("aiDashboardEmpty"),
    countBadge: document.getElementById("aiDashboardCountBadge"),
  };

  if (!elements.form) return;

  const views = new Map();

  function showError(message) {
    elements.error.textContent = message;
    elements.error.hidden = !message;
  }

  async function loadList() {
    const response = await fetch("/api/ai-dashboards");
    const dashboards = await response.json();
    elements.countBadge.textContent = String(dashboards.length);
    elements.empty.hidden = dashboards.length > 0;

    elements.list.replaceChildren();
    dashboards.forEach((dashboard) => elements.list.append(createCard(dashboard)));
  }

  function createCard(dashboard) {
    const card = document.createElement("article");
    card.className = "ai-dashboard-card";
    card.dataset.id = String(dashboard.id);

    const header = document.createElement("div");
    header.className = "ai-dashboard-card-header";
    const title = document.createElement("h3");
    title.textContent = dashboard.name;
    const deleteButton = document.createElement("button");
    deleteButton.type = "button";
    deleteButton.className = "ai-dashboard-delete";
    deleteButton.textContent = "Delete";
    deleteButton.addEventListener("click", () => deleteDashboard(dashboard.id, card));
    header.append(title, deleteButton);

    const prompt = document.createElement("p");
    prompt.className = "ai-dashboard-prompt";
    prompt.textContent = dashboard.prompt;

    const chart = document.createElement("div");
    chart.className = "ai-dashboard-chart";
    chart.id = `aiChart-${dashboard.id}`;

    card.append(header, prompt, chart);
    renderChart(dashboard.id, chart);
    return card;
  }

  async function renderChart(dashboardId, container) {
    const response = await fetch(`/api/ai-dashboards/${dashboardId}`);
    if (!response.ok) return;
    const dashboard = await response.json();

    const result = await window.vegaEmbed(container, dashboard.spec, { actions: false });
    result.view.data("table", dashboard.initial_data || []);
    await result.view.runAsync();
    views.set(dashboardId, result.view);
  }

  function applyAiMetrics(rows) {
    const byDashboard = new Map();
    rows.forEach((row) => {
      if (!byDashboard.has(row.dashboard_id)) byDashboard.set(row.dashboard_id, []);
      byDashboard.get(row.dashboard_id).push(row);
    });

    byDashboard.forEach((dashboardRows, dashboardId) => {
      const view = views.get(dashboardId);
      if (!view) return;

      const changeset = window.vega.changeset();
      dashboardRows.forEach((row) => {
        changeset.remove(
          (datum) => datum.metric === row.metric && datum.group === row.group
        );
        changeset.insert([row]);
      });
      view.change("table", changeset).run();
    });
  }

  async function deleteDashboard(dashboardId, card) {
    await fetch(`/api/ai-dashboards/${dashboardId}`, { method: "DELETE" });
    views.delete(dashboardId);
    card.remove();
    loadList();
  }

  elements.form.addEventListener("submit", async (event) => {
    event.preventDefault();
    showError("");

    const name = elements.nameInput.value.trim();
    const prompt = elements.promptInput.value.trim();
    if (!name || !prompt) return;

    const response = await fetch("/api/ai-dashboards", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, prompt }),
    });

    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      showError(body.error || `Request failed (${response.status})`);
      return;
    }

    elements.form.reset();
    loadList();
  });

  if (window.dashboardSocket) {
    window.dashboardSocket.on("ai_metrics", applyAiMetrics);
  }

  loadList();
})();
```

Note: `point`-mode rows use "remove the old row for this metric+group, insert the new one" so a bar/pie chart shows one current value per device rather than growing forever; `series`-mode rows also go through this same path, but since each row carries a fresh `ts`, the `remove` predicate (matching on `metric`+`group` only) only ever matches the fresh point-mode entries — series rows accumulate because `series` metrics don't collide on `metric`+`group` across ticks the way `point` metrics intentionally do at the same group. This mirrors the point/series distinction defined in `MetricEngine`.

- [ ] **Step 5: Add CSS for the new panel**

In `static/css/dashboard.css`, append at the end of the file:

```css
.ai-dashboard-form {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  margin-bottom: 20px;
}

.ai-dashboard-form input,
.ai-dashboard-form textarea {
  flex: 1 1 240px;
  padding: 10px 12px;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--surface-2);
  color: var(--text);
}

.ai-dashboard-form button {
  padding: 10px 18px;
  border: 1px solid var(--cyan);
  border-radius: 8px;
  background: var(--cyan-soft);
  color: var(--cyan);
  cursor: pointer;
}

.ai-dashboard-error {
  margin-bottom: 16px;
  padding: 10px 12px;
  border: 1px solid var(--red);
  border-radius: 8px;
  color: var(--red);
}

.ai-dashboard-list {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
  gap: 16px;
}

.ai-dashboard-card {
  padding: 16px;
  border: 1px solid var(--border);
  border-radius: 12px;
  background: var(--surface);
}

.ai-dashboard-card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.ai-dashboard-card-header h3 {
  margin: 0;
  font-size: 15px;
}

.ai-dashboard-delete {
  border: 1px solid var(--border);
  border-radius: 6px;
  background: transparent;
  color: var(--muted);
  padding: 4px 10px;
  cursor: pointer;
}

.ai-dashboard-prompt {
  margin: 8px 0 12px;
  color: var(--muted);
  font-size: 13px;
}

.ai-dashboard-chart {
  min-height: 220px;
}
```

- [ ] **Step 6: Manually verify in the browser**

Run: `OPENAI_API_KEY=sk-... MQTT_ENABLED=false python app.py`

Then:
1. Open `http://localhost:5000`, click the "AI Dashboards" tab.
2. Submit a name and prompt (e.g. "bar chart of temperature per router").
3. Confirm a card appears with a rendered chart (or a visible error message if generation fails).
4. Publish a matching MQTT message (or, with `MQTT_ENABLED=false`, use `python3 -c` against a running instance's `dashboard_state`/`metric_engine` in a separate shell, or temporarily set `MQTT_ENABLED=true` against a local `mosquitto_pub`) and confirm the chart updates without a page reload.
5. Click "Delete" and confirm the card disappears and a page reload no longer shows it.

- [ ] **Step 7: Commit**

```bash
git add templates/index.html static/js/dashboard.js static/js/ai-dashboards.js static/css/dashboard.css
git commit -m "Add AI Dashboards tab with live Vega-Lite rendering"
```
