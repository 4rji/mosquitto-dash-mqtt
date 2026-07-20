# AI-Generated Dashboards — Design

**Date:** 2026-07-20
**Status:** Approved (pending spec review)

## Goal

Add a fourth capability to the dashboard: a user describes, in a text prompt,
a chart they want (e.g. "gráfico de temperatura por router"), and an LLM
(OpenAI) turns that prompt plus a sample of recent MQTT traffic into a chart
definition. The resulting chart is **live** — it keeps updating as new MQTT
messages arrive, the same way the rest of the dashboard does, without calling
the LLM again. Generated dashboards are named and persisted so the user can
come back to them later.

## Non-goals (YAGNI)

- No arbitrary code generation/execution (client- or server-side). The LLM
  only ever produces declarative JSON (a Vega-Lite spec + metric
  definitions) — nothing it returns is `eval`'d or run as a function.
- No access to the fixed aggregates the app already exposes (`devices`,
  `topics`, `stats`, `system`) — those already have dedicated tabs. AI
  dashboards work exclusively off custom metrics extracted from message
  payloads (see Approach B below). Keeps a single, consistent data-reference
  mechanism instead of two.
- No payload redaction/anonymization before sending samples to OpenAI — the
  user explicitly opted to send sample topics/payloads as-is.
- No JSON array indexing in metric paths (`items[0].value`) — dot-path only.
  Can be added later if a real payload needs it.
- No editing of a saved dashboard's prompt/spec — regenerate (new prompt, new
  saved entry) instead of in-place edit.

## Approach considered

Three ways to let the LLM reference live payload data were considered:

- **A — Fixed catalog:** LLM only picks from existing aggregates
  (`devices`/`topics`/`stats`/`system`). Simplest and safest, but can't chart
  a field that isn't already aggregated (e.g. an arbitrary payload key like
  `temperature`).
- **B — Custom metric extractors (chosen):** LLM defines named metrics as
  `{topic_filter, json_path, mode}`. The backend evaluates these against the
  same decoded-message pipeline `DashboardState` already uses, with no code
  execution — just MQTT topic-wildcard matching and JSON dot-path lookup.
  Flexible enough to satisfy "point at the log, chart this field," while
  staying declarative and safe.
- **C — LLM-generated JS executed client-side:** maximum flexibility, but
  executing LLM-generated code (even sandboxed, even client-side) is a
  security risk not justified by this feature. Rejected.

Approach B is the design below.

## Architecture & data flow

**Generation (on demand, user-triggered):**

```
User submits {name, prompt} in the "AI Dashboards" tab
   -> POST /api/ai-dashboards
   -> ai_dashboard.build_context(store, state)
        samples recent messages, deduped by topic, capped topic count
   -> ai_dashboard.generate_dashboard(prompt, context, client)
        calls OpenAI (structured JSON output), validates the response,
        retries once on validation failure, else raises AIDashboardError
   -> AIDashboardStore.save(name, prompt, spec, metrics)
   -> MetricEngine.register(dashboard_id, metrics)
   -> 201 {id, name, prompt, spec, metrics, created_at}
```

**Live updates (existing message pipeline, extended):**

```
Mosquitto -> MQTTClient -> app.handle_message
   -> DashboardState.record_message(...)      # unchanged
   -> MetricEngine.ingest(message)             # new
        for each registered metric:
          if topic_matches_sub(metric.topic_filter, message["topic"]):
            value = dot-path lookup of metric.json_path in message["json"]
            if value is not None: update point/series buffer, return changed row
   -> SocketEventBatcher
        existing "mqtt_messages"/"stats"/"system" emits, unchanged
        + new "ai_metrics" emit (same batch tick) with any changed rows
   -> ai-dashboards.js: for the currently open dashboard(s), patch the
      Vega view via a changeset — no re-render, no re-fetch
```

`topic_matches_sub` is `paho.mqtt.client.topic_matches_sub` — already a
dependency, no new wildcard-matching code needed. `infer_device` (grouping,
see below) reuses the existing function in `dashboard_state.py`.

## Data contract

Every metric value, regardless of source, is shaped as one row:

```json
{"metric": "temp_router01", "group": "router01", "value": 23.5, "ts": "2026-07-20T10:15:00.000Z"}
```

`group` is `infer_device(topic)` of the message that produced the value —
lets a single chart compare multiple devices without the LLM inventing its
own grouping logic. `ts` is the message's existing UTC timestamp.

OpenAI must return JSON shaped as:

```json
{
  "spec": { "...": "Vega-Lite v5 spec; data.name == \"table\"; encoding uses metric/group/value/ts" },
  "metrics": [
    {
      "id": "temp_router01",
      "label": "Temperatura",
      "topic_filter": "+/temperature",
      "json_path": "temperature",
      "mode": "point"
    }
  ]
}
```

- `topic_filter`: MQTT wildcard pattern (`+`, `#`).
- `json_path`: dot-path into the decoded JSON payload (e.g. `load_avg.1min`).
- `mode`: `"point"` (latest value per `group` — bar/pie snapshots) or
  `"series"` (append to a per-metric rolling buffer, capped at
  `AI_METRIC_SERIES_MAXLEN` — line/area over time).
- `metrics[].id` must be unique within the response (validated).
- Dot-path lookup: if any segment is missing or the value at that point isn't
  a dict, the lookup yields "no value" — treated the same as the sparse-payload
  case in Error handling (that message is skipped for that metric, not an error).

Requested via OpenAI's structured JSON output (JSON Schema response format)
to minimize malformed responses; validated again server-side regardless
(schema/type checks, non-empty `metrics`, unique ids, `mode` in the allowed
set) as defense in depth — the LLM is an untrusted input source.

## Components

### 1. `ai_dashboard.py` (new module)

```python
def build_context(store: MessageStore | None, state: DashboardState,
                   sample_size: int = 50, max_topics: int = 30) -> str:
    """Sample recent messages, deduped by topic (most recent payload per
    topic), capped at max_topics, formatted for the prompt."""

def generate_dashboard(prompt: str, context: str, client: OpenAIClient) -> dict:
    """Call OpenAI, validate the structured response, retry once on
    validation failure. Raises AIDashboardError on failure or when the
    client is unconfigured."""

class AIDashboardError(Exception): ...
```

`client` is always injected (constructor param on the caller side, never a
module-level global) — mirrors how `Config(MQTT_ENABLED=False)` keeps tests
off the live broker; here it keeps tests off the live OpenAI API.

### 2. `metric_engine.py` (new module)

```python
class MetricEngine:
    """Thread-safe registry + rolling buffers for AI-dashboard metrics.
    Same lock-and-`_unlocked`-helpers pattern as DashboardState."""

    def register(self, dashboard_id: int, metrics: list[dict]) -> None: ...
    def unregister(self, dashboard_id: int) -> None: ...
    def ingest(self, message: dict) -> list[dict]:
        """Match message against all registered metrics; update point/series
        buffers; return the changed rows (dashboard_id included) for the
        batcher to emit."""
    def snapshot(self, dashboard_id: int) -> list[dict]:
        """Current buffered rows for one dashboard — used to seed
        GET /api/ai-dashboards/<id>."""
```

### 3. `ai_dashboard_store.py` (new module)

```python
class AIDashboardStore:
    """SQLite persistence for generated dashboards. Same shape as
    MessageStore: own connection, WAL mode, simple schema."""

    def __init__(self, path: str) -> None: ...
    def save(self, name: str, prompt: str, spec: dict, metrics: list[dict]) -> dict: ...
    def list(self) -> list[dict]: ...
    def get(self, dashboard_id: int) -> dict | None: ...
    def delete(self, dashboard_id: int) -> None: ...
```

Schema: `ai_dashboards(id INTEGER PRIMARY KEY, name TEXT, prompt TEXT,
spec_json TEXT, metrics_json TEXT, created_at TEXT)`. Same DB file as
`MessageStore` (`LOG_DB_PATH`) by default — a new table, `messages` untouched.

### 4. `app.py` changes

- `create_app()` gains an optional `openai_client` parameter (same pattern as
  the existing `start_mqtt: bool`), so tests and `main()` can supply
  different clients without touching global state. Instantiates
  `AIDashboardStore` and `MetricEngine`, stored on `app.extensions` like the
  existing services.
- `handle_message()`: after `state.record_message(...)`, also calls
  `ai_engine.ingest(message)`; any returned rows get pushed into the batcher
  for the `"ai_metrics"` event.
- `SocketEventBatcher`: extended to also drain and emit pending `ai_metrics`
  rows on the same tick as `mqtt_messages` — no new timer.
- New routes:
  - `POST /api/ai-dashboards` — `{name, prompt}` → 201 with the full record,
    502 on OpenAI failure, 422 on repeated validation failure, 503 if
    `OPENAI_API_KEY` is unset.
  - `GET /api/ai-dashboards` — lightweight list (no `spec`/`metrics`).
  - `GET /api/ai-dashboards/<id>` — full record + `initial_data` from
    `MetricEngine.snapshot(id)`.
  - `DELETE /api/ai-dashboards/<id>` — 204; also `MetricEngine.unregister(id)`.

### 5. `config.py` changes

```python
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
AI_DASHBOARD_SAMPLE_SIZE: int = int(os.getenv("AI_DASHBOARD_SAMPLE_SIZE", "50"))
AI_DASHBOARD_MAX_TOPICS: int = int(os.getenv("AI_DASHBOARD_MAX_TOPICS", "30"))
AI_METRIC_SERIES_MAXLEN: int = int(os.getenv("AI_METRIC_SERIES_MAXLEN", "200"))
```

### 6. Frontend

`templates/index.html`: fourth `.view-tab` ("AI Dashboards") next to the
existing three/four tabs; panel with a name+prompt form, a list of saved
dashboards, and a render area. Adds a `vega-embed` `<script>` tag (CDN,
pinned version) — the only external frontend dependency this feature
introduces.

`static/js/ai-dashboards.js` (new file, kept separate from `dashboard.js`):

- Submits the generation form to `POST /api/ai-dashboards`, shows the result.
- Lists saved dashboards (`GET /api/ai-dashboards`); clicking one fetches the
  full record and renders it with `vegaEmbed(el, spec, {...})`, seeding the
  `"table"` dataset from `initial_data`.
- `socket.on("ai_metrics", ...)`: for each row belonging to a currently
  rendered dashboard, applies a `vega.changeset()` — `insert` for `series`
  rows, `remove` (matching `metric`+`group`) followed by `insert` for `point`
  rows — to the view's `"table"` dataset. No re-fetch, no re-render.
- Delete button calls `DELETE /api/ai-dashboards/<id>` and removes the card.

## Error handling

- `OPENAI_API_KEY` unset: `POST` returns 503 immediately; app still starts
  and runs normally otherwise (same graceful-degradation style as
  `MQTT_ENABLED=False`).
- OpenAI call fails (timeout/rate-limit/auth/network): caught in
  `generate_dashboard`, wrapped as `AIDashboardError`, route returns 502 with
  a short message; full detail goes to `logger`, never to the client.
  Nothing is saved.
- Malformed/invalid structured response: one automatic retry (validation
  error fed back to the model), then 422 asking the user to rephrase.
  Nothing is saved on failure.
- Spec renders oddly in the browser: not pre-validated server-side (would
  mean re-implementing Vega-Lite semantics in Python); `vega-embed`'s own
  render error is surfaced in that dashboard's card, rest of the app
  unaffected.
- Broad `topic_filter` (e.g. `#`): accepted as-is; cost is O(active metrics)
  per message, acceptable at this app's expected traffic. No extra guard.
- `json_path` absent on a given matching message: `ingest()` skips that
  message for that metric silently — normal for sparse payloads, not an
  error.
- SQLite write failure in `AIDashboardStore`: propagates like
  `MessageStore` already does today (no bespoke handling) — Flask returns
  500, consistent with existing behavior.
- Deleting a dashboard that's open in a browser tab: `MetricEngine` stops
  producing rows for it; the open chart just stops moving, no explicit
  signal needed.

## Testing

Same conventions as the rest of the repo: `unittest`, `test_<module>.py`,
`*Tests` classes, `test_<behavior>` methods, nothing hits a live external
service.

`tests/test_metric_engine.py` (new):
- `register` + `ingest`: matching topic (including wildcards) extracts and
  groups by device in `point` mode.
- `series` mode respects the configured buffer cap.
- Non-matching topic is ignored; missing `json_path` on a matching message is
  skipped without raising.
- `unregister` stops further updates for that dashboard's metrics.

`tests/test_ai_dashboard_store.py` (new): save/list/get/delete — same
skeleton as `tests/test_message_store.py`.

`tests/test_ai_dashboard.py` (new):
- `build_context` dedupes by topic and respects `max_topics`.
- `generate_dashboard` with an injected fake OpenAI client returning canned
  valid JSON parses correctly.
- Invalid JSON triggers exactly one retry, then `AIDashboardError`.
- Schema-rejection cases: duplicate metric ids, invalid `mode`.

`tests/test_app.py` (additions): integration tests for
`POST`/`GET`/`DELETE /api/ai-dashboards` using `create_app(..., openai_client=FakeClient())`
— never touches the real OpenAI API, same spirit as `Config(MQTT_ENABLED=False)`.

## Configuration summary

| Variable | Default | Purpose |
| --- | --- | --- |
| `OPENAI_API_KEY` | empty | OpenAI API key; feature disabled (503 on generate) when unset |
| `OPENAI_MODEL` | `gpt-4o-mini` | Model used for dashboard generation |
| `AI_DASHBOARD_SAMPLE_SIZE` | `50` | Recent messages sampled for prompt context |
| `AI_DASHBOARD_MAX_TOPICS` | `30` | Max distinct topics included in prompt context |
| `AI_METRIC_SERIES_MAXLEN` | `200` | Max points kept per `series`-mode metric buffer |
