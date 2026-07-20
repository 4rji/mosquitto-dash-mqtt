# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Structure & Module Organization

Flask/Socket.IO service with Python modules at the repository root — no `src/` layout. `app.py` creates the web app and the WebSocket event batcher; `mqtt_client.py` handles broker connectivity; `dashboard_state.py`, `system_metrics.py`, and `message_store.py` own aggregation, telemetry normalization, and SQLite persistence respectively. Runtime settings live in `config.py`.

Browser code is `templates/index.html`, `static/js/dashboard.js`, `static/css/dashboard.css` — no build step, no frontend framework. Tests in `tests/` mirror module names (`tests/test_message_store.py`, etc.).

`mqtt_tx64.py` is a companion script that runs *on* a Digi router device, not in this dashboard's process. It imports `acl` and `digidevice`, which only exist on-device — it will not import in a normal dev environment and is not covered by the test suite.

## Commands

```bash
# setup
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# run (reads config from env vars / .env; copy .env.example as a starting point)
python app.py

# run all tests
python -m unittest discover -s tests

# run a single test file / case
python -m unittest tests.test_dashboard_state
python -m unittest tests.test_dashboard_state.DashboardStateTests.test_infers_first_segment_only_for_nested_topics

# docker
docker build -t digi-mqtt-dashboard .
docker run --rm -p 5000:5000 -e MQTT_HOST=10.10.65.42 digi-mqtt-dashboard
```

No formatter or linter is configured — match surrounding style.

## Architecture

```
Mosquitto broker
    │
    ▼
MQTTClient (paho-mqtt, mqtt_client.py)
    │  on_message(topic, payload, qos, retain) callback
    ▼
DashboardState.record_message()  (dashboard_state.py)
    │  decodes payload, infers device, updates bounded in-memory aggregates
    ├──▶ MessageStore.append()   (message_store.py, SQLite write-through)
    ▼
SocketEventBatcher.push()  (app.py)
    │  coalesces messages, flushes on a timer (SOCKET_BATCH_INTERVAL)
    ▼
Flask-SocketIO → browser (static/js/dashboard.js, WebSocket-only, no polling)
```

- `MQTTClient`, `DashboardState`, `MessageStore`, and `SocketEventBatcher` are constructed and wired together in `create_app()` (`app.py`) and stored on `app.extensions` rather than as globals. This is also the place new services get added.
- `DashboardState` is the single source of truth for the live feed, per-topic aggregates, per-device aggregates, and per-device system telemetry. It is accessed from both the MQTT callback thread and the Socket.IO background task, so all mutation goes through `self._lock` (an `RLock`) — see the `*_unlocked` helper convention: public methods acquire the lock, `_unlocked` methods assume it's already held and are safe to call from within another locked method.
- On startup, if `LOG_PERSISTENCE_ENABLED`, `MessageStore.recent()` reloads the last `MESSAGE_LIMIT` messages from SQLite into `DashboardState` via `restore()`, which preserves original ids/timestamps and resumes id generation after the max restored id.
- Payload decoding (`decode_payload` in `dashboard_state.py`) is lossless: valid UTF-8 is shown as text, valid JSON additionally gets a parsed tree, and non-UTF-8 bytes become a `0x`-prefixed hex string. No payload schema is assumed anywhere in the ingestion path.
- Device inference (`infer_device`) uses the first segment of a multi-level topic (`router01/status` → `router01`); single-level topics map to `Unknown`.
- System/telemetry topics are detected by `is_system_topic` (last topic segment matches `SYSTEM_TOPIC_SUFFIX`, default `system`) and normalized by `normalize_metrics` in `system_metrics.py`, which tolerates double-JSON-encoded payloads (`_maybe_unwrap_json`) and coerces numeric strings.
- `Config` (`config.py`) is a frozen dataclass read once from environment variables via `python-dotenv`; tests construct `Config(MQTT_ENABLED=False, ...)` directly rather than mutating env vars, so the app never touches a live broker under test.

## Coding Style & Naming Conventions

Python: four-space indentation, type hints, module docstrings, `snake_case` for functions/variables, `PascalCase` for classes. Keep the ingestion/state/persistence/presentation separation intact — don't collapse these modules together. JavaScript: two-space indentation, `camelCase`, `const`/`let`, semicolons. CSS: lowercase kebab-case classes.

## Testing Guidelines

`unittest`-based; name files `test_<module>.py`, classes `*Tests`, methods `test_<behavior>`. Tests must not require a live MQTT broker — construct `Config(MQTT_ENABLED=False)` when a test needs a `Config`/`app` instance. Add unit tests alongside state/persistence changes, and integration coverage in `test_app.py` for routes or Socket.IO events.

## Commit & PR Guidelines

Concise, imperative commit subjects (e.g. `Add retention test for message store`), one logical change per commit. PRs should explain behavior changes, list verification commands run, and call out configuration or schema effects. Never commit `.env`, credentials, or generated SQLite files (e.g. `mqtt_dashboard.db`).
