# SQLite Log Persistence — Design

**Date:** 2026-06-25
**Status:** Approved

## Goal

Persist every MQTT message to SQLite so history survives process restarts.
On startup the dashboard reloads the most recent messages into memory so the
live feed, topic explorer, devices, and system tabs are populated immediately.

## Decisions

- **What to persist:** all messages (full archive).
- **Read path:** load-on-startup — reload the last N messages into in-memory
  state; the rest of the UI is unchanged.
- **Retention:** by count — keep the last N rows, prune the oldest.

## Non-goals (YAGNI)

- No async write queue (single insert under a lock with WAL is ample for
  router telemetry rates).
- No in-UI history search endpoint (write + startup-load only).
- No per-metric time series (separate concern).

## Architecture & data flow

```
MQTT message
  -> app.handle_message
       ├─ state.record_message(...)   # pure, in-memory aggregates (unchanged behavior)
       └─ store.append(message)       # write-through to SQLite

Startup (create_app)
  -> rows = store.recent(MESSAGE_LIMIT)
  -> state.restore(rows)              # rebuild feed/topics/devices/system, preserve id+timestamp
```

The SQLite write lives in `app.handle_message`, **not** inside
`record_message`. Reason: startup replays stored messages back into the state
to rebuild aggregates; if `record_message` wrote to the DB, replay would
re-insert everything in a loop. Persistence is a separate concern wired at the
application layer.

## Components

### 1. `message_store.py` (new) — `MessageStore`

A thin wrapper around a SQLite connection.

Schema:

```sql
CREATE TABLE IF NOT EXISTS messages (
    id               INTEGER PRIMARY KEY,
    timestamp        TEXT NOT NULL,
    topic            TEXT NOT NULL,
    payload          TEXT NOT NULL,
    payload_size     INTEGER NOT NULL,
    payload_encoding TEXT NOT NULL,
    is_json          INTEGER NOT NULL,
    json_text        TEXT,
    device           TEXT NOT NULL,
    qos              INTEGER NOT NULL,
    retain           INTEGER NOT NULL
);
```

`id` mirrors the in-memory message id (not autoincrement) so ids are stable
across restart and the browser's id-based dedup keeps working.

Concurrency:

- `sqlite3.connect(path, check_same_thread=False)`.
- `PRAGMA journal_mode=WAL` for durability and reader/writer concurrency.
- All access guarded by a `threading.Lock`. Writes come from the single paho
  callback thread; the lock makes append/recent/prune safe regardless.

API:

- `append(message: dict) -> None` — insert one message (`json` serialized to
  `json_text` via `json.dumps`, or `NULL`). Prunes every `prune_every` inserts:
  `DELETE FROM messages WHERE id < (max_id - retention)`.
- `recent(limit: int) -> list[dict]` — newest `limit` rows returned
  **oldest-first**, each shaped exactly like an in-memory message
  (`is_json`/`retain` as bools, `json` parsed from `json_text`).
- `close() -> None`.

### 2. `dashboard_state.py` — targeted refactor + restore

- Extract the aggregate-update logic currently inline in `record_message`
  into `_apply_message_unlocked(message, received_monotonic)` (updates
  `_messages`, `_topics`, `_devices`, `_system`). Justified because `restore`
  must reuse exactly this logic.
- `record_message` builds the message dict (id, timestamp, decode) then calls
  `_apply_message_unlocked`. Behavior unchanged; existing tests stay green.
- `restore(messages: list[dict]) -> None` (new): for each stored message
  (oldest-first) call `_apply_message_unlocked`, preserving the message's own
  `id` and `timestamp`. After the batch, set `_next_id = max(id) + 1`.
  `restore` does **not** touch the live rate window and does **not** change
  `_total_messages` (that stat stays scoped to the current process session).

### 3. `app.py`

- In `create_app`, when `LOG_PERSISTENCE_ENABLED`, build
  `MessageStore(settings.LOG_DB_PATH, settings.LOG_RETENTION)`, then
  `state.restore(store.recent(settings.MESSAGE_LIMIT))`.
- `handle_message`: after `state.record_message(...)`, call
  `store.append(message)` (the returned dict).
- Register the store in `app.extensions["message_store"]`.
- `main()` `finally`: `store.close()`.

### 4. `config.py`

| Variable | Default | Purpose |
| --- | --- | --- |
| `LOG_PERSISTENCE_ENABLED` | `true` | Toggle SQLite persistence |
| `LOG_DB_PATH` | `mqtt_dashboard.db` | SQLite file path |
| `LOG_RETENTION` | `100000` | Max rows retained (prune oldest) |

### 5. Frontend

No changes. The feed renders from `snapshot.messages`, which now includes the
restored history on connect.

### 6. `.gitignore`

Add `*.db`, `*.db-wal`, `*.db-shm`.

## Error handling

- DB open/append failures are logged and must not crash the MQTT callback;
  the in-memory dashboard keeps working without persistence.
- Malformed `json_text` on reload: fall back to `json=None`, `is_json=False`.
- `total_messages` reflects only the current session, by design.

## Testing (strict TDD — tests first)

`tests/test_message_store.py` (new):

- `append` then `recent` returns rows oldest-first with all fields intact.
- `json` round-trips through `json_text`; non-JSON payload stores `NULL`.
- Retention prunes to the last N rows.
- Data persists across `close()` + reopen on the same path.

`tests/test_dashboard_state.py` (additions):

- `restore` rebuilds feed, topics, devices, and system aggregates.
- `restore` preserves ids/timestamps and a subsequent `record_message` gets
  `max(restored id) + 1`.
