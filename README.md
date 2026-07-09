![Digi MQTT Monitor dashboard](dashboard.webp)

# Digi Router MQTT Dashboard

A real-time, dark-theme dashboard for observing every message published to a Mosquitto broker by Digi routers. The server subscribes to `#`, keeps a bounded in-memory view of recent traffic, and pushes batched updates to browsers over WebSockets.

## Features

- Broker connection state with automatic MQTT reconnection
- Live SIEM-style feed with the newest messages first
- Total messages, one-second message rate, topic/device counts, uptime, and last-seen statistics
- Search across topic, payload, and inferred device
- Topic explorer with raw payload, payload size, and collapsible JSON
- Device cards inferred from the first segment of multi-level topics
- Lossless binary representation as hexadecimal
- Approximately 1,000 recent messages retained in memory by default
- Batched browser delivery to remain responsive under bursty traffic

## Requirements

- Python 3.12 or newer
- Access to a Mosquitto broker
- A modern browser with WebSocket support

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

Configuration is centralized in `config.py` and can be overridden with environment variables.

| Variable | Default | Purpose |
| --- | --- | --- |
| `MQTT_HOST` | `10.10.65.x` | Mosquitto hostname or IP address |
| `MQTT_PORT` | `1883` | Plain MQTT port |
| `MQTT_USERNAME` | empty | Optional broker username |
| `MQTT_PASSWORD` | empty | Optional broker password |
| `MQTT_TOPIC` | `#` | Subscription filter |
| `SYSTEM_TOPIC_SUFFIX` | `system` | Last topic segment identifying system telemetry (e.g. `router01/system`) |
| `MQTT_KEEPALIVE` | `60` | MQTT keepalive in seconds |
| `MESSAGE_LIMIT` | `1000` | Maximum recent messages retained |
| `DEVICE_ONLINE_SECONDS` | `60` | Recent-activity window for online state |
| `LOG_PERSISTENCE_ENABLED` | `true` | Persist every message to SQLite and reload on startup |
| `LOG_DB_PATH` | `mqtt_dashboard.db` | SQLite database file path |
| `LOG_RETENTION` | `100000` | Maximum messages retained in SQLite (oldest pruned) |
| `APP_HOST` | `0.0.0.0` | Web server bind address |
| `APP_PORT` | `5000` | Web server port |
| `APP_DEBUG` | `false` | Flask debug mode |

For example:

```bash
export MQTT_HOST=10.10.65.42
export MQTT_PORT=1883
python app.py
```

No username, password, or TLS is used when the authentication variables are empty.

## Running

```bash
python app.py
```

Open [http://localhost:5000](http://localhost:5000). The browser uses a WebSocket-only Socket.IO connection; there is no polling or page refresh.

## Topic and payload behavior

The MQTT client subscribes to `#`, so all application topics are received. Mosquitto system topics beginning with `$` are not matched by `#` under MQTT rules; use an additional `$SYS/#` subscription later if broker telemetry is needed.

No payload schema is assumed:

- Valid UTF-8 is displayed exactly as text.
- Valid JSON is also parsed into a collapsible, syntax-colored tree.
- Non-UTF-8 bytes are displayed losslessly as a hexadecimal value prefixed with `0x`.

For device inference, the first non-empty segment of a multi-level topic is used:

```text
router01/status       -> router01
router01/interfaces   -> router01
single-level-topic    -> Unknown
```

## Tests

The core state tests use the Python standard library:

```bash
python -m unittest discover -s tests
```

## Docker

```bash
docker build -t digi-mqtt-dashboard .
docker run --rm -p 5000:5000 \
  -e MQTT_HOST=10.10.65.42 \
  digi-mqtt-dashboard
```

If Mosquitto runs on the Docker host, set `MQTT_HOST=host.docker.internal` on macOS or Windows.

## Architecture

```text
Mosquitto
    │
    ▼
MQTTClient (paho-mqtt)
    │
    ├── DashboardState (bounded history and aggregates)
    │
    ▼
SocketEventBatcher
    │
    ▼
Flask-SocketIO WebSocket
    │
    ▼
Vanilla JavaScript dashboard
```

`MQTTClient`, `DashboardState`, and `SocketEventBatcher` have narrow responsibilities. Persistence, alerting, publishing, authentication, REST endpoints, and health checks can be added as separate services without changing the current message-ingestion path.
