# Mosquitto MQTT Dashboard — User Manual

## Overview

Mosquitto MQTT Dashboard is a real-time web dashboard that connects to an MQTT broker, displays incoming messages, tracks device activity, and provides basic system health metrics. It runs as a lightweight Flask/SocketIO server and updates the browser instantly via WebSocket.

---

## Requirements

| Component | Minimum version |
|-----------|----------------|
| Python    | 3.11            |
| pip       | 23+             |
| rsync     | any             |
| systemd   | 249+            |
| MQTT broker (e.g. Mosquitto) | 2.x |

The install script also requires **root/sudo** access.

---

## Installation

### 1. Clone or download the repository

```bash
git clone https://github.com/4rji/mosquitto-dash-mqtt.git
cd mosquitto-dash-mqtt
```

### 2. Run the installer

```bash
sudo bash install.sh
```

The script will:

1. Copy the application files to `/opt/mosquitto-dash-mqtt`.
2. Create a Python virtual environment and install all dependencies.
3. Create `/opt/mosquitto-dash-mqtt/.env` from `.env.example` (only on first run).
4. Register and start the `mosquitto-dash` systemd service.

### 3. Configure the environment

Edit the environment file before (or immediately after) running the installer:

```bash
sudo nano /opt/mosquitto-dash-mqtt/.env
```

At minimum, set `MQTT_HOST` to the IP or hostname of your MQTT broker.

---

## Configuration Reference

All settings are read from `/opt/mosquitto-dash-mqtt/.env` (environment variables).

| Variable | Default | Description |
|----------|---------|-------------|
| `MQTT_HOST` | `10.10.65.x` | MQTT broker address |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `MQTT_USERNAME` | _(empty)_ | Broker username (leave empty if not required) |
| `MQTT_PASSWORD` | _(empty)_ | Broker password |
| `MQTT_TOPIC` | `#` | Topic filter (`#` subscribes to all topics) |
| `MQTT_KEEPALIVE` | `60` | Keep-alive interval in seconds |
| `MQTT_ENABLED` | `true` | Set to `false` to run without an MQTT connection |
| `SYSTEM_TOPIC_SUFFIX` | `system` | Topic suffix used to identify system-health messages |
| `APP_HOST` | `0.0.0.0` | Network interface the web server binds to |
| `APP_PORT` | `5000` | TCP port for the web dashboard |
| `APP_DEBUG` | `false` | Enable Flask debug mode (do not use in production) |
| `SECRET_KEY` | _(development key)_ | Flask session secret — change this in production |
| `MESSAGE_LIMIT` | `1000` | Maximum number of messages kept in memory |
| `DEVICE_ONLINE_SECONDS` | `60` | Seconds of silence before a device is considered offline |
| `SOCKET_BATCH_INTERVAL` | `0.05` | WebSocket batch flush interval in seconds |
| `LOG_PERSISTENCE_ENABLED` | `true` | Persist messages to SQLite |
| `LOG_DB_PATH` | `mqtt_dashboard.db` | SQLite database file path |
| `LOG_RETENTION` | `100000` | Maximum rows kept in the database |

After changing the `.env` file, restart the service:

```bash
sudo systemctl restart mosquitto-dash
```

---

## Accessing the Dashboard

Open a browser and navigate to:

```
http://<server-ip>:5000
```

Replace `<server-ip>` with the IP address of the machine where the dashboard is installed, and `5000` with the value of `APP_PORT` if you changed it.

---

## Dashboard Features

### Message Feed

The main panel displays MQTT messages in real time as they arrive from the broker. Each row shows:

- **Timestamp** — when the message was received.
- **Topic** — the MQTT topic path.
- **Payload** — the raw message payload.
- **QoS** — quality-of-service level (0, 1, or 2).
- **Retain** flag — whether the broker retained the message.

### Device List

A sidebar lists every device (topic prefix) that has published at least one message. Devices that have not published within `DEVICE_ONLINE_SECONDS` are shown as **offline**.

### Statistics Panel

Displays aggregate counters updated every second:

- Total messages received.
- Messages per second (current rate).
- Number of distinct topics seen.
- Number of active (online) devices.

### System Health Tab

When a device publishes to a topic ending in `/system` (configurable via `SYSTEM_TOPIC_SUFFIX`), the dashboard parses the payload as a JSON system snapshot and renders CPU, memory, and disk metrics on the System Health tab.

### MQTT Connection Status

A status indicator in the header shows whether the dashboard is currently connected to the broker. It updates automatically when the connection is established or lost.

---

## Managing the Service

| Task | Command |
|------|---------|
| Check status | `sudo systemctl status mosquitto-dash` |
| View live logs | `sudo journalctl -u mosquitto-dash -f` |
| Stop the service | `sudo systemctl stop mosquitto-dash` |
| Start the service | `sudo systemctl start mosquitto-dash` |
| Restart the service | `sudo systemctl restart mosquitto-dash` |
| Disable autostart | `sudo systemctl disable mosquitto-dash` |
| Enable autostart | `sudo systemctl enable mosquitto-dash` |

---

## Updating

To deploy a new version, pull the latest code and re-run the installer:

```bash
git pull
sudo bash install.sh
```

The installer preserves the existing `/opt/mosquitto-dash-mqtt/.env` file so your configuration is not overwritten.

---

## Uninstalling

```bash
sudo systemctl stop mosquitto-dash
sudo systemctl disable mosquitto-dash
sudo rm /etc/systemd/system/mosquitto-dash.service
sudo systemctl daemon-reload
sudo rm -rf /opt/mosquitto-dash-mqtt
```

---

## Troubleshooting

### Dashboard does not load

1. Check that the service is running: `sudo systemctl status mosquitto-dash`
2. Check for errors: `sudo journalctl -u mosquitto-dash -n 50`
3. Verify `APP_PORT` is not blocked by a firewall.

### No messages appear

1. Confirm `MQTT_HOST` points to the correct broker.
2. Test broker connectivity from the server: `mosquitto_sub -h $MQTT_HOST -t '#' -v`
3. Check credentials (`MQTT_USERNAME` / `MQTT_PASSWORD`) if the broker requires authentication.
4. Verify `MQTT_TOPIC` matches the topics your devices publish to.

### Service fails to start

1. Run `sudo journalctl -u mosquitto-dash -b` to see the full boot log.
2. Ensure Python 3.11+ is installed: `python3 --version`
3. Re-run `sudo bash install.sh` to rebuild the virtual environment.

### Messages are not persisted after restart

- Check that `LOG_PERSISTENCE_ENABLED=true` in `.env`.
- Verify the process user has write access to the directory containing `LOG_DB_PATH`.

---

## Security Notes

- Change `SECRET_KEY` to a long random string before exposing the dashboard to a network.
- If the broker requires authentication, set `MQTT_USERNAME` and `MQTT_PASSWORD` and restrict permissions on `.env` (`chmod 600`).
- Do not set `APP_DEBUG=true` in production — it enables the Werkzeug debugger which allows arbitrary code execution.
- Consider placing the dashboard behind a reverse proxy (nginx, Caddy) with HTTPS and basic authentication if it is accessible from outside a trusted network.
