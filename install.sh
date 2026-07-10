#!/usr/bin/env bash
# Installs mosquitto-dash-mqtt to /opt and registers it as a systemd service.
# Must be run with sudo: sudo bash install.sh

set -euo pipefail

INSTALL_DIR="/opt/mosquitto-dash-mqtt"
SERVICE_NAME="mosquitto-dash"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
RUN_USER="${SUDO_USER:-root}"

# ── Checks ────────────────────────────────────────────────────────────────────

if [[ $EUID -ne 0 ]]; then
    echo "ERROR: Run this script with sudo." >&2
    exit 1
fi

if ! command -v python3 &>/dev/null; then
    echo "ERROR: python3 is required but was not found." >&2
    exit 1
fi

PYTHON=$(command -v python3)

# ── Copy application files ────────────────────────────────────────────────────

echo "==> Creating ${INSTALL_DIR}"
mkdir -p "${INSTALL_DIR}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Copying application files"
rsync -a --delete \
    --exclude='.git' \
    --exclude='.venv' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='.DS_Store' \
    --exclude='tests/' \
    --exclude='.env' \
    --exclude='install.sh' \
    --exclude='pasos.md' \
    --exclude='dashboard.webp' \
    "${SCRIPT_DIR}/" "${INSTALL_DIR}/"

# ── Environment file ──────────────────────────────────────────────────────────

if [[ ! -f "${INSTALL_DIR}/.env" ]]; then
    echo "==> Creating ${INSTALL_DIR}/.env from .env.example"
    cp "${INSTALL_DIR}/.env.example" "${INSTALL_DIR}/.env"
    echo ""
    echo "  IMPORTANT: Edit ${INSTALL_DIR}/.env before starting the service."
    echo "  At minimum set MQTT_HOST to your broker address."
    echo ""
fi

# ── Python virtual environment ────────────────────────────────────────────────

echo "==> Setting up Python virtual environment"
"${PYTHON}" -m venv "${INSTALL_DIR}/.venv"
"${INSTALL_DIR}/.venv/bin/pip" install --quiet --upgrade pip
"${INSTALL_DIR}/.venv/bin/pip" install --quiet -r "${INSTALL_DIR}/requirements.txt"

# ── File ownership ────────────────────────────────────────────────────────────

echo "==> Setting ownership to ${RUN_USER}"
chown -R "${RUN_USER}:${RUN_USER}" "${INSTALL_DIR}"
chmod 600 "${INSTALL_DIR}/.env"

# ── systemd service ───────────────────────────────────────────────────────────

echo "==> Writing ${SERVICE_FILE}"
cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=Mosquitto MQTT Dashboard
After=network.target

[Service]
Type=simple
User=${RUN_USER}
WorkingDirectory=${INSTALL_DIR}
EnvironmentFile=${INSTALL_DIR}/.env
ExecStart=${INSTALL_DIR}/.venv/bin/python app.py
Restart=on-failure
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

[Install]
WantedBy=multi-user.target
EOF

echo "==> Enabling and starting ${SERVICE_NAME}"
systemctl daemon-reload
systemctl enable --now "${SERVICE_NAME}"

# ── Done ──────────────────────────────────────────────────────────────────────

echo ""
echo "Installation complete."
echo ""
echo "Useful commands:"
echo "  Status : sudo systemctl status ${SERVICE_NAME}"
echo "  Logs   : sudo journalctl -u ${SERVICE_NAME} -f"
echo "  Stop   : sudo systemctl stop ${SERVICE_NAME}"
echo "  Restart: sudo systemctl restart ${SERVICE_NAME}"
echo ""
echo "Dashboard URL: http://$(hostname -I | awk '{print $1}'):${APP_PORT:-5000}"
