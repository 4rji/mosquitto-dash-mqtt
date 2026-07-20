"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class Config:
    """Runtime settings for the web server and MQTT connection."""

    MQTT_HOST: str = os.getenv("MQTT_HOST", "10.10.65.x")
    MQTT_PORT: int = int(os.getenv("MQTT_PORT", "1883"))
    MQTT_USERNAME: str = os.getenv("MQTT_USERNAME", "")
    MQTT_PASSWORD: str = os.getenv("MQTT_PASSWORD", "")
    MQTT_TLS_ENABLED: bool = _env_bool("MQTT_TLS_ENABLED", False)
    MQTT_CA_CERT: str = os.getenv("MQTT_CA_CERT", "")
    MQTT_TOPIC: str = os.getenv("MQTT_TOPIC", "#")
    MQTT_KEEPALIVE: int = int(os.getenv("MQTT_KEEPALIVE", "60"))
    MQTT_ENABLED: bool = _env_bool("MQTT_ENABLED", True)
    SYSTEM_TOPIC_SUFFIX: str = os.getenv("SYSTEM_TOPIC_SUFFIX", "system")

    APP_HOST: str = os.getenv("APP_HOST", "0.0.0.0")
    APP_PORT: int = int(os.getenv("APP_PORT", "5000"))
    APP_DEBUG: bool = _env_bool("APP_DEBUG", False)
    SECRET_KEY: str = os.getenv("SECRET_KEY", "mqtt-dashboard-development-key")

    MESSAGE_LIMIT: int = int(os.getenv("MESSAGE_LIMIT", "1000"))
    DEVICE_ONLINE_SECONDS: int = int(os.getenv("DEVICE_ONLINE_SECONDS", "60"))
    SOCKET_BATCH_INTERVAL: float = float(os.getenv("SOCKET_BATCH_INTERVAL", "0.05"))

    LOG_PERSISTENCE_ENABLED: bool = _env_bool("LOG_PERSISTENCE_ENABLED", True)
    LOG_DB_PATH: str = os.getenv("LOG_DB_PATH", "mqtt_dashboard.db")
    LOG_RETENTION: int = int(os.getenv("LOG_RETENTION", "100000"))

    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    AI_DASHBOARD_SAMPLE_SIZE: int = int(os.getenv("AI_DASHBOARD_SAMPLE_SIZE", "50"))
    AI_DASHBOARD_MAX_TOPICS: int = int(os.getenv("AI_DASHBOARD_MAX_TOPICS", "30"))
    AI_METRIC_SERIES_MAXLEN: int = int(os.getenv("AI_METRIC_SERIES_MAXLEN", "200"))
