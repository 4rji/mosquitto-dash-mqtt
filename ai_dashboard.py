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
