"""OpenAI-backed generation of AI dashboard specs from MQTT context."""

from __future__ import annotations

import json
import re
from typing import Any, Protocol

from dashboard_state import DashboardState
from message_store import MessageStore

_ALLOWED_MODES = {"point", "series"}
_ALLOWED_VALUE_SOURCES = {"payload", "topic"}

_SYSTEM_PROMPT = (
    "You design a single Vega-Lite v5 chart for live MQTT telemetry. "
    "Reply with a single JSON object only, no prose, matching exactly this shape:\n"
    '{"spec": <Vega-Lite v5 spec object>, "metrics": ['
    '{"id": <string>, "label": <string>, "topic_filter": <string>, '
    '"value_source": "payload" | "topic", "json_path": <string>, '
    '"topic_regex": <string>, "mode": "point" | "series"}, ...]}\n'
    "\n"
    "The chart's data source MUST be named \"table\" (spec.data.name == \"table\") "
    "and rows have exactly these fields: metric (string), group (string), "
    "value (number), ts (ISO-8601 string).\n"
    "\n"
    "Each entry in \"metrics\" extracts one live value stream from the sampled MQTT "
    "traffic below. topic_filter is a real MQTT subscription wildcard, matched on whole "
    "'/'-separated segments only: '+' matches exactly one whole segment, '#' matches all "
    "remaining segments and must be the last segment, either alone or immediately after "
    "a '/' (e.g. \"router/+/temperature\" or \"router/#\"). Neither wildcard does "
    "substring or prefix matching within a segment — \"Health #\" does NOT match a topic "
    "like \"Health 100\" (that is one segment, not two). If the value lives in a single "
    "segment with no reliable '/' hierarchy to filter on, just use topic_filter \"#\" and "
    "rely on topic_regex (below) for the actual selectivity. mode is \"point\" (latest "
    "value per device, for bar/pie/gauge charts) or \"series\" (value over time, for "
    "line/area charts).\n"
    "\n"
    "Most telemetry is JSON in the payload: set value_source to \"payload\" and "
    "json_path to a dot-path into the decoded JSON payload (e.g. \"load_avg.1min\"); "
    "omit topic_regex.\n"
    "Some devices instead encode the value directly in the topic string itself, with a "
    "mostly-constant payload (e.g. topic \"Health 100\", payload \"log-status-ok\"). For "
    "those, set value_source to \"topic\" and topic_regex to a regular expression with "
    "exactly ONE capturing group around the numeric value, matched with re.search against "
    "the full topic string (e.g. \"Health (\\\\d+)\" matches topic \"Health 100\"); omit "
    "json_path. Look at the sample traffic below to decide which source applies.\n"
    "\n"
    "IMPORTANT: any transform filter in the spec that compares against the \"metric\" "
    "field MUST use the exact same string as that metric's \"id\" — never its label or "
    "any other name.\n"
    "\n"
    "Vega-Lite's \"shape\" encoding channel only accepts these values: circle, square, "
    "cross, diamond, triangle-up, triangle-down, triangle-right, triangle-left, "
    "triangle, arrow, wedge, stroke, none. There is no \"heart\" shape and no native "
    "liquid-fill gauge mark. To represent something like a heart-shaped health "
    "indicator, use a layered spec with text marks and a glyph instead, for example "
    "(single metric \"health\", mode \"point\"):\n"
    '{"data": {"name": "table"}, "transform": [{"filter": "datum.metric == \'health\'"}], '
    '"layer": [\n'
    '  {"mark": {"type": "text", "fontSize": 80, "text": "' + "❤" + '"}, '
    '"encoding": {"opacity": {"field": "value", "type": "quantitative", '
    '"scale": {"domain": [0, 100], "range": [0.15, 1]}}, '
    '"color": {"condition": {"test": "datum.value <= 0", "value": "red"}, "value": "black"}}},\n'
    '  {"mark": {"type": "text", "fontSize": 20, "dy": 60}, '
    '"encoding": {"text": {"condition": {"test": "datum.value <= 0", "value": "ALERT"}, '
    '"field": "value", "type": "quantitative"}, '
    '"color": {"condition": {"test": "datum.value <= 0", "value": "red"}, "value": "black"}}}\n'
    "]}\n"
    "This pattern (glyph opacity scaled by value, conditional red alert at the low "
    "threshold, a second text layer for the number) generalizes to other single-value "
    "indicators the user asks for — reuse it rather than inventing unsupported marks. "
    "Copy the glyph character above verbatim rather than substituting a different emoji: "
    "write it as a literal character in your JSON string, never as a \\u escape — many "
    "emoji sit above U+FFFF and require a surrogate pair when escaped, which is easy to "
    "get wrong and will corrupt the glyph.\n"
    "\n"
    "\"metrics\" must contain at least one entry."
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
            response_format={"type": "json_object"},
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
        if metric.get("mode") not in _ALLOWED_MODES:
            raise AIDashboardError(f"metric {metric_id}: mode must be one of {sorted(_ALLOWED_MODES)}")

        value_source = metric.get("value_source")
        if value_source not in _ALLOWED_VALUE_SOURCES:
            raise AIDashboardError(
                f"metric {metric_id}: value_source must be one of {sorted(_ALLOWED_VALUE_SOURCES)}"
            )
        if value_source == "payload":
            if not isinstance(metric.get("json_path"), str) or not metric["json_path"]:
                raise AIDashboardError(
                    f"metric {metric_id}: json_path must be a non-empty string when value_source is \"payload\""
                )
        else:
            topic_regex = metric.get("topic_regex")
            if not isinstance(topic_regex, str) or not topic_regex:
                raise AIDashboardError(
                    f"metric {metric_id}: topic_regex must be a non-empty string when value_source is \"topic\""
                )
            try:
                compiled = re.compile(topic_regex)
            except re.error as error:
                raise AIDashboardError(f"metric {metric_id}: topic_regex is not a valid regex: {error}")
            if compiled.groups != 1:
                raise AIDashboardError(
                    f"metric {metric_id}: topic_regex must have exactly one capturing group"
                )

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
