from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from .config import LivenessConfig


SCHEMA_VERSION = 1

EVENT_TYPES = {
    "run_started",
    "run_updated",
    "run_closed",
    "lane_started",
    "lane_stopped",
    "tool_started",
    "tool_heartbeat",
    "tool_activity",
    "token_delta",
    "tool_completed",
    "tool_failed",
    "producer_dropped_events",
}
RUN_EVENTS = {"run_started", "run_updated", "run_closed"}
LANE_EVENTS = {"lane_started", "lane_stopped"}
TOOL_EVENTS = {"tool_started", "tool_heartbeat", "tool_activity", "token_delta", "tool_completed", "tool_failed"}
TERMINAL_EVENTS = {"tool_completed", "tool_failed", "lane_stopped"}
SOURCES = {"hook", "transcript-tailer", "shim", "reducer"}
DECLARED_OUTCOMES = {"success", "failure", "partial", "unknown"}
DROPPABLE_EVENT_TYPES = {"tool_heartbeat", "tool_activity", "token_delta"}
DERIVED_STATE_FIELDS = {"status", "stuck", "heartbeat_age"}

KNOWN_TOP_LEVEL_FIELDS = {
    "schema_version",
    "event_id",
    "event_type",
    "ts",
    "run_id",
    "session_id",
    "orchestrator",
    "agent_id",
    "quarantine",
    "identity_error",
    "agent_type",
    "provider",
    "model",
    "tool_run_id",
    "trace_id",
    "span_id",
    "source",
    "source_instance_id",
    "source_seq",
    "ingest_seq",
    "pid",
    "worktree",
    "branch",
    "role",
    "activity",
    "tokens_delta",
    "tokens_total",
    "cost_usd_total",
    "cost_source",
    "pricing_version",
    "stop_reason",
    "declared_outcome",
    "final_message",
    "final_message_preview",
    "final_message_sha256",
    "summary",
    "exit_code",
    "error",
    "prompt_hash",
    "cwd",
    "project",
    "operator_label",
    "dropped_count",
    "dropped_event_types",
    "loss_scope",
    "payload",
    "ext",
}

IDENTITY_RE = re.compile(r"^[A-Za-z0-9._:@/-]+$")
SUMMARY_RE = re.compile(r"^TELEMETRY_SUMMARY:\s*(.+?)\s*$", re.MULTILINE)


class ValidationError(ValueError):
    """Raised when a producer event violates the liveness ingest contract."""


def parse_timestamp(value: str) -> datetime:
    if not isinstance(value, str):
        raise ValidationError("ts must be an RFC3339 string")
    text = value
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValidationError("ts must be a valid RFC3339 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValidationError("ts must include a timezone")
    return parsed.astimezone(UTC)


def to_rfc3339(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def event_ts(event: dict[str, Any]) -> datetime:
    return parse_timestamp(str(event["ts"]))


def validate_event(raw: dict[str, Any], config: LivenessConfig, now: datetime | None = None) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValidationError("request JSON must be an object")
    now = (now or datetime.now(UTC)).astimezone(UTC)

    unknown = sorted(set(raw) - KNOWN_TOP_LEVEL_FIELDS)
    if unknown:
        raise ValidationError(f"unknown top-level field: {unknown[0]}")
    derived = sorted(set(raw) & DERIVED_STATE_FIELDS)
    if derived:
        raise ValidationError(f"producer-emitted derived field is forbidden: {derived[0]}")

    canonical = dict(raw)
    if canonical.get("schema_version") != SCHEMA_VERSION:
        raise ValidationError("schema_version must be 1")

    event_type = _required_str(canonical, "event_type")
    if event_type not in EVENT_TYPES:
        raise ValidationError(f"unknown event_type: {event_type}")

    event_id = _required_str(canonical, "event_id")
    if len(event_id) > config.event_id_max_len:
        raise ValidationError(f"event_id length exceeds {config.event_id_max_len}")
    try:
        uuid.UUID(event_id)
    except ValueError as exc:
        raise ValidationError("event_id must be a UUID") from exc

    run_id = _required_str(canonical, "run_id")
    if len(run_id) > config.run_id_max_len:
        raise ValidationError(f"run_id length exceeds {config.run_id_max_len}")
    _required_str(canonical, "session_id")

    produced_ts = parse_timestamp(_required_str(canonical, "ts"))
    if produced_ts < now - timedelta(seconds=config.timestamp_past_window_sec):
        raise ValidationError("ts is outside the allowed past sanity window")
    if produced_ts > now + timedelta(seconds=config.timestamp_future_window_sec):
        raise ValidationError("ts is outside the allowed future sanity window")
    canonical["ts"] = to_rfc3339(produced_ts)

    source = _required_str(canonical, "source")
    if source not in SOURCES:
        raise ValidationError(f"unknown source: {source}")
    source_instance_id = _required_str(canonical, "source_instance_id")
    _validate_source_instance(source, source_instance_id)
    source_seq = canonical.get("source_seq")
    if not isinstance(source_seq, int) or source_seq < 1:
        raise ValidationError("source_seq must be an integer >= 1")
    if source == "hook" and source_seq != 1:
        raise ValidationError("hook events must use source_seq = 1")

    ingest_seq = canonical.get("ingest_seq")
    if ingest_seq is not None:
        raise ValidationError("ingest_seq must be null or omitted on producer events")
    canonical["ingest_seq"] = None

    _normalize_ext(canonical)
    _normalize_agent_identity(canonical, event_type, config)
    _validate_event_specifics(canonical, event_type)
    _truncate_bounded_fields(canonical, config)
    _sanitize_final_message(canonical, config)
    _normalize_numeric_fields(canonical)
    return canonical


def serialize_event(event: dict[str, Any]) -> str:
    return json.dumps(event, sort_keys=True, separators=(",", ":"))


def quarantine_agent_id(event: dict[str, Any]) -> str:
    bucket = str(event.get("ts", ""))[:16]
    material = "|".join(
        [
            str(event.get("source_instance_id", "")),
            str(event.get("tool_run_id", "")),
            str(event.get("pid", "")),
            bucket,
        ]
    )
    return f"quarantine:{hashlib.sha256(material.encode()).hexdigest()[:16]}"


def _required_str(event: dict[str, Any], field: str) -> str:
    value = event.get(field)
    if not isinstance(value, str) or value == "":
        raise ValidationError(f"{field} is required")
    return value


def _validate_source_instance(source: str, source_instance_id: str) -> None:
    prefixes = {
        "hook": "hook:",
        "transcript-tailer": "tailer:",
        "shim": "shim:",
        "reducer": "reducer:",
    }
    if not source_instance_id.startswith(prefixes[source]):
        raise ValidationError(f"source_instance_id must start with {prefixes[source]} for source {source}")


def _normalize_ext(event: dict[str, Any]) -> None:
    ext: dict[str, Any] = {}
    if isinstance(event.get("ext"), dict):
        ext.update(event["ext"])
    payload = event.pop("payload", None)
    if payload is not None:
        if not isinstance(payload, dict):
            raise ValidationError("payload must be an object when present")
        payload_ext = payload.get("ext", {})
        if not isinstance(payload_ext, dict):
            raise ValidationError("payload.ext must be an object when present")
        ext.update(payload_ext)
    event["ext"] = ext


def _normalize_agent_identity(event: dict[str, Any], event_type: str, config: LivenessConfig) -> None:
    agent_id = event.get("agent_id")
    if event_type in RUN_EVENTS:
        if agent_id is not None:
            raise ValidationError("agent_id must be null or omitted for run events")
        event["agent_id"] = None
        return

    if isinstance(agent_id, str) and len(agent_id) > config.agent_id_max_len:
        raise ValidationError(f"agent_id length exceeds {config.agent_id_max_len}")
    if isinstance(agent_id, str) and agent_id and IDENTITY_RE.match(agent_id):
        event["quarantine"] = bool(event.get("quarantine", False))
        return

    event["agent_id"] = quarantine_agent_id(event)
    event["quarantine"] = True
    event["identity_error"] = "missing_or_malformed_agent_id"


def _validate_event_specifics(event: dict[str, Any], event_type: str) -> None:
    if event_type in TOOL_EVENTS:
        _required_str(event, "tool_run_id")
    if event_type == "lane_stopped":
        outcome = event.get("declared_outcome", "unknown")
        if outcome not in DECLARED_OUTCOMES:
            raise ValidationError("declared_outcome must be success, failure, partial, or unknown")
        event["declared_outcome"] = outcome
    if event_type == "producer_dropped_events":
        dropped_count = event.get("dropped_count")
        if not isinstance(dropped_count, int) or dropped_count < 1:
            raise ValidationError("producer_dropped_events requires dropped_count >= 1")
        dropped_types = event.get("dropped_event_types")
        if not isinstance(dropped_types, list) or not dropped_types:
            raise ValidationError("producer_dropped_events requires dropped_event_types")
        invalid = [item for item in dropped_types if item not in DROPPABLE_EVENT_TYPES]
        if invalid:
            raise ValidationError(f"invalid dropped_event_type: {invalid[0]}")
        if not isinstance(event.get("loss_scope"), str) or not event["loss_scope"]:
            raise ValidationError("producer_dropped_events requires loss_scope")


def _truncate_bounded_fields(event: dict[str, Any], config: LivenessConfig) -> None:
    _truncate_str(event, "summary", config.summary_max_chars)
    _truncate_str(event, "worktree", config.worktree_max_chars)
    _truncate_str(event, "model", config.model_max_chars)
    _truncate_str(event, "activity", config.activity_max_chars)
    if isinstance(event.get("error"), str):
        _truncate_str(event, "error", config.error_max_chars)
    elif isinstance(event.get("error"), dict):
        event["error"] = _truncate_nested_strings(event["error"], config.error_max_chars)


def _truncate_str(event: dict[str, Any], field: str, max_chars: int) -> None:
    value = event.get(field)
    if isinstance(value, str) and len(value) > max_chars:
        event[field] = value[:max_chars]


def _truncate_nested_strings(value: Any, max_chars: int) -> Any:
    if isinstance(value, str):
        return value[:max_chars]
    if isinstance(value, dict):
        return {str(k): _truncate_nested_strings(v, max_chars) for k, v in value.items()}
    if isinstance(value, list):
        return [_truncate_nested_strings(item, max_chars) for item in value]
    return value


def _sanitize_final_message(event: dict[str, Any], config: LivenessConfig) -> None:
    final_message = event.get("final_message")
    if not isinstance(final_message, str):
        return

    event["final_message_preview"] = final_message[: config.final_message_preview_chars]
    event["final_message_sha256"] = hashlib.sha256(final_message.encode()).hexdigest()
    match = SUMMARY_RE.search(final_message)
    if match:
        event["summary"] = match.group(1)[: config.summary_max_chars]
    elif not isinstance(event.get("summary"), str):
        event["summary"] = None
    if not config.store_full_final_message:
        event.pop("final_message", None)


def _normalize_numeric_fields(event: dict[str, Any]) -> None:
    for field in ("tokens_delta", "tokens_total"):
        value = event.get(field)
        if value is None:
            continue
        if not isinstance(value, dict):
            raise ValidationError(f"{field} must be an object")
        for direction in ("in", "out"):
            amount = value.get(direction, 0)
            if not isinstance(amount, (int, float)) or amount < 0:
                raise ValidationError(f"{field}.{direction} must be a non-negative number")
            value[direction] = amount
    cost = event.get("cost_usd_total")
    if cost is not None and (not isinstance(cost, (int, float)) or cost < 0):
        raise ValidationError("cost_usd_total must be a non-negative number")
