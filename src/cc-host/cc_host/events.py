from __future__ import annotations

import hashlib
import uuid
from typing import Any

from .util import rfc3339


SCHEMA_VERSION = 1

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

RUN_EVENTS = {"run_started", "run_updated", "run_closed"}
DROPPABLE_EVENT_TYPES = {"tool_heartbeat", "tool_activity", "token_delta"}
NEVER_DROP_EVENT_TYPES = {
    "run_started",
    "run_closed",
    "lane_started",
    "lane_stopped",
    "tool_started",
    "tool_completed",
    "tool_failed",
    "producer_dropped_events",
}
DROP_ORDER = ("tool_heartbeat", "tool_activity", "token_delta")


def build_event(**fields: Any) -> dict[str, Any]:
    event: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "event_id": fields.pop("event_id", str(uuid.uuid4())),
        "ts": rfc3339(fields.pop("ts", None)),
        "ingest_seq": None,
    }
    event.update(fields)
    if "event_type" not in event:
        raise ValueError("event_type is required")
    if "source_seq" not in event:
        raise ValueError("source_seq is required")
    if "source" not in event:
        raise ValueError("source is required")
    if "source_instance_id" not in event:
        raise ValueError("source_instance_id is required")
    if "run_id" not in event:
        raise ValueError("run_id is required")
    if "session_id" not in event:
        raise ValueError("session_id is required")
    event.setdefault("orchestrator", "claude-code")
    if event["event_type"] in RUN_EVENTS:
        event.pop("agent_id", None)
        event.pop("quarantine", None)
        event.pop("identity_error", None)
    else:
        event.setdefault("quarantine", False)
    return {key: value for key, value in event.items() if key in KNOWN_TOP_LEVEL_FIELDS and value is not None}


def final_message_fields(message: str, preview_chars: int = 256) -> dict[str, str]:
    return {
        "final_message_preview": message[:preview_chars],
        "final_message_sha256": hashlib.sha256(message.encode("utf-8")).hexdigest(),
    }


def prompt_hash(prompt: str) -> str:
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()
