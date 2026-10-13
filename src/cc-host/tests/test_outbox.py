from __future__ import annotations

import json
from datetime import UTC, datetime

from cc_host.events import build_event
from cc_host.outbox import Outbox


NOW = datetime(2026, 5, 31, 18, 0, 0, tzinfo=UTC)


def event(event_type: str, source_instance: str, seq: int, **extra: object) -> dict[str, object]:
    base = {
        "event_type": event_type,
        "run_id": "run-1",
        "session_id": "session-1",
        "orchestrator": "claude-code",
        "agent_id": "agent-1",
        "agent_type": "codex",
        "provider": "openai",
        "source": "shim",
        "source_instance_id": source_instance,
        "source_seq": seq,
        "ts": NOW,
    }
    base.update(extra)
    return build_event(**base)


def test_next_sendable_preserves_fifo_per_source(tmp_path) -> None:
    outbox = Outbox(tmp_path / "outbox.db", max_events=100)
    newer_a = event("tool_heartbeat", "shim:tool-a", 2, tool_run_id="tool-a")
    older_b = event("tool_started", "shim:tool-b", 1, tool_run_id="tool-b")
    older_a = event("tool_started", "shim:tool-a", 1, tool_run_id="tool-a")

    outbox.append(newer_a)
    outbox.append(older_b)
    outbox.append(older_a)

    first = outbox.next_sendable(now=NOW)
    assert first is not None
    assert (first.event["source_instance_id"], first.event["source_seq"]) == ("shim:tool-b", 1)
    outbox.mark_sent(first.id)

    second = outbox.next_sendable(now=NOW)
    assert second is not None
    assert (second.event["source_instance_id"], second.event["source_seq"]) == ("shim:tool-a", 1)
    outbox.mark_sent(second.id)

    third = outbox.next_sendable(now=NOW)
    assert third is not None
    assert (third.event["source_instance_id"], third.event["source_seq"]) == ("shim:tool-a", 2)


def test_priority_drop_sheds_droppable_events_and_reports_loss(tmp_path) -> None:
    outbox = Outbox(tmp_path / "outbox.db", max_events=4)
    outbox.append(event("run_started", "hook:session-1:UserPromptSubmit:one", 1, source="hook", agent_id=None))
    outbox.append(event("lane_started", "hook:session-1:SubagentStart:one", 1, source="hook"))
    outbox.append(event("tool_started", "shim:tool-a", 1, tool_run_id="tool-a"))
    outbox.append(event("tool_heartbeat", "shim:tool-a", 2, tool_run_id="tool-a"))
    outbox.append(event("tool_activity", "shim:tool-a", 3, tool_run_id="tool-a", activity="editing"))
    outbox.append(
        event("tool_completed", "shim:tool-a", 4, tool_run_id="tool-a", exit_code=0, cost_source="unavailable")
    )

    unsent_types = [row.event["event_type"] for row in outbox.unsent_rows()]
    assert "run_started" in unsent_types
    assert "lane_started" in unsent_types
    assert "tool_started" in unsent_types
    assert "tool_completed" in unsent_types
    assert "tool_heartbeat" not in unsent_types
    assert "tool_activity" not in unsent_types

    drops = outbox.pending_drop_reports()
    assert len(drops) == 1
    assert drops[0].dropped_count == 2
    assert json.loads(drops[0].dropped_event_types_json) == ["tool_heartbeat", "tool_activity"]

    reports = outbox.enqueue_drop_reports(now=NOW)
    assert len(reports) == 1
    report = reports[0].event
    assert report["event_type"] == "producer_dropped_events"
    assert report["dropped_count"] == 2
    assert report["dropped_event_types"] == ["tool_heartbeat", "tool_activity"]
    assert report["loss_scope"] == "non_terminal_only"
