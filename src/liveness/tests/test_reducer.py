from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from liveness.reducer import EventRow, Reducer, ReducerConfig
from liveness.store import LivenessStore


BASE = datetime(2026, 5, 31, 14, 22, 1, tzinfo=UTC)


def ts(offset: int = 0) -> str:
    return (BASE + timedelta(seconds=offset)).isoformat().replace("+00:00", "Z")


def row(ingest_seq: int, event: dict[str, Any]) -> EventRow:
    return EventRow(
        ingest_seq=ingest_seq,
        event_id=str(event["event_id"]),
        run_id=str(event["run_id"]),
        agent_id=event.get("agent_id") if isinstance(event.get("agent_id"), str) else None,
        event_type=str(event["event_type"]),
        ts=str(event["ts"]),
        source_instance_id=str(event["source_instance_id"]),
        source_seq=int(event["source_seq"]),
        payload_json=json.dumps(event, sort_keys=True),
    )


def event(
    event_id: str,
    event_type: str,
    *,
    ingest_seq: int = 1,
    run_id: str = "run-1",
    session_id: str = "session-1",
    agent_id: str | None = "agent-1",
    source: str = "shim",
    source_instance_id: str = "shim:tool-1",
    source_seq: int = 1,
    offset: int = 0,
    **extra: object,
) -> EventRow:
    payload: dict[str, object] = {
        "schema_version": 1,
        "event_id": event_id,
        "event_type": event_type,
        "ts": ts(offset),
        "run_id": run_id,
        "session_id": session_id,
        "orchestrator": "claude-code",
        "agent_id": agent_id,
        "quarantine": False,
        "agent_type": "codex",
        "provider": "openai",
        "model": "gpt-5.4-mini",
        "tool_run_id": "tool-1",
        "source": source,
        "source_instance_id": source_instance_id,
        "source_seq": source_seq,
        "ext": {},
    }
    payload.update(extra)
    if event_type.startswith("run_"):
        payload["agent_id"] = None
        payload.pop("tool_run_id", None)
    return row(ingest_seq, payload)


def seed_run_and_lane(reducer: Reducer) -> None:
    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-000000000001",
                "run_started",
                ingest_seq=1,
                agent_id=None,
                source="hook",
                source_instance_id="hook:session-1:UserPromptSubmit:one",
                source_seq=1,
            ),
            event(
                "00000000-0000-4000-8000-000000000002",
                "lane_started",
                ingest_seq=2,
                source="hook",
                source_instance_id="hook:session-1:SubagentStart:one",
                source_seq=1,
            ),
        ],
        now=BASE,
    )


def test_out_of_order_tool_started_after_tool_completed_does_not_regress() -> None:
    reducer = Reducer(ReducerConfig(gap_timeout_sec=0))
    seed_run_and_lane(reducer)

    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-000000000003",
                "tool_completed",
                ingest_seq=3,
                source_seq=2,
                exit_code=0,
                offset=2,
            ),
            event(
                "00000000-0000-4000-8000-000000000004",
                "tool_started",
                ingest_seq=4,
                source_seq=1,
                offset=1,
            ),
        ],
        now=BASE + timedelta(seconds=2),
    )

    lane = reducer.snapshot_run("run-1", now=BASE + timedelta(seconds=2))["lanes"][0]
    assert lane["tools"]["tool-1"]["state"] == "completed"


def test_duplicate_event_id_is_idempotent() -> None:
    reducer = Reducer(ReducerConfig(gap_timeout_sec=0))
    seed_run_and_lane(reducer)
    token = event(
        "00000000-0000-4000-8000-000000000005",
        "token_delta",
        ingest_seq=3,
        source_seq=1,
        tokens_delta={"in": 7, "out": 11},
    )

    reducer.apply_rows([token, token], now=BASE)

    metrics = reducer.metrics_snapshot(now=BASE)
    assert metrics.tokens[("openai", "claude-code", "in")] == 7
    assert metrics.tokens[("openai", "claude-code", "out")] == 11


def test_late_tool_completed_after_out_of_order_heartbeat_is_not_ignored() -> None:
    reducer = Reducer(ReducerConfig(gap_timeout_sec=0))
    seed_run_and_lane(reducer)

    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-000000000006",
                "tool_started",
                ingest_seq=3,
                source_seq=1,
                offset=1,
            ),
            event(
                "00000000-0000-4000-8000-000000000007",
                "tool_heartbeat",
                ingest_seq=4,
                source_seq=3,
                offset=3,
                activity="still working",
            ),
        ],
        now=BASE + timedelta(seconds=4),
    )
    reducer.flush_expired_gaps(now=BASE + timedelta(seconds=5))
    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-000000000008",
                "tool_completed",
                ingest_seq=5,
                source_seq=2,
                exit_code=0,
                offset=2,
            )
        ],
        now=BASE + timedelta(seconds=5),
    )

    lane = reducer.snapshot_run("run-1", now=BASE + timedelta(seconds=5))["lanes"][0]
    assert lane["tools"]["tool-1"]["state"] == "completed"


def test_terminal_tool_state_does_not_flip_to_different_terminal() -> None:
    reducer = Reducer(ReducerConfig(gap_timeout_sec=0))
    seed_run_and_lane(reducer)

    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-000000000023",
                "tool_started",
                ingest_seq=3,
                source_seq=1,
                offset=1,
            ),
            event(
                "00000000-0000-4000-8000-000000000024",
                "tool_failed",
                ingest_seq=4,
                source_seq=2,
                exit_code=1,
                offset=2,
            ),
            event(
                "00000000-0000-4000-8000-000000000025",
                "tool_completed",
                ingest_seq=5,
                source_seq=3,
                exit_code=0,
                offset=3,
            ),
        ],
        now=BASE + timedelta(seconds=3),
    )

    lane = reducer.snapshot_run("run-1", now=BASE + timedelta(seconds=3))["lanes"][0]
    assert lane["tools"]["tool-1"]["state"] == "failed"
    assert lane["state"] == "running_with_failed_child"


def test_source_seq_gap_is_buffered_then_resolved() -> None:
    reducer = Reducer(ReducerConfig(gap_timeout_sec=30))
    seed_run_and_lane(reducer)

    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-000000000009",
                "tool_started",
                ingest_seq=3,
                source_seq=1,
                offset=1,
            ),
            event(
                "00000000-0000-4000-8000-000000000010",
                "tool_heartbeat",
                ingest_seq=4,
                source_seq=3,
                offset=3,
                activity="buffered heartbeat",
            ),
        ],
        now=BASE + timedelta(seconds=3),
    )
    lane_before = reducer.snapshot_run("run-1", now=BASE + timedelta(seconds=3))["lanes"][0]
    assert lane_before["last_activity_ts"] == ts(1)

    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-000000000011",
                "tool_activity",
                ingest_seq=5,
                source_seq=2,
                offset=2,
                activity="gap closer",
            )
        ],
        now=BASE + timedelta(seconds=4),
    )

    lane_after = reducer.snapshot_run("run-1", now=BASE + timedelta(seconds=4))["lanes"][0]
    assert lane_after["last_activity_ts"] == ts(3)


def test_checkpoint_watermark_waits_for_buffered_ingest_gap() -> None:
    reducer = Reducer(ReducerConfig(gap_timeout_sec=30))
    seed_run_and_lane(reducer)

    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-000000000019",
                "tool_started",
                ingest_seq=3,
                source_seq=1,
                offset=1,
            ),
            event(
                "00000000-0000-4000-8000-000000000020",
                "tool_heartbeat",
                ingest_seq=4,
                source_seq=3,
                offset=3,
            ),
            event(
                "00000000-0000-4000-8000-000000000021",
                "producer_dropped_events",
                ingest_seq=5,
                source_instance_id="shim:tool-2",
                source_seq=1,
                dropped_count=1,
                dropped_event_types=["tool_activity"],
                loss_scope="non_terminal_only",
            ),
        ],
        now=BASE + timedelta(seconds=3),
    )

    assert reducer.has_pending_gaps() is True
    assert reducer.last_ingest_seq_applied == 3

    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-000000000022",
                "tool_activity",
                ingest_seq=6,
                source_seq=2,
                offset=2,
            )
        ],
        now=BASE + timedelta(seconds=4),
    )

    assert reducer.has_pending_gaps() is False
    assert reducer.last_ingest_seq_applied == 6


@pytest.mark.parametrize(
    ("declared_outcome", "child_failed", "expected"),
    [
        ("success", False, "done"),
        ("success", True, "done_with_warnings"),
        ("failure", False, "failed"),
        ("partial", False, "done_with_warnings"),
        ("unknown", False, "done"),
        ("unknown", True, "failed"),
    ],
)
def test_lane_stopped_resolution(declared_outcome: str, child_failed: bool, expected: str) -> None:
    reducer = Reducer(ReducerConfig(gap_timeout_sec=0))
    seed_run_and_lane(reducer)
    rows = [
        event(
            "00000000-0000-4000-8000-000000000012",
            "tool_started",
            ingest_seq=3,
            source_seq=1,
            offset=1,
        )
    ]
    if child_failed:
        rows.append(
            event(
                "00000000-0000-4000-8000-000000000013",
                "tool_failed",
                ingest_seq=4,
                source_seq=2,
                exit_code=1,
                error="boom",
                offset=2,
            )
        )
    rows.append(
        event(
            f"00000000-0000-4000-8000-0000000001{len(declared_outcome):02d}",
            "lane_stopped",
            ingest_seq=5,
            source="hook",
            source_instance_id="hook:session-1:SubagentStop:one",
            source_seq=1,
            declared_outcome=declared_outcome,
            stop_reason="normal_stop",
            offset=3,
        )
    )

    reducer.apply_rows(rows, now=BASE + timedelta(seconds=3))

    lane = reducer.snapshot_run("run-1", now=BASE + timedelta(seconds=3))["lanes"][0]
    assert lane["state"] == expected


def test_producer_dropped_events_marks_lane_telemetry_degraded() -> None:
    reducer = Reducer(ReducerConfig(gap_timeout_sec=0))
    seed_run_and_lane(reducer)

    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-000000000014",
                "producer_dropped_events",
                ingest_seq=3,
                source="shim",
                source_instance_id="shim:tool-1",
                source_seq=1,
                dropped_count=3,
                dropped_event_types=["tool_heartbeat"],
                loss_scope="non_terminal_only",
            )
        ],
        now=BASE,
    )

    lane = reducer.snapshot_run("run-1", now=BASE)["lanes"][0]
    assert lane["telemetry_degraded"] is True
    assert lane["dropped_events_total"] == 3


def test_producer_dropped_events_total_rolls_up_into_metrics() -> None:
    from liveness.metrics import prometheus_text

    reducer = Reducer(ReducerConfig(gap_timeout_sec=0))
    seed_run_and_lane(reducer)

    # No drops yet → emit a zero baseline so the panel resolves to 0, not "No data".
    snap = reducer.metrics_snapshot(now=BASE)
    text = prometheus_text(snap)
    assert 'liveness_producer_dropped_events_total{orchestrator="all",provider="all"} 0' in text

    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-000000000014",
                "producer_dropped_events",
                ingest_seq=3,
                source="shim",
                source_instance_id="shim:tool-1",
                source_seq=1,
                dropped_count=3,
                dropped_event_types=["tool_heartbeat"],
                loss_scope="non_terminal_only",
            )
        ],
        now=BASE,
    )

    snap = reducer.metrics_snapshot(now=BASE)
    # Rolled up by (provider, orchestrator) of the lane the drop report referenced.
    assert sum(snap.producer_dropped_events_total.values()) == 3
    text = prometheus_text(snap)
    assert "liveness_producer_dropped_events_total" in text
    # Loss is now visible and non-zero.
    assert any(
        line.startswith("liveness_producer_dropped_events_total{") and line.rstrip().endswith(" 3")
        for line in text.splitlines()
    )
    # Low-cardinality: never leak the raw source_instance_id or agent_id in labels.
    assert "tool-1" not in text
    assert "agent_id" not in text


def test_open_run_without_run_closed_becomes_abandoned_or_crashed() -> None:
    reducer = Reducer(ReducerConfig(stale_threshold_sec=10, stuck_threshold_sec=20, run_abandon_threshold_sec=30))
    seed_run_and_lane(reducer)
    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-000000000015",
                "lane_stopped",
                ingest_seq=3,
                source="hook",
                source_instance_id="hook:session-1:SubagentStop:one",
                source_seq=1,
                declared_outcome="success",
                stop_reason="normal_stop",
                offset=1,
            )
        ],
        now=BASE + timedelta(seconds=1),
    )

    abandoned = reducer.snapshot_run("run-1", now=BASE + timedelta(seconds=40))
    assert abandoned["state"] == "abandoned"

    crashed_reducer = Reducer(
        ReducerConfig(stale_threshold_sec=10, stuck_threshold_sec=20, run_abandon_threshold_sec=30)
    )
    seed_run_and_lane(crashed_reducer)
    crashed = crashed_reducer.snapshot_run(
        "run-1",
        now=BASE + timedelta(seconds=40),
        session_alive={"session-1": False},
    )
    assert crashed["state"] == "crashed"


def test_two_waves_in_one_session_each_get_distinct_run_closed() -> None:
    """Two orchestration waves in one session produce two distinct run_closed
    events, each referencing the corresponding run_id; each run resolves to state=closed."""
    reducer = Reducer(ReducerConfig(gap_timeout_sec=0))
    rows = [
        # Wave A opens and closes via its own SessionEnd invocation
        event(
            "00000000-0000-4000-8000-0000000000a0",
            "run_started",
            ingest_seq=1,
            run_id="session-1:1000",
            agent_id=None,
            source="hook",
            source_instance_id="hook:session-1:UserPromptSubmit:wave-a",
            source_seq=1,
        ),
        event(
            "00000000-0000-4000-8000-0000000000a1",
            "run_closed",
            ingest_seq=2,
            run_id="session-1:1000",
            agent_id=None,
            source="hook",
            source_instance_id="hook:session-1:SessionEnd:wave-a",
            source_seq=1,
            offset=1,
        ),
        # Wave B: distinct run_id in the same session, distinct SessionEnd invocation
        event(
            "00000000-0000-4000-8000-0000000000b0",
            "run_started",
            ingest_seq=3,
            run_id="session-1:2000",
            agent_id=None,
            source="hook",
            source_instance_id="hook:session-1:UserPromptSubmit:wave-b",
            source_seq=1,
            offset=2,
        ),
        event(
            "00000000-0000-4000-8000-0000000000b1",
            "run_closed",
            ingest_seq=4,
            run_id="session-1:2000",
            agent_id=None,
            source="hook",
            source_instance_id="hook:session-1:SessionEnd:wave-b",
            source_seq=1,
            offset=3,
        ),
    ]
    reducer.apply_rows(rows, now=BASE + timedelta(seconds=3))

    snap_a = reducer.snapshot_run("session-1:1000", now=BASE + timedelta(seconds=3))
    snap_b = reducer.snapshot_run("session-1:2000", now=BASE + timedelta(seconds=3))

    # Two distinct run_ids, each closed, each with its own closed_ts.
    assert snap_a["run_id"] != snap_b["run_id"]
    assert snap_a["state"] == "closed" and snap_a["closed_ts"] is not None
    assert snap_b["state"] == "closed" and snap_b["closed_ts"] is not None

    # Two distinct run_closed events, each referencing the corresponding run_id,
    # carrying distinct SessionEnd invocation UUIDs (distinct source_instance_id).
    closed = [json.loads(r.payload_json) for r in rows if r.event_type == "run_closed"]
    assert {c["run_id"] for c in closed} == {"session-1:1000", "session-1:2000"}
    assert {c["source_instance_id"] for c in closed} == {
        "hook:session-1:SessionEnd:wave-a",
        "hook:session-1:SessionEnd:wave-b",
    }
    assert len({c["event_id"] for c in closed}) == 2


def test_replay_from_checkpoint_reproduces_identical_state(tmp_path) -> None:
    store = LivenessStore(tmp_path / "events.db")
    store.initialize()
    rows = [
        event(
            "00000000-0000-4000-8000-000000000016",
            "run_started",
            ingest_seq=1,
            agent_id=None,
            source="hook",
            source_instance_id="hook:session-1:UserPromptSubmit:one",
            source_seq=1,
        ),
        event(
            "00000000-0000-4000-8000-000000000017",
            "lane_started",
            ingest_seq=2,
            source="hook",
            source_instance_id="hook:session-1:SubagentStart:one",
            source_seq=1,
        ),
        event(
            "00000000-0000-4000-8000-000000000018",
            "lane_stopped",
            ingest_seq=3,
            source="hook",
            source_instance_id="hook:session-1:SubagentStop:one",
            source_seq=1,
            declared_outcome="success",
            stop_reason="normal_stop",
        ),
    ]
    for item in rows:
        store.insert_event(json.loads(item.payload_json))

    reducer = Reducer()
    reducer.rebuild_from_store(store)
    before = reducer.snapshot_run("run-1", now=BASE)

    restarted = Reducer()
    restarted.rebuild_from_store(store)
    after = restarted.snapshot_run("run-1", now=BASE)

    assert store.get_checkpoint("liveness-reducer") == 3
    assert after == before


def _seed_three_terminal_events(store: LivenessStore) -> None:
    rows = [
        event(
            "00000000-0000-4000-8000-0000000000a1",
            "run_started",
            ingest_seq=1,
            agent_id=None,
            source="hook",
            source_instance_id="hook:session-1:UserPromptSubmit:one",
            source_seq=1,
        ),
        event(
            "00000000-0000-4000-8000-0000000000a2",
            "lane_started",
            ingest_seq=2,
            source="hook",
            source_instance_id="hook:session-1:SubagentStart:one",
            source_seq=1,
        ),
        event(
            "00000000-0000-4000-8000-0000000000a3",
            "lane_stopped",
            ingest_seq=3,
            source="hook",
            source_instance_id="hook:session-1:SubagentStop:one",
            source_seq=1,
            declared_outcome="success",
            stop_reason="normal_stop",
        ),
    ]
    for item in rows:
        store.insert_event(json.loads(item.payload_json))


def test_prune_never_drops_events_at_or_above_checkpoint(tmp_path) -> None:
    """invariant: the WAL is NEVER pruned below the reducer checkpoint.

    With the checkpoint at ingest_seq=2 (reducer applied through seq 2 but not 3),
    only seq 1 is eligible. The event AT the checkpoint (seq 2) and everything after
    (seq 3) must survive so a restart can deterministically replay from the checkpoint.
    """
    store = LivenessStore(tmp_path / "events.db")
    store.initialize()
    _seed_three_terminal_events(store)

    # Reducer has durably applied through seq 2 only (not yet seq 3).
    store.set_checkpoint("liveness-reducer", 2)

    floor, deleted = store.prune_below_checkpoint("liveness-reducer")
    assert floor == 2
    assert deleted == 1  # only seq 1 removed
    remaining = [r.ingest_seq for r in store.fetch_after(0, 100)]
    assert remaining == [2, 3]  # checkpoint event AND everything after retained
    assert store.min_ingest_seq() == 2  # never pruned below the checkpoint floor


def test_prune_floor_cannot_exceed_checkpoint_even_if_caller_asks(tmp_path) -> None:
    """retain_through_ingest_seq can only LOWER the floor, never raise it above the
    checkpoint. Asking to retain through a seq past the checkpoint must not delete the
    checkpoint or anything after it."""
    store = LivenessStore(tmp_path / "events.db")
    store.initialize()
    _seed_three_terminal_events(store)
    store.set_checkpoint("liveness-reducer", 2)

    # Caller (wrongly) asks to retain only through seq 999 -> would delete seq 1,2 if honored.
    floor, deleted = store.prune_below_checkpoint("liveness-reducer", retain_through_ingest_seq=999)
    assert floor == 2  # clamped to the checkpoint, NOT 999
    assert deleted == 1
    assert store.min_ingest_seq() == 2  # checkpoint event still present


def test_prune_with_no_checkpoint_deletes_nothing(tmp_path) -> None:
    """If the reducer has no durable high-water mark (checkpoint 0), there is no safe
    floor, so prune must delete nothing rather than guess."""
    store = LivenessStore(tmp_path / "events.db")
    store.initialize()
    _seed_three_terminal_events(store)
    # no set_checkpoint call -> get_checkpoint returns 0

    floor, deleted = store.prune_below_checkpoint("liveness-reducer")
    assert (floor, deleted) == (0, 0)
    assert [r.ingest_seq for r in store.fetch_after(0, 100)] == [1, 2, 3]


def test_replay_after_prune_reproduces_identical_state(tmp_path) -> None:
    """After pruning below the checkpoint, rebuild_from_store must still reproduce the
    same projected state for the surviving (post-checkpoint) events — proving the floor
    keeps replay deterministic."""
    store = LivenessStore(tmp_path / "events.db")
    store.initialize()
    _seed_three_terminal_events(store)

    full = Reducer()
    full.rebuild_from_store(store)  # applies all three, sets checkpoint to 3
    before = full.snapshot_run("run-1", now=BASE)
    assert store.get_checkpoint("liveness-reducer") == 3

    # Checkpoint is now 3; prune everything strictly below 3.
    floor, deleted = store.prune_below_checkpoint("liveness-reducer")
    assert floor == 3
    assert deleted == 2  # seq 1 and 2 archived/pruned
    assert store.min_ingest_seq() == 3  # the terminal lane_stopped (seq 3) survives

    restarted = Reducer()
    restarted.rebuild_from_store(store)
    after = restarted.snapshot_run("run-1", now=BASE)
    assert after["state"] == before["state"]  # terminal state preserved across prune
    assert after["lanes"][0]["state"] == before["lanes"][0]["state"]


def test_control_plane_metrics_render_and_stay_low_cardinality() -> None:
    from liveness.metrics import prometheus_text

    reducer = Reducer(ReducerConfig(gap_timeout_sec=30))
    seed_run_and_lane(reducer)

    # Open a gap on the shim source so the reducer buffers (producer-backlog proxy).
    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-000000000031",
                "tool_heartbeat",
                ingest_seq=3,
                source="shim",
                source_instance_id="shim:tool-1",
                source_seq=5,  # gap: expected 1, got 5 → buffered
                offset=1,
            )
        ],
        now=BASE + timedelta(seconds=1),
    )

    snap = reducer.metrics_snapshot(now=BASE + timedelta(seconds=1), max_ingest_seq=9, wal_size_bytes=4096)
    # last_ingest_seq_applied tracked the contiguously-applied seqs (1,2); seq 3 is buffered.
    assert snap.last_ingest_seq_applied == 2
    assert snap.max_ingest_seq == 9
    assert snap.reducer_lag_events == 7
    assert snap.wal_size_bytes == 4096
    # gap buffer holds one shim event → producer-backlog proxy bucketed by source kind.
    assert snap.source_backlog_events.get("shim") == 1

    text = prometheus_text(snap)
    assert "liveness_ingest_requests_total" in text
    assert "liveness_reducer_last_ingest_seq_applied 2" in text
    assert "liveness_reducer_lag_events 7" in text
    assert "liveness_wal_size_bytes 4096" in text
    assert 'liveness_source_backlog_events{source_kind="shim"} 1' in text
    # Privacy / low-cardinality: never leak the raw source_instance_id or agent_id.
    assert "tool-1" not in text
    assert "agent_id" not in text
    assert "shim:tool" not in text


def test_record_ingest_counts_by_result() -> None:
    from liveness.metrics import prometheus_text

    reducer = Reducer()
    reducer.record_ingest("accepted")
    reducer.record_ingest("accepted")
    reducer.record_ingest("error")
    snap = reducer.metrics_snapshot(now=BASE)
    assert snap.ingest_requests_total["accepted"] == 2
    assert snap.ingest_requests_total["error"] == 1
    text = prometheus_text(snap)
    assert 'liveness_ingest_requests_total{result="accepted"} 2' in text
    assert 'liveness_ingest_requests_total{result="error"} 1' in text


def test_watermark_skips_rolled_back_autoincrement_holes(tmp_path) -> None:
    """A hole in the ingest_seq sequence (rolled-back AUTOINCREMENT slot) must not stall the
    global replay watermark — it should advance past confirmed-missing seqs."""
    store = LivenessStore(tmp_path / "events.db")
    store.initialize()
    # Apply contiguous seqs 1..3, then 5..6 — seq 4 is a permanent hole (never committed).
    store.insert_event  # touch for clarity
    reducer = Reducer(ReducerConfig(gap_timeout_sec=0))
    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-0000000000a1",
                "run_started",
                ingest_seq=1,
                agent_id=None,
                source="hook",
                source_instance_id="hook:s:UserPromptSubmit:a",
                source_seq=1,
            ),
            event(
                "00000000-0000-4000-8000-0000000000a2",
                "lane_started",
                ingest_seq=2,
                source="hook",
                source_instance_id="hook:s:SubagentStart:a",
                source_seq=1,
            ),
            event("00000000-0000-4000-8000-0000000000a3", "tool_started", ingest_seq=3, source_seq=1, offset=1),
            event(
                "00000000-0000-4000-8000-0000000000a5",
                "tool_completed",
                ingest_seq=5,
                source_seq=2,
                exit_code=0,
                offset=2,
            ),
        ],
        now=BASE + timedelta(seconds=2),
    )
    # Without skip, watermark stalls at 3 waiting for seq 4 which never exists.
    assert reducer.last_ingest_seq_applied == 3
    # Tell the reducer seq 4 is a confirmed hole; watermark should jump to 5.
    reducer.mark_seqs_missing({4})
    assert reducer.last_ingest_seq_applied == 5
    snap = reducer.metrics_snapshot(now=BASE + timedelta(seconds=2), max_ingest_seq=5)
    assert snap.reducer_lag_events == 0


def test_store_missing_ingest_seqs_finds_holes(tmp_path) -> None:
    from liveness.schema import validate_event
    from liveness.config import LivenessConfig

    store = LivenessStore(tmp_path / "events.db")
    store.initialize()
    cfg = LivenessConfig(db_path=tmp_path / "events.db", ingest_token="x")
    # Insert two events (seq 1, 2). A duplicate event_id is IGNORED but burns an AUTOINCREMENT
    # slot in SQLite, producing a hole in the ingest sequence.
    import json as _json

    def mkraw(eid, sseq):
        return _json.loads(
            _json.dumps(
                {
                    "schema_version": 1,
                    "event_id": eid,
                    "event_type": "run_started",
                    "ts": ts(0),
                    "run_id": "r",
                    "session_id": "s",
                    "orchestrator": "claude-code",
                    "source": "hook",
                    "source_instance_id": f"hook:s:UserPromptSubmit:{eid}",
                    "source_seq": sseq,
                    "ext": {},
                }
            )
        )

    e1 = validate_event(mkraw("00000000-0000-4000-8000-0000000000b1", 1), cfg, BASE)
    store.insert_event(e1)
    # No holes yet.
    assert store.missing_ingest_seqs() == set()


def test_stale_lane_in_abandoned_run_is_reaped_not_counted_stale() -> None:
    """An orphan lane (lane_started, never lane_stopped) whose run goes silent past the abandon
    window must be REAPED to a terminal state — dropping out of subagent_lanes_stale (which grew
    unbounded before) and into subagent_lanes_abandoned_total instead."""
    reducer = Reducer(ReducerConfig(stale_threshold_sec=10, stuck_threshold_sec=20, run_abandon_threshold_sec=30))
    seed_run_and_lane(reducer)  # run_started + lane_started at BASE; no lane_stopped → orphan

    # Within the window: lane is genuinely stuck and the run is still open ⇒ counts as stale.
    live = reducer.metrics_snapshot(now=BASE + timedelta(seconds=25))
    assert live.lanes_stale.get(("openai", "claude-code")) == 1
    assert live.lanes_abandoned_total.get(("openai", "claude-code")) is None

    # Past the abandon window: run abandons, lane is reaped → off the stale gauge, onto abandoned.
    reaped = reducer.metrics_snapshot(now=BASE + timedelta(seconds=40))
    assert reaped.lanes_stale.get(("openai", "claude-code")) is None
    assert reaped.lanes_abandoned_total.get(("openai", "claude-code")) == 1
    # And the reap is durable: the lane's base_state is now terminal.
    lane = reducer.snapshot_run("run-1", now=BASE + timedelta(seconds=40))["lanes"][0]
    assert lane["base_state"] == "abandoned"
    assert lane["state"] == "abandoned"


def test_run_closed_reaps_orphan_lane() -> None:
    """A lane still non-terminal at a clean run_closed never got a lane_stopped — reap it so a
    closed run carries no phantom running/stale lane."""
    reducer = Reducer(ReducerConfig(gap_timeout_sec=0))
    seed_run_and_lane(reducer)  # lane_started, still running
    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-0000000000c1",
                "run_closed",
                ingest_seq=3,
                agent_id=None,
                source="hook",
                source_instance_id="hook:session-1:SessionEnd:one",
                source_seq=1,
                offset=1,
            )
        ],
        now=BASE + timedelta(seconds=1),
    )
    snap = reducer.snapshot_run("run-1", now=BASE + timedelta(seconds=1))
    assert snap["state"] == "closed"
    assert snap["lanes"][0]["base_state"] == "abandoned"
    # A cleanly-closed run never contributes to the live stale gauge.
    metrics = reducer.metrics_snapshot(now=BASE + timedelta(seconds=1))
    assert metrics.lanes_stale == {}


def test_cleanly_stopped_lane_survives_run_abandonment() -> None:
    """Reaping must touch ONLY non-terminal lanes: a lane that stopped cleanly keeps its declared
    outcome even after its run is later abandoned (terminal never regresses)."""
    reducer = Reducer(ReducerConfig(stale_threshold_sec=10, stuck_threshold_sec=20, run_abandon_threshold_sec=30))
    seed_run_and_lane(reducer)
    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-0000000000d1",
                "lane_stopped",
                ingest_seq=3,
                source="hook",
                source_instance_id="hook:session-1:SubagentStop:one",
                source_seq=1,
                declared_outcome="success",
                stop_reason="normal_stop",
                offset=1,
            )
        ],
        now=BASE + timedelta(seconds=1),
    )
    snap = reducer.snapshot_run("run-1", now=BASE + timedelta(seconds=40))
    assert snap["state"] == "abandoned"  # run with no further events abandons
    assert snap["lanes"][0]["base_state"] == "done"  # but the cleanly-done lane is untouched
    assert snap["lanes"][0]["declared_outcome"] == "success"


def test_snapshot_lanes_current_flattens_open_run_lanes() -> None:
    """/lanes/current: flat per-agent view across open runs, with token/cost totals; closed runs excluded."""
    reducer = Reducer(ReducerConfig(gap_timeout_sec=0))
    seed_run_and_lane(reducer)  # run-1 open, lane agent-1 running
    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-0000000000e1",
                "token_delta",
                ingest_seq=3,
                source_seq=1,
                tokens_delta={"in": 100, "out": 40},
            )
        ],
        now=BASE,
    )
    snap = reducer.snapshot_lanes_current(now=BASE)
    assert len(snap["lanes"]) == 1
    lane = snap["lanes"][0]
    assert lane["agent_id"] == "agent-1"
    assert lane["state"] == "running"
    assert lane["tokens_in"] == 100 and lane["tokens_out"] == 40
    assert "project" in lane and lane["run_state"] == "open"
    # Parent-orchestrator identity for dashboard grouping.
    assert lane["session_id"] == "session-1"
    assert "session-" in lane["orchestrator_label"]

    # A closed run's lanes drop out of the live view.
    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-0000000000e2",
                "run_closed",
                ingest_seq=4,
                agent_id=None,
                source="hook",
                source_instance_id="hook:session-1:SessionEnd:one",
                source_seq=1,
                offset=1,
            )
        ],
        now=BASE + timedelta(seconds=1),
    )
    assert reducer.snapshot_lanes_current(now=BASE + timedelta(seconds=1))["lanes"] == []


def test_heartbeating_lane_with_no_progress_projects_stalled() -> None:
    """A lane that keeps heartbeating but emits no real progress past the stall window is 'stalled'
    (the zombie case the silence-based stale/stuck detector misses, since heartbeats keep it fresh)."""
    reducer = Reducer(
        ReducerConfig(gap_timeout_sec=0, stale_threshold_sec=60, stuck_threshold_sec=300, stall_threshold_sec=600)
    )
    seed_run_and_lane(reducer)  # run_started + lane_started at BASE (sets last_progress_ts=BASE)
    reducer.apply_rows(
        [event("00000000-0000-4000-8000-0000000000f1", "tool_started", ingest_seq=3, source_seq=1, offset=1)],
        now=BASE + timedelta(seconds=1),
    )
    # Heartbeats keep last_activity fresh out to t=900s, but NO token_delta/activity (no progress).
    seq = 2
    for t in range(30, 901, 30):
        reducer.apply_rows(
            [
                event(
                    f"00000000-0000-4000-8000-0000000{1000 + t:04d}",
                    "tool_heartbeat",
                    ingest_seq=3 + seq,
                    source_seq=seq,
                    offset=t,
                )
            ],
            now=BASE + timedelta(seconds=t),
        )
        seq += 1
    now = BASE + timedelta(seconds=900)
    lane = reducer.snapshot_run("run-1", now=now)["lanes"][0]
    # Not stale/stuck (heartbeat at t=900 keeps last_activity fresh), but progress (t=1) is 899s old > 600.
    assert lane["last_activity_ts"] == ts(900)
    assert lane["state"] == "stalled"
    assert lane["base_state"] == "running"
    metrics = reducer.metrics_snapshot(now=now)
    assert sum(metrics.lanes_stalled.values()) == 1
    assert sum(metrics.lanes_stale.values()) == 0  # not counted as stale

    # A token_delta (real progress) clears the stall.
    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-0000000000f9",
                "token_delta",
                ingest_seq=999,
                source_seq=seq,
                offset=901,
                tokens_delta={"in": 5, "out": 2},
            )
        ],
        now=BASE + timedelta(seconds=901),
    )
    lane2 = reducer.snapshot_run("run-1", now=BASE + timedelta(seconds=905))["lanes"][0]
    assert lane2["state"] == "running"


def test_lanes_current_idle_seconds_tracks_progress_not_heartbeat() -> None:
    """idle_seconds on /lanes/current is age of last_PROGRESS_ts, not last_activity_ts: a hung child that
    keeps heartbeating must NOT show a small (green) idle while its state reads 'stalled' (orange). The
    idle column has to agree with the problem state, so it measures time since real work."""
    reducer = Reducer(
        ReducerConfig(gap_timeout_sec=0, stale_threshold_sec=60, stuck_threshold_sec=300, stall_threshold_sec=600)
    )
    seed_run_and_lane(reducer)  # run-1 open, lane agent-1, last_progress_ts=BASE
    reducer.apply_rows(
        [event("00000000-0000-4000-8000-00000000ab01", "tool_started", ingest_seq=3, source_seq=1, offset=1)],
        now=BASE + timedelta(seconds=1),
    )
    seq = 2
    for t in range(30, 901, 30):  # heartbeats only — last_activity fresh to t=900, last_progress frozen at t=1
        reducer.apply_rows(
            [
                event(
                    f"00000000-0000-4000-8000-0000000{2000 + t:04d}",
                    "tool_heartbeat",
                    ingest_seq=3 + seq,
                    source_seq=seq,
                    offset=t,
                )
            ],
            now=BASE + timedelta(seconds=t),
        )
        seq += 1
    lane = reducer.snapshot_lanes_current(now=BASE + timedelta(seconds=900))["lanes"][0]
    assert lane["state"] == "stalled"
    # idle reflects progress age (~899s, red), NOT the fresh heartbeat activity (~0s, green).
    assert lane["idle_seconds"] >= 600
    assert lane["idle_seconds"] == 899

    # A real progress event (token_delta) resets idle to ~0 even though heartbeats were identical.
    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-00000000ab02",
                "token_delta",
                ingest_seq=9000,
                source_seq=seq,
                offset=901,
                tokens_delta={"in": 3, "out": 1},
            )
        ],
        now=BASE + timedelta(seconds=901),
    )
    lane2 = reducer.snapshot_lanes_current(now=BASE + timedelta(seconds=902))["lanes"][0]
    assert lane2["idle_seconds"] == 1


def test_lanes_current_idle_seconds_never_negative_on_clock_skew() -> None:
    """A producer clock skewed ahead (ts validation tolerates up to the future window) would make
    server_now - last_progress_ts negative; idle_seconds must clamp to 0, not render a green '-Ns'."""
    reducer = Reducer(ReducerConfig(gap_timeout_sec=0))
    seed_run_and_lane(reducer)  # last_progress_ts = BASE
    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-00000000ac01",
                "token_delta",
                ingest_seq=3,
                source_seq=1,
                offset=120,
                tokens_delta={"in": 1, "out": 1},
            )
        ],  # event ts = BASE+120 (producer clock ahead)
        now=BASE + timedelta(seconds=120),
    )
    # Read the snapshot as if the server clock (BASE+60) is BEHIND the progress timestamp (BASE+120).
    lane = reducer.snapshot_lanes_current(now=BASE + timedelta(seconds=60))["lanes"][0]
    assert lane["idle_seconds"] == 0  # clamped, not -60


def test_failed_and_abandoned_are_real_monotonic_counters() -> None:
    """failed/abandoned counters increment ONCE on transition and are not recomputed from current
    lane state each scrape (so increase() over a window is accurate)."""
    reducer = Reducer(ReducerConfig(gap_timeout_sec=0))
    seed_run_and_lane(reducer)  # run-1, lane agent-1 running
    reducer.apply_rows(
        [
            event(
                "00000000-0000-4000-8000-0000000000fa",
                "lane_stopped",
                ingest_seq=3,
                source="hook",
                source_instance_id="hook:session-1:SubagentStop:one",
                source_seq=1,
                declared_outcome="failure",
                stop_reason="normal_stop",
            )
        ],
        now=BASE,
    )
    assert sum(reducer.metrics_snapshot(now=BASE).lanes_failed_total.values()) == 1
    # second scrape does NOT re-count (real counter, not state recompute)
    assert sum(reducer.metrics_snapshot(now=BASE).lanes_failed_total.values()) == 1

    # abandoned counter increments once when a silent run is reaped
    r2 = Reducer(
        ReducerConfig(stale_threshold_sec=10, stuck_threshold_sec=20, run_abandon_threshold_sec=30, gap_timeout_sec=0)
    )
    seed_run_and_lane(r2)  # lane running, last_activity = BASE
    r2.metrics_snapshot(now=BASE + timedelta(seconds=40))  # triggers abandon + reap
    assert sum(r2.metrics_snapshot(now=BASE + timedelta(seconds=41)).lanes_abandoned_total.values()) == 1
