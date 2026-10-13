from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from .config import LivenessConfig
from .models import EventRow
from .schema import to_rfc3339


RUN_TERMINAL = {"closed", "abandoned", "crashed"}
# done/done_with_warnings/failed come from a real lane_stopped (declared outcome). abandoned/crashed
# are reducer-derived terminals for orphan lanes (lane_started, never lane_stopped) whose owning run
# closed, was abandoned (silence past the window), or crashed. All terminal ⇒ never regress.
LANE_TERMINAL = {"done", "done_with_warnings", "failed", "abandoned", "crashed"}
TOOL_TERMINAL = {"completed", "failed"}
ACTIVE_LANE_STATES = {"running", "running_with_failed_child"}
REDUCER_NAME = "liveness-reducer"


@dataclass(frozen=True)
class ReducerConfig:
    stale_threshold_sec: float = 60.0
    stuck_threshold_sec: float = 300.0
    gap_timeout_sec: float = 30.0
    run_abandon_threshold_sec: float = 3600.0
    # A running lane that keeps heartbeating but emits no real progress (tokens/activity/tool events)
    # for this long is projected "stalled" — catches hung/zombie children the silence-based stale/stuck
    # detector misses (the supervisor heartbeat keeps last_activity fresh). Default 10 min.
    stall_threshold_sec: float = 600.0

    @classmethod
    def from_liveness_config(cls, config: LivenessConfig) -> "ReducerConfig":
        return cls(
            stale_threshold_sec=config.stale_threshold_sec,
            stuck_threshold_sec=config.stuck_threshold_sec,
            gap_timeout_sec=config.gap_timeout_sec,
            run_abandon_threshold_sec=config.run_abandon_threshold_sec,
            stall_threshold_sec=config.stall_threshold_sec,
        )


@dataclass
class ToolState:
    tool_run_id: str
    state: str = "running"
    started_ts: str | None = None
    last_activity_ts: str | None = None
    completed_ts: str | None = None
    failed_ts: str | None = None
    exit_code: int | None = None
    error: Any = None
    provider: str = "unknown"
    model: str = "unknown"
    tokens_in: float = 0
    tokens_out: float = 0
    cost_usd_total: float = 0


@dataclass
class LaneState:
    run_id: str
    agent_id: str
    state: str = "running"
    changed_version: int = 0
    started_ts: str | None = None
    stopped_ts: str | None = None
    last_activity_ts: str | None = None
    # last_activity_ts is bumped by EVERY event including tool_heartbeat (supervisor keepalive), so a
    # hung child still looks "fresh". last_progress_ts is bumped only by REAL work (tool_started/
    # _activity/token_delta/_completed/_failed, lane_started) — never by heartbeat — so the reducer can
    # tell "heartbeating but making no progress" (a stalled/zombie lane) from a genuinely-active one.
    last_progress_ts: str | None = None
    orchestrator: str = "unknown"
    provider: str = "unknown"
    agent_type: str | None = None
    model: str | None = None
    worktree: str | None = None
    branch: str | None = None
    quarantine: bool = False
    identity_error: str | None = None
    failed_child_count: int = 0
    telemetry_degraded: bool = False
    dropped_events_total: int = 0
    declared_outcome: str | None = None
    stop_reason: str | None = None
    summary: str | None = None
    final_message_preview: str | None = None
    final_message_sha256: str | None = None
    tools: dict[str, ToolState] = field(default_factory=dict)


@dataclass
class RunState:
    run_id: str
    session_id: str
    state: str = "open"
    changed_version: int = 0
    started_ts: str | None = None
    closed_ts: str | None = None
    last_event_ts: str | None = None
    orchestrator: str = "unknown"
    prompt_hash: str | None = None
    cwd: str | None = None
    project: str | None = None
    operator_label: str | None = None
    lanes: dict[str, LaneState] = field(default_factory=dict)


@dataclass
class MetricsSnapshot:
    lanes_running: dict[tuple[str, str], int] = field(default_factory=dict)
    lanes_stale: dict[tuple[str, str], int] = field(default_factory=dict)
    # Heartbeating but no real progress past the stall window — hung/zombie lanes.
    lanes_stalled: dict[tuple[str, str], int] = field(default_factory=dict)
    lanes_failed_total: dict[tuple[str, str], int] = field(default_factory=dict)
    # Lanes reaped to a terminal abandoned/crashed state (orphans: lane_started, no lane_stopped).
    # Surfaced separately so reaped orphans are VISIBLE instead of silently inflating
    # the actionable lanes_stale gauge — which they did before runs were gated/reaped.
    lanes_abandoned_total: dict[tuple[str, str], int] = field(default_factory=dict)
    tokens: dict[tuple[str, str, str], float] = field(default_factory=dict)
    cost: dict[tuple[str, str], float] = field(default_factory=dict)
    # Control-plane health for the liveness plane. All reducer-derived, never producer-emitted.
    ingest_requests_total: dict[str, int] = field(default_factory=dict)  # keyed by result: accepted|duplicate|error
    last_ingest_seq_applied: int = 0
    max_ingest_seq: int = 0
    reducer_lag_events: int = 0  # max_ingest_seq - last_ingest_seq_applied
    wal_size_bytes: int = 0
    # Reducer-visible per-source backlog: events received but not yet contiguously applied
    # (gap buffer depth). Server-side proxy for producer outbox pressure. Keyed by source_kind
    # (hook|shim|tailer|reducer) — low-cardinality, never the raw source_instance_id.
    source_backlog_events: dict[str, int] = field(default_factory=dict)
    # Producer-reported loss visibility. Reducer accumulates dropped_count from
    # producer_dropped_events facts, keyed by (provider, orchestrator) — low-cardinality.
    # Loss is visible, never silent; a non-zero rate here means an outbox shed non-terminal events.
    producer_dropped_events_total: dict[tuple[str, str], int] = field(default_factory=dict)


class Reducer:
    def __init__(self, config: ReducerConfig | None = None):
        self.config = config or ReducerConfig()
        self.runs: dict[str, RunState] = {}
        self.processed_event_ids: set[str] = set()
        self.source_next_seq: dict[str, int] = {}
        self.source_buffers: dict[str, dict[int, EventRow]] = {}
        self.source_gap_since: dict[str, datetime] = {}
        self.snapshot_version = 0
        self.last_ingest_seq_applied = 0
        self.max_ingest_seq = 0  # highest ingest_seq the reducer has seen (applied or buffered)
        self._applied_ingest_seqs: set[int] = set()
        # ingest_seqs the WAL proves never existed (rolled-back AUTOINCREMENT slots). The global
        # replay watermark must skip these or it stalls forever waiting on a seq that was never
        # committed.
        self._known_missing_seqs: set[int] = set()
        self.initialized = False
        self.tokens: dict[tuple[str, str, str], float] = {}
        self.cost: dict[tuple[str, str], float] = {}
        # High-water cost per tool_run_id — cost_usd_total is an absolute total, so we add only the
        # positive increment to the cost counter (prevents double-counting re-reported cost).
        self._tool_cost_seen: dict[str, float] = {}
        # Real monotonic terminal counters, keyed (provider, orchestrator). Incremented ONCE on the
        # transition to failed (lane_stopped) / abandoned|crashed (reap) — NOT recomputed from current
        # lane state each snapshot — so increase() over a window is accurate (replay re-derives them).
        # Known limitations (low-probability timing/restart-window jitter; removing them needs a durable
        # counter checkpoint and tick-based reaping, since partial fixes would break counter monotonicity):
        #   1. Not durably checkpointed: a WAL prune below the reducer checkpoint + restart can replay a
        #      partial counter, so increase() around a restart can briefly mislead.
        #   2. Reap-at-read: _project_run_state() (called from metrics_snapshot/snapshot_current) can
        #      terminalize+increment, so the abandoned-counter timestamp is observer-dependent. Mitigated
        #      in practice by the steady ~15s /metrics scrape driving reaping on a regular cadence.
        #   3. Ordering: run_closed reaps orphan lanes to abandoned immediately; a late lane_stopped(failure)
        #      arriving after run_closed is then ignored (terminal-never-regress), so abandoned++ not failed++.
        self.lanes_failed_total: dict[tuple[str, str], int] = {}
        self.lanes_abandoned_total: dict[tuple[str, str], int] = {}
        # Producer-reported loss totals, keyed by (provider, orchestrator). Reducer-derived
        # rollup of producer_dropped_events facts so the control-plane dashboard can surface loss.
        self.producer_dropped_events_total: dict[tuple[str, str], int] = {}
        # Control-plane ingest counters: request rate / errors, keyed by result.
        self.ingest_requests_total: dict[str, int] = {"accepted": 0, "duplicate": 0, "error": 0}
        self._lock = threading.RLock()

    def record_ingest(self, result: str) -> None:
        """Count an ingest attempt by result (accepted|duplicate|error). Reducer-derived."""
        with self._lock:
            self.ingest_requests_total[result] = self.ingest_requests_total.get(result, 0) + 1

    def apply_rows(self, rows: list[EventRow], now: datetime | None = None) -> None:
        with self._lock:
            apply_now = now or datetime.now(UTC)
            for row in sorted(rows, key=lambda item: item.ingest_seq):
                self._apply_or_buffer(row, apply_now)
                if self.config.gap_timeout_sec <= 0:
                    self.flush_expired_gaps(apply_now)
            self.initialized = True

    def flush_expired_gaps(self, now: datetime | None = None) -> None:
        with self._lock:
            check_now = now or datetime.now(UTC)
            expired_sources = [
                source
                for source, started in self.source_gap_since.items()
                if check_now - started >= timedelta(seconds=self.config.gap_timeout_sec)
            ]
            for source in expired_sources:
                buffer = self.source_buffers.get(source, {})
                for seq in sorted(buffer):
                    self._apply_event(buffer[seq], late=True)
                    self.source_next_seq[source] = max(self.source_next_seq.get(source, 1), seq + 1)
                buffer.clear()
                self.source_gap_since.pop(source, None)

    def rebuild_from_store(self, store: Any) -> None:
        with self._lock:
            Reducer.__init__(self, self.config)
            since = 0
            while True:
                rows = store.fetch_after(since, 1000)
                if not rows:
                    break
                self.apply_rows(rows)
                since = rows[-1].ingest_seq
            self.flush_expired_gaps(datetime.now(UTC) + timedelta(seconds=self.config.gap_timeout_sec + 1))
            # Skip rolled-back AUTOINCREMENT holes so the replay watermark does not stall.
            missing = getattr(store, "missing_ingest_seqs", None)
            if callable(missing):
                self.mark_seqs_missing(missing())
            store.set_checkpoint(REDUCER_NAME, self.last_ingest_seq_applied)
            self.initialized = True

    def snapshot_current(self, now: datetime | None = None) -> dict[str, Any]:
        with self._lock:
            server_now = now or datetime.now(UTC)
            runs = [
                self._run_to_dict(run, server_now, None)
                for run in self.runs.values()
                if self._project_run_state(run, server_now, None) == "open"
            ]
            return {
                "run_id": None,
                "runs": runs,
                "snapshot_version": self.snapshot_version,
                "last_ingest_seq_applied": self.last_ingest_seq_applied,
                "server_time": to_rfc3339(server_now),
            }

    def snapshot_lanes_current(self, now: datetime | None = None) -> dict[str, Any]:
        """Flat list of every lane across all OPEN runs — the per-agent 'who is doing what right now'
        view a dashboard swimlane needs (the metric rollups only give counts). Each lane carries its
        run's project/state plus token/cost totals summed across its tools."""
        with self._lock:
            server_now = now or datetime.now(UTC)
            lanes: list[dict[str, Any]] = []
            for run in self.runs.values():
                if self._project_run_state(run, server_now, None) != "open":
                    continue
                for lane in run.lanes.values():
                    # "Currently live" excludes cleanly-finished/cleaned-up lanes — those clutter the
                    # live roster. Keep active + attention states (running/stale/stuck/stalled/failed/crashed).
                    if self._project_lane_state(lane, server_now) in {"done", "done_with_warnings", "abandoned"}:
                        continue
                    item = self._lane_to_dict(lane, server_now)
                    item["project"] = run.project  # the worktree/cwd this agent runs in
                    item["session_id"] = run.session_id
                    item["run_state"] = "open"
                    # Precise idleness in seconds (vague "a few seconds ago" relative time isn't useful on
                    # a liveness board) — the dashboard renders it with color thresholds. Measured from
                    # last_PROGRESS_ts (real work), NOT last_activity_ts: tool_heartbeat keeps last_activity
                    # fresh on a hung child, so an activity-based idle would show a stalled zombie as "fresh"
                    # (green) while its state column reads "stalled" (orange) — a self-contradicting row.
                    # Progress-age makes the idle column agree with all three problem states (stale/stuck/
                    # stalled) and never false-alarms healthy agents, since token_delta/tool_activity bump
                    # progress continuously (only the pure keepalive heartbeat does not).
                    # max(0, …): a producer clock skewed ahead (ts validation allows up to
                    # timestamp_future_window_sec into the future) would otherwise yield a negative idle,
                    # which the dashboard's (None→GREEN) threshold renders as a nonsensical fresh/green "-Ns".
                    lp = parse_optional_ts(lane.last_progress_ts)
                    item["idle_seconds"] = max(0, round((server_now - lp).total_seconds())) if lp else None
                    # The PARENT orchestrator is the Claude Code SESSION, not the worktree — one session
                    # fanning out across N worktrees must group as ONE parent. Label by short session id.
                    sess = (run.session_id or "")[:12]  # 12 hex chars — ample margin against prefix collisions
                    item["orchestrator_label"] = f"session {sess}" if sess else "session ?"
                    tools = item.get("tools", {})
                    item["tokens_in"] = sum(float(t.get("tokens_in", 0) or 0) for t in tools.values())
                    item["tokens_out"] = sum(float(t.get("tokens_out", 0) or 0) for t in tools.values())
                    item["cost_usd_total"] = sum(float(t.get("cost_usd_total", 0) or 0) for t in tools.values())
                    lanes.append(item)
            return {
                "lanes": lanes,
                "snapshot_version": self.snapshot_version,
                "last_ingest_seq_applied": self.last_ingest_seq_applied,
                "server_time": to_rfc3339(server_now),
            }

    def snapshot_run(
        self,
        run_id: str,
        now: datetime | None = None,
        session_alive: dict[str, bool] | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            server_now = now or datetime.now(UTC)
            run = self.runs[run_id]
            return self._run_to_dict(run, server_now, session_alive)

    def snapshot_lanes_since(
        self, run_id: str, since_snapshot_version: int, now: datetime | None = None
    ) -> dict[str, Any]:
        with self._lock:
            server_now = now or datetime.now(UTC)
            run = self.runs[run_id]
            lanes = [
                self._lane_to_dict(lane, server_now)
                for lane in run.lanes.values()
                if lane.changed_version > since_snapshot_version
            ]
            return {
                "run_id": run_id,
                "lanes": lanes,
                "snapshot_version": self.snapshot_version,
                "last_ingest_seq_applied": self.last_ingest_seq_applied,
                "server_time": to_rfc3339(server_now),
            }

    def metrics_snapshot(
        self,
        now: datetime | None = None,
        max_ingest_seq: int | None = None,
        wal_size_bytes: int = 0,
    ) -> MetricsSnapshot:
        with self._lock:
            server_now = now or datetime.now(UTC)
            snapshot = MetricsSnapshot(tokens=dict(self.tokens), cost=dict(self.cost))
            for run in self.runs.values():
                # Project the run first: this also TERMINALIZES + reaps lanes of runs that have gone
                # silent past the abandon window, so a periodic /metrics scrape doubles as the reaper.
                run_open = self._project_run_state(run, server_now, None) not in RUN_TERMINAL
                for lane in run.lanes.values():
                    key = (lane.provider or "unknown", lane.orchestrator or "unknown")
                    projected = self._project_lane_state(lane, server_now)
                    # running/stale describe lanes in LIVE runs only. A lane in a closed/abandoned/
                    # crashed run is history, not an actionable "stuck" lane — counting those made
                    # subagent_lanes_stale grow without bound as orphan runs accumulated.
                    if run_open and projected in {"running", "running_with_failed_child"}:
                        snapshot.lanes_running[key] = snapshot.lanes_running.get(key, 0) + 1
                    if run_open and projected in {"stale", "stuck"}:
                        snapshot.lanes_stale[key] = snapshot.lanes_stale.get(key, 0) + 1
                    if run_open and projected == "stalled":
                        snapshot.lanes_stalled[key] = snapshot.lanes_stalled.get(key, 0) + 1

            # failed/abandoned are REAL monotonic counters (incremented once on transition), copied
            # straight through — not recomputed from current lane state (which would make increase() lie).
            snapshot.lanes_failed_total = dict(self.lanes_failed_total)
            snapshot.lanes_abandoned_total = dict(self.lanes_abandoned_total)
            # Control-plane health for the liveness plane — all reducer-derived.
            snapshot.ingest_requests_total = dict(self.ingest_requests_total)
            snapshot.last_ingest_seq_applied = self.last_ingest_seq_applied
            high_water = self.max_ingest_seq if max_ingest_seq is None else max_ingest_seq
            snapshot.max_ingest_seq = high_water
            snapshot.reducer_lag_events = max(0, high_water - self.last_ingest_seq_applied)
            snapshot.wal_size_bytes = wal_size_bytes
            # Per-source gap-buffer depth → producer backlog proxy, bucketed by source kind.
            backlog: dict[str, int] = {}
            for source, buffer in self.source_buffers.items():
                if not buffer:
                    continue
                kind = source.split(":", 1)[0] if ":" in source else source
                backlog[kind] = backlog.get(kind, 0) + len(buffer)
            snapshot.source_backlog_events = backlog
            snapshot.producer_dropped_events_total = dict(self.producer_dropped_events_total)
            return snapshot

    def has_pending_gaps(self) -> bool:
        with self._lock:
            return any(bool(buffer) for buffer in self.source_buffers.values())

    def _apply_or_buffer(self, row: EventRow, now: datetime) -> None:
        self.max_ingest_seq = max(self.max_ingest_seq, row.ingest_seq)
        if row.event_id in self.processed_event_ids:
            self._mark_ingest_applied(row.ingest_seq)
            return
        source = row.source_instance_id
        expected = self.source_next_seq.get(source, 1)
        if row.source_seq == expected:
            self._apply_event(row, late=False)
            self.source_next_seq[source] = expected + 1
            self._flush_contiguous(source)
            return
        if row.source_seq > expected:
            self.source_buffers.setdefault(source, {})[row.source_seq] = row
            self.source_gap_since.setdefault(source, now)
            return
        self._apply_event(row, late=True)

    def _flush_contiguous(self, source: str) -> None:
        buffer = self.source_buffers.get(source)
        if not buffer:
            self.source_gap_since.pop(source, None)
            return
        expected = self.source_next_seq.get(source, 1)
        while expected in buffer:
            row = buffer.pop(expected)
            self._apply_event(row, late=False)
            expected += 1
            self.source_next_seq[source] = expected
        if not buffer:
            self.source_gap_since.pop(source, None)

    def _apply_event(self, row: EventRow, late: bool) -> None:
        if row.event_id in self.processed_event_ids:
            self._mark_ingest_applied(row.ingest_seq)
            return
        event = json.loads(row.payload_json)
        event_type = row.event_type
        run = self._ensure_run(event)
        event_time = str(event["ts"])
        if run.state not in RUN_TERMINAL:
            run.last_event_ts = _max_ts(run.last_event_ts, event_time)

        if event_type == "run_started":
            self._apply_run_started(run, event)
        elif event_type == "run_updated":
            self._touch_run(run, event)
        elif event_type == "run_closed":
            if run.state not in RUN_TERMINAL:
                run.state = "closed"
                run.closed_ts = event_time
                self._bump_run(run)
                # Lanes still non-terminal at a clean close never received a lane_stopped — orphans.
                # Reap them so they don't linger as stale/stuck (reducer-derived terminal).
                self._reap_active_lanes(run, "abandoned", event_time)
        elif event_type == "lane_started":
            lane = self._ensure_lane(run, event)
            if lane.state not in LANE_TERMINAL:
                lane.state = "running"
                lane.started_ts = lane.started_ts or event_time
                lane.last_activity_ts = _max_ts(lane.last_activity_ts, event_time)
                lane.last_progress_ts = _max_ts(lane.last_progress_ts, event_time)
                self._bump_lane(run, lane)
        elif event_type == "lane_stopped":
            lane = self._ensure_lane(run, event)
            lane.declared_outcome = event.get("declared_outcome", "unknown")
            lane.stop_reason = event.get("stop_reason")
            lane.summary = event.get("summary")
            lane.final_message_preview = event.get("final_message_preview")
            lane.final_message_sha256 = event.get("final_message_sha256")
            lane.stopped_ts = event_time
            if lane.state not in LANE_TERMINAL:
                lane.state = self._resolve_lane_stop(lane)
                if lane.state == "failed":
                    # Real monotonic counter, incremented once on the transition to failed (not
                    # recomputed from current state each scrape) so increase() over a window is accurate.
                    fk = (lane.provider or "unknown", lane.orchestrator or "unknown")
                    self.lanes_failed_total[fk] = self.lanes_failed_total.get(fk, 0) + 1
            self._bump_lane(run, lane)
        elif event_type in {
            "tool_started",
            "tool_heartbeat",
            "tool_activity",
            "token_delta",
            "tool_completed",
            "tool_failed",
        }:
            lane = self._ensure_lane(run, event)
            tool = self._ensure_tool(lane, event)
            self._apply_tool_event(lane, tool, event_type, event, event_time, late)
            self._apply_event_metrics(event)
            self._bump_lane(run, lane)
        elif event_type == "producer_dropped_events":
            lane = self._ensure_lane(run, event)
            lane.telemetry_degraded = True
            dropped = int(event.get("dropped_count", 0))
            lane.dropped_events_total += dropped
            key = (lane.provider or "unknown", lane.orchestrator or "unknown")
            self.producer_dropped_events_total[key] = self.producer_dropped_events_total.get(key, 0) + dropped
            self._bump_lane(run, lane)

        self.processed_event_ids.add(row.event_id)
        self._mark_ingest_applied(row.ingest_seq)

    def mark_seqs_missing(self, seqs: set[int]) -> None:
        """Record ingest_seqs the WAL proves are absent (rolled-back AUTOINCREMENT holes).

        Lets the global replay watermark advance past slots that were never committed, instead
        of stalling on a seq that will never arrive. Re-runs the contiguous advance afterward.
        """
        with self._lock:
            self._known_missing_seqs.update(seqs)
            self.max_ingest_seq = max(self.max_ingest_seq, max(seqs, default=0))
            self._advance_applied()

    def _mark_ingest_applied(self, ingest_seq: int) -> None:
        self._applied_ingest_seqs.add(ingest_seq)
        self._advance_applied()

    def _advance_applied(self) -> None:
        next_seq = self.last_ingest_seq_applied + 1
        # Advance over applied seqs and over confirmed holes (below the high-water mark) alike.
        while next_seq in self._applied_ingest_seqs or (
            next_seq in self._known_missing_seqs and next_seq <= self.max_ingest_seq
        ):
            self._applied_ingest_seqs.discard(next_seq)
            self.last_ingest_seq_applied = next_seq
            next_seq += 1

    def _apply_run_started(self, run: RunState, event: dict[str, Any]) -> None:
        if run.state in RUN_TERMINAL:
            return
        run.state = "open"
        run.started_ts = run.started_ts or str(event["ts"])
        run.orchestrator = str(event.get("orchestrator") or run.orchestrator or "unknown")
        run.prompt_hash = event.get("prompt_hash")
        run.cwd = event.get("cwd")
        run.project = event.get("project")
        run.operator_label = event.get("operator_label")
        self._bump_run(run)

    def _touch_run(self, run: RunState, event: dict[str, Any]) -> None:
        if run.state not in RUN_TERMINAL:
            run.orchestrator = str(event.get("orchestrator") or run.orchestrator or "unknown")
            self._bump_run(run)

    def _apply_tool_event(
        self,
        lane: LaneState,
        tool: ToolState,
        event_type: str,
        event: dict[str, Any],
        event_time: str,
        late: bool,
    ) -> None:
        lane.last_activity_ts = _max_ts(lane.last_activity_ts, event_time)
        # Heartbeat is keepalive, not progress — everything else here is real work.
        if event_type != "tool_heartbeat":
            lane.last_progress_ts = _max_ts(lane.last_progress_ts, event_time)
        if event_type == "tool_started":
            if tool.state not in TOOL_TERMINAL:
                tool.state = "running"
                tool.started_ts = tool.started_ts or event_time
                tool.last_activity_ts = _max_ts(tool.last_activity_ts, event_time)
            if lane.state not in LANE_TERMINAL and lane.failed_child_count == 0:
                lane.state = "running"
            return

        if event_type in {"tool_heartbeat", "tool_activity", "token_delta"}:
            if tool.state not in TOOL_TERMINAL:
                tool.last_activity_ts = _max_ts(tool.last_activity_ts, event_time)
            if event_type == "token_delta":
                delta = event.get("tokens_delta") or {}
                tool.tokens_in += float(delta.get("in", 0))
                tool.tokens_out += float(delta.get("out", 0))
            if lane.state not in LANE_TERMINAL and lane.failed_child_count > 0:
                lane.state = "running_with_failed_child"
            elif lane.state not in LANE_TERMINAL:
                lane.state = "running"
            return

        if event_type == "tool_completed":
            if tool.state not in TOOL_TERMINAL:
                tool.state = "completed"
                tool.completed_ts = event_time
                tool.exit_code = _optional_int(event.get("exit_code"))
            return

        if event_type == "tool_failed":
            was_failed = tool.state == "failed"
            if tool.state not in TOOL_TERMINAL:
                tool.state = "failed"
                tool.failed_ts = event_time
                tool.exit_code = _optional_int(event.get("exit_code"))
                tool.error = event.get("error")
            if not was_failed:
                lane.failed_child_count += 1
            if lane.state not in LANE_TERMINAL:
                lane.state = "running_with_failed_child"

    def _apply_event_metrics(self, event: dict[str, Any]) -> None:
        provider = str(event.get("provider") or "unknown")
        orchestrator = str(event.get("orchestrator") or "unknown")
        tokens_delta = event.get("tokens_delta")
        if isinstance(tokens_delta, dict):
            for direction in ("in", "out"):
                amount = float(tokens_delta.get(direction, 0))
                key = (provider, orchestrator, direction)
                self.tokens[key] = self.tokens.get(key, 0) + amount
        cost = event.get("cost_usd_total")
        if isinstance(cost, (int, float)):
            # cost_usd_total is an ABSOLUTE per-tool total (re-reported each event), not a delta.
            # Adding it raw double-counts when a provider reports cost more than once for a tool.
            # Track the high-water cost per tool_run_id and add only the positive increment.
            # Limitation: _tool_cost_seen is rebuilt from replay, not checkpointed. If the WAL is
            # pruned below the reducer checkpoint and the same tool re-reports an absolute cost after
            # a restart, that cost is added in full. A durable reducer snapshot would remove this.
            tool_run_id = str(event.get("tool_run_id") or "")
            prev = self._tool_cost_seen.get(tool_run_id, 0.0) if tool_run_id else 0.0
            delta = max(0.0, float(cost) - prev)
            if tool_run_id:
                self._tool_cost_seen[tool_run_id] = max(prev, float(cost))
            cost_key = (provider, orchestrator)
            self.cost[cost_key] = self.cost.get(cost_key, 0.0) + delta

    def _ensure_run(self, event: dict[str, Any]) -> RunState:
        run_id = str(event["run_id"])
        run = self.runs.get(run_id)
        if run is None:
            run = RunState(
                run_id=run_id,
                session_id=str(event.get("session_id") or ""),
                orchestrator=str(event.get("orchestrator") or "unknown"),
                last_event_ts=str(event.get("ts")),
            )
            self.runs[run_id] = run
        return run

    def _ensure_lane(self, run: RunState, event: dict[str, Any]) -> LaneState:
        agent_id = str(event.get("agent_id") or "quarantine:missing")
        lane = run.lanes.get(agent_id)
        if lane is None:
            lane = LaneState(run_id=run.run_id, agent_id=agent_id)
            run.lanes[agent_id] = lane
        lane.orchestrator = str(event.get("orchestrator") or lane.orchestrator or "unknown")
        lane.provider = str(event.get("provider") or lane.provider or "unknown")
        lane.agent_type = event.get("agent_type") or lane.agent_type
        lane.model = event.get("model") or lane.model
        lane.worktree = event.get("worktree") or lane.worktree
        lane.branch = event.get("branch") or lane.branch
        lane.quarantine = bool(event.get("quarantine", lane.quarantine))
        lane.identity_error = event.get("identity_error") or lane.identity_error
        return lane

    def _ensure_tool(self, lane: LaneState, event: dict[str, Any]) -> ToolState:
        tool_run_id = str(event.get("tool_run_id") or "unknown")
        tool = lane.tools.get(tool_run_id)
        if tool is None:
            tool = ToolState(tool_run_id=tool_run_id)
            lane.tools[tool_run_id] = tool
        tool.provider = str(event.get("provider") or tool.provider or "unknown")
        tool.model = str(event.get("model") or tool.model or "unknown")
        cost = event.get("cost_usd_total")
        if isinstance(cost, (int, float)):
            tool.cost_usd_total = float(cost)
        return tool

    def _resolve_lane_stop(self, lane: LaneState) -> str:
        outcome = lane.declared_outcome or "unknown"
        has_failed_child = lane.failed_child_count > 0 or any(tool.state == "failed" for tool in lane.tools.values())
        if outcome == "success":
            return "done_with_warnings" if has_failed_child else "done"
        if outcome == "failure":
            return "failed"
        if outcome == "partial":
            return "done_with_warnings"
        if has_failed_child:
            return "failed"
        return "done"

    def _bump_run(self, run: RunState) -> None:
        self.snapshot_version += 1
        run.changed_version = self.snapshot_version

    def _bump_lane(self, run: RunState, lane: LaneState) -> None:
        self.snapshot_version += 1
        run.changed_version = self.snapshot_version
        lane.changed_version = self.snapshot_version

    def _project_run_state(self, run: RunState, now: datetime, session_alive: dict[str, bool] | None) -> str:
        if run.state in RUN_TERMINAL:
            return run.state
        active_lanes = [lane for lane in run.lanes.values() if lane.state in ACTIVE_LANE_STATES]
        # A session known-dead with a still-stuck lane crashed rather than closed cleanly.
        if active_lanes and session_alive is not None and session_alive.get(run.session_id) is False:
            if any(self._project_lane_state(lane, now) == "stuck" for lane in active_lanes):
                self._terminate_run(run, "crashed", now)
                return run.state
        # No applied event for the WHOLE run past the abandon window ⇒ nothing in it is alive.
        # run.last_event_ts advances on every applied event, so a genuinely-active lane keeps it
        # fresh; crossing the window means every lane has gone silent. Abandon + reap orphan lanes.
        last_event = parse_optional_ts(run.last_event_ts)
        if last_event is not None and now - last_event > timedelta(seconds=self.config.run_abandon_threshold_sec):
            self._terminate_run(run, "abandoned", now)
            return run.state
        return run.state

    def _terminate_run(self, run: RunState, run_state: str, now: datetime) -> None:
        """Mark a run terminal (abandoned|crashed) and reap its still-open lanes to the matching
        lane-terminal state. Without this, an orphan lane (lane_started, no lane_stopped — parent
        killed before SubagentStop fired) projects as stale/stuck forever and the gauge only grows.
        Reducer-derived; producers still emit facts only."""
        run.state = run_state
        self._bump_run(run)
        self._reap_active_lanes(run, "crashed" if run_state == "crashed" else "abandoned", to_rfc3339(now))

    def _reap_active_lanes(self, run: RunState, lane_state: str, reaped_ts: str) -> None:
        for lane in run.lanes.values():
            if lane.state not in LANE_TERMINAL:
                lane.state = lane_state
                lane.stop_reason = lane.stop_reason or lane_state
                lane.stopped_ts = lane.stopped_ts or reaped_ts
                # Real monotonic counter, incremented once when a lane is reaped to abandoned/crashed.
                ak = (lane.provider or "unknown", lane.orchestrator or "unknown")
                self.lanes_abandoned_total[ak] = self.lanes_abandoned_total.get(ak, 0) + 1
                self._bump_lane(run, lane)

    def _project_lane_state(self, lane: LaneState, now: datetime) -> str:
        if lane.state in LANE_TERMINAL:
            return lane.state
        last_activity = parse_optional_ts(lane.last_activity_ts)
        if last_activity is not None:
            age = now - last_activity
            if age > timedelta(seconds=self.config.stuck_threshold_sec):
                return "stuck"
            if age > timedelta(seconds=self.config.stale_threshold_sec):
                return "stale"
        # Heartbeating (so not silent → not stale/stuck) but no REAL progress for the stall window:
        # a hung/zombie child. Caught here, where pure silence-detection can't see it.
        if lane.state in ACTIVE_LANE_STATES:
            last_progress = parse_optional_ts(lane.last_progress_ts)
            if last_progress is not None and now - last_progress > timedelta(seconds=self.config.stall_threshold_sec):
                return "stalled"
        return lane.state

    def _run_to_dict(self, run: RunState, now: datetime, session_alive: dict[str, bool] | None) -> dict[str, Any]:
        state = self._project_run_state(run, now, session_alive)
        return {
            "run_id": run.run_id,
            "session_id": run.session_id,
            "state": state,
            "orchestrator": run.orchestrator,
            "started_ts": run.started_ts,
            "closed_ts": run.closed_ts,
            "last_event_ts": run.last_event_ts,
            "prompt_hash": run.prompt_hash,
            "cwd": run.cwd,
            "project": run.project,
            "operator_label": run.operator_label,
            "lanes": [self._lane_to_dict(lane, now) for lane in run.lanes.values()],
            "snapshot_version": self.snapshot_version,
            "last_ingest_seq_applied": self.last_ingest_seq_applied,
            "server_time": to_rfc3339(now),
        }

    def _lane_to_dict(self, lane: LaneState, now: datetime) -> dict[str, Any]:
        return {
            "run_id": lane.run_id,
            "agent_id": lane.agent_id,
            "state": self._project_lane_state(lane, now),
            "base_state": lane.state,
            "changed_version": lane.changed_version,
            "started_ts": lane.started_ts,
            "stopped_ts": lane.stopped_ts,
            "last_activity_ts": lane.last_activity_ts,
            "orchestrator": lane.orchestrator,
            "provider": lane.provider,
            "agent_type": lane.agent_type,
            "model": lane.model,
            "worktree": lane.worktree,
            "branch": lane.branch,
            "quarantine": lane.quarantine,
            "identity_error": lane.identity_error,
            "failed_child_count": lane.failed_child_count,
            "telemetry_degraded": lane.telemetry_degraded,
            "dropped_events_total": lane.dropped_events_total,
            "declared_outcome": lane.declared_outcome,
            "stop_reason": lane.stop_reason,
            "summary": lane.summary,
            "final_message_preview": lane.final_message_preview,
            "final_message_sha256": lane.final_message_sha256,
            "tools": {tool_id: self._tool_to_dict(tool) for tool_id, tool in lane.tools.items()},
        }

    def _tool_to_dict(self, tool: ToolState) -> dict[str, Any]:
        return {
            "tool_run_id": tool.tool_run_id,
            "state": tool.state,
            "started_ts": tool.started_ts,
            "last_activity_ts": tool.last_activity_ts,
            "completed_ts": tool.completed_ts,
            "failed_ts": tool.failed_ts,
            "exit_code": tool.exit_code,
            "error": tool.error,
            "provider": tool.provider,
            "model": tool.model,
            "tokens_in": tool.tokens_in,
            "tokens_out": tool.tokens_out,
            "cost_usd_total": tool.cost_usd_total,
        }


def _parse_ts(value: str) -> datetime:
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    return datetime.fromisoformat(text).astimezone(UTC)


def parse_optional_ts(value: str | None) -> datetime | None:
    return None if value is None else _parse_ts(value)


def _max_ts(left: str | None, right: str) -> str:
    if left is None:
        return right
    return right if _parse_ts(right) > _parse_ts(left) else left


def _optional_int(value: Any) -> int | None:
    return int(value) if isinstance(value, int) else None
