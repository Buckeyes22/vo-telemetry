"""Typed data models for the snapshot API response shapes.

Keeps parsing isolated so the rest of the watcher works with typed objects
rather than raw dicts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ToolRun:
    tool_run_id: str
    state: str
    started_ts: Optional[str]
    completed_ts: Optional[str]
    failed_ts: Optional[str]
    last_activity_ts: Optional[str]
    exit_code: Optional[int]
    error: Optional[str]
    model: Optional[str]
    provider: Optional[str]
    tokens_in: int
    tokens_out: int
    cost_usd_total: float

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ToolRun":
        return cls(
            tool_run_id=d.get("tool_run_id", ""),
            state=d.get("state", "unknown"),
            started_ts=d.get("started_ts"),
            completed_ts=d.get("completed_ts"),
            failed_ts=d.get("failed_ts"),
            last_activity_ts=d.get("last_activity_ts"),
            exit_code=d.get("exit_code"),
            error=d.get("error"),
            model=d.get("model"),
            provider=d.get("provider"),
            tokens_in=int(d.get("tokens_in", 0)),
            tokens_out=int(d.get("tokens_out", 0)),
            cost_usd_total=float(d.get("cost_usd_total", 0.0)),
        )


@dataclass
class Lane:
    run_id: str
    agent_id: str
    agent_type: Optional[str]
    orchestrator: Optional[str]
    provider: Optional[str]
    model: Optional[str]
    state: str
    base_state: str
    started_ts: Optional[str]
    stopped_ts: Optional[str]
    last_activity_ts: Optional[str]
    declared_outcome: Optional[str]
    summary: Optional[str]
    final_message_preview: Optional[str]
    stop_reason: Optional[str]
    quarantine: bool
    telemetry_degraded: bool
    identity_error: Optional[str]
    failed_child_count: int
    dropped_events_total: int
    changed_version: int
    branch: Optional[str]
    worktree: Optional[str]
    tools: Dict[str, ToolRun] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Lane":
        tools: Dict[str, ToolRun] = {}
        for k, v in (d.get("tools") or {}).items():
            if isinstance(v, dict):
                tools[k] = ToolRun.from_dict(v)
        return cls(
            run_id=d.get("run_id", ""),
            agent_id=d.get("agent_id", ""),
            agent_type=d.get("agent_type"),
            orchestrator=d.get("orchestrator"),
            provider=d.get("provider"),
            model=d.get("model"),
            state=d.get("state", "unknown"),
            base_state=d.get("base_state", "unknown"),
            started_ts=d.get("started_ts"),
            stopped_ts=d.get("stopped_ts"),
            last_activity_ts=d.get("last_activity_ts"),
            declared_outcome=d.get("declared_outcome"),
            summary=d.get("summary"),
            final_message_preview=d.get("final_message_preview"),
            stop_reason=d.get("stop_reason"),
            quarantine=bool(d.get("quarantine", False)),
            telemetry_degraded=bool(d.get("telemetry_degraded", False)),
            identity_error=d.get("identity_error"),
            failed_child_count=int(d.get("failed_child_count", 0)),
            dropped_events_total=int(d.get("dropped_events_total", 0)),
            changed_version=int(d.get("changed_version", 0)),
            branch=d.get("branch"),
            worktree=d.get("worktree"),
            tools=tools,
        )

    @property
    def is_terminal(self) -> bool:
        return self.state in {"done", "failed", "abandoned", "crashed"}

    @property
    def is_active(self) -> bool:
        return not self.is_terminal


@dataclass
class Run:
    run_id: str
    state: str
    started_ts: Optional[str]
    closed_ts: Optional[str]
    last_event_ts: Optional[str]
    orchestrator: Optional[str]
    project: Optional[str]
    operator_label: Optional[str]
    session_id: Optional[str]
    cwd: Optional[str]
    snapshot_version: int
    last_ingest_seq_applied: int
    server_time: Optional[str]
    lanes: List[Lane] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Run":
        lanes = [Lane.from_dict(lane) for lane in (d.get("lanes") or [])]
        return cls(
            run_id=d.get("run_id", ""),
            state=d.get("state", "unknown"),
            started_ts=d.get("started_ts"),
            closed_ts=d.get("closed_ts"),
            last_event_ts=d.get("last_event_ts"),
            orchestrator=d.get("orchestrator"),
            project=d.get("project"),
            operator_label=d.get("operator_label"),
            session_id=d.get("session_id"),
            cwd=d.get("cwd"),
            snapshot_version=int(d.get("snapshot_version", 0)),
            last_ingest_seq_applied=int(d.get("last_ingest_seq_applied", 0)),
            server_time=d.get("server_time"),
            lanes=lanes,
        )

    @property
    def is_closed(self) -> bool:
        return self.state in {"closed", "abandoned", "crashed"}

    @property
    def active_lanes(self) -> List[Lane]:
        return [la for la in self.lanes if la.is_active]


@dataclass
class CurrentSnapshot:
    """Top-level response from GET /runs/current."""

    snapshot_version: int
    last_ingest_seq_applied: int
    server_time: Optional[str]
    run_id: Optional[str]
    runs: List[Run] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "CurrentSnapshot":
        runs = [Run.from_dict(r) for r in (d.get("runs") or [])]
        return cls(
            snapshot_version=int(d.get("snapshot_version", 0)),
            last_ingest_seq_applied=int(d.get("last_ingest_seq_applied", 0)),
            server_time=d.get("server_time"),
            run_id=d.get("run_id"),
            runs=runs,
        )
