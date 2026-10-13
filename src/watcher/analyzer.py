"""Watcher analysis engine — the judgment layer a dashboard cannot provide.

This module applies higher-order reasoning to the snapshot data:

1. **Correlated stall detection** — identify when a quorum of active lanes all
   go stale within the same time window, suggesting a systemic cause rather
   than an individual agent hang.

2. **Stall classification** — decide whether a stall is *transient* (likely
   self-recovering; log and wait) or *escalate* (requires operator attention;
   send alert).

3. **Lane failure classification** — distinguish clean terminal states from
   unexpected failures or quarantined lanes.

4. **End-of-run summary production** — when a run closes, consume each lane's
   ``declared_outcome`` and ``summary``, aggregate cost, and produce the
   Telegram summary payload.

All methods are PURE with respect to side-effects — they return analysis
results; they do NOT send notifications, execute actions, or write audit
entries.  The calling loop in ``run.py`` is responsible for acting on the
results through the governance engine.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .config import WatcherConfig
from .models import Lane, Run

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
# Result types
# ------------------------------------------------------------------ #


@dataclass
class StallGroup:
    """A set of lanes that stalled together (correlated stall)."""

    lanes: List[Lane]
    # How long the oldest lane has been stalled (seconds)
    max_stall_seconds: float
    # True if this looks like a systemic stall (quorum met)
    is_correlated: bool
    classification: str  # "transient" | "escalate"
    incident_key: str

    @property
    def agent_ids(self) -> List[str]:
        return [la.agent_id for la in self.lanes]


@dataclass
class LaneAnomaly:
    """A single-lane anomaly: failure, quarantine, or identity error."""

    lane: Lane
    anomaly_type: str  # "failed" | "quarantine" | "identity_error" | "telemetry_degraded"
    description: str
    incident_key: str


@dataclass
class RunSummary:
    """End-of-run analysis produced when a run closes."""

    run_id: str
    project: Optional[str]
    state: str
    total_cost_usd: float
    lane_summaries: List[Dict]
    had_failures: bool
    had_quarantines: bool
    incident_key: str


# ------------------------------------------------------------------ #
# Analyzer
# ------------------------------------------------------------------ #


class Analyzer:
    """Stateful analyzer tracking per-run stall onset times."""

    def __init__(self, config: WatcherConfig) -> None:
        self._config = config
        # run_id → {agent_id → monotonic time of first stall detection}
        self._stall_onset: Dict[str, Dict[str, float]] = {}

    # ------------------------------------------------------------------ #
    # Main entry points
    # ------------------------------------------------------------------ #

    def analyze_run(self, run: Run, now_monotonic: float) -> Tuple[List[StallGroup], List[LaneAnomaly]]:
        """Analyze a single run snapshot.

        Returns (stall_groups, lane_anomalies).
        Both lists are empty if nothing notable is detected.
        """
        stall_groups = self._detect_stalls(run, now_monotonic)
        anomalies = self._detect_lane_anomalies(run)
        return stall_groups, anomalies

    def produce_run_summary(self, run: Run) -> RunSummary:
        """Produce an end-of-run summary for a closed run."""
        total_cost = 0.0
        lane_summaries = []
        had_failures = False
        had_quarantines = False

        for lane in run.lanes:
            cost = sum(t.cost_usd_total for t in lane.tools.values())
            total_cost += cost
            if lane.declared_outcome in {"failure", "error", "crashed"}:
                had_failures = True
            if lane.quarantine:
                had_quarantines = True

            lane_summaries.append(
                {
                    "agent_id": lane.agent_id,
                    "declared_outcome": lane.declared_outcome or "unknown",
                    # summary is redacted by the telegram formatter; pass raw
                    "summary": lane.summary,
                    "failed_children": lane.failed_child_count,
                    "cost_usd": cost,
                    "quarantine": lane.quarantine,
                }
            )

        # Clean up stall tracking state for this run
        self._stall_onset.pop(run.run_id, None)

        return RunSummary(
            run_id=run.run_id,
            project=run.project,
            state=run.state,
            total_cost_usd=total_cost,
            lane_summaries=lane_summaries,
            had_failures=had_failures,
            had_quarantines=had_quarantines,
            incident_key=f"end_of_run:{run.run_id}",
        )

    # ------------------------------------------------------------------ #
    # Stall detection
    # ------------------------------------------------------------------ #

    def _detect_stalls(self, run: Run, now_monotonic: float) -> List[StallGroup]:
        """Find lanes that have exceeded the stall threshold."""
        cfg = self._config
        active = run.active_lanes
        if not active:
            return []

        stalled: List[Lane] = []
        stall_seconds: Dict[str, float] = {}
        onset_map = self._stall_onset.setdefault(run.run_id, {})

        server_now = _parse_iso(run.server_time)

        for lane in active:
            last_ts = _parse_iso(lane.last_activity_ts)
            if last_ts is None or server_now is None:
                continue
            idle = (server_now - last_ts).total_seconds()
            if idle >= cfg.lane_stall_threshold_seconds:
                stalled.append(lane)
                stall_seconds[lane.agent_id] = idle
                # Record first detection time using monotonic clock
                if lane.agent_id not in onset_map:
                    onset_map[lane.agent_id] = now_monotonic

        if not stalled:
            return []

        # Determine correlation: do we have a quorum of active lanes stalled?
        fraction = len(stalled) / len(active)
        is_correlated = fraction >= cfg.correlated_stall_quorum

        max_stall = max(stall_seconds.values())
        classification = "transient" if max_stall < cfg.transient_stall_max_seconds else "escalate"

        if is_correlated:
            group_key = f"correlated_stall:{run.run_id}:{_lane_key(stalled)}"
            return [
                StallGroup(
                    lanes=stalled,
                    max_stall_seconds=max_stall,
                    is_correlated=True,
                    classification=classification,
                    incident_key=group_key,
                )
            ]

        # Non-correlated: one StallGroup per stalled lane
        groups = []
        for lane in stalled:
            groups.append(
                StallGroup(
                    lanes=[lane],
                    max_stall_seconds=stall_seconds[lane.agent_id],
                    is_correlated=False,
                    classification=classification,
                    incident_key=f"stall:{run.run_id}:{lane.agent_id}",
                )
            )
        return groups

    # ------------------------------------------------------------------ #
    # Lane anomaly detection
    # ------------------------------------------------------------------ #

    def _detect_lane_anomalies(self, run: Run) -> List[LaneAnomaly]:
        anomalies: List[LaneAnomaly] = []
        for lane in run.lanes:
            # Quarantine lanes always surface
            if lane.quarantine:
                anomalies.append(
                    LaneAnomaly(
                        lane=lane,
                        anomaly_type="quarantine",
                        description=(
                            f"Lane {lane.agent_id} is quarantined"
                            + (f": {lane.identity_error}" if lane.identity_error else "")
                        ),
                        incident_key=f"quarantine:{run.run_id}:{lane.agent_id}",
                    )
                )

            # Terminal failure lanes
            elif lane.state in {"failed", "crashed", "abandoned"}:
                anomalies.append(
                    LaneAnomaly(
                        lane=lane,
                        anomaly_type="failed",
                        description=(
                            f"Lane {lane.agent_id} terminal state={lane.state}"
                            f", declared_outcome={lane.declared_outcome or 'none'}"
                            f", failed_children={lane.failed_child_count}"
                        ),
                        incident_key=f"lane_failed:{run.run_id}:{lane.agent_id}",
                    )
                )

            # Telemetry degraded (dropped events)
            if lane.telemetry_degraded:
                anomalies.append(
                    LaneAnomaly(
                        lane=lane,
                        anomaly_type="telemetry_degraded",
                        description=(
                            f"Lane {lane.agent_id} has dropped events"
                            f" (dropped_events_total={lane.dropped_events_total})"
                        ),
                        incident_key=(f"telem_degraded:{run.run_id}:{lane.agent_id}"),
                    )
                )

            # Identity error without quarantine
            if lane.identity_error and not lane.quarantine:
                anomalies.append(
                    LaneAnomaly(
                        lane=lane,
                        anomaly_type="identity_error",
                        description=(
                            f"Lane {lane.agent_id} identity error: "
                            # identity_error is metadata, not prompt/response content
                            f"{lane.identity_error}"
                        ),
                        incident_key=(f"identity_error:{run.run_id}:{lane.agent_id}"),
                    )
                )

        return anomalies


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        # Python 3.7+ handles Z suffix in fromisoformat only from 3.11;
        # normalise it manually.
        normalised = ts.replace("Z", "+00:00")
        return datetime.fromisoformat(normalised)
    except (ValueError, AttributeError):
        return None


def _lane_key(lanes: List[Lane]) -> str:
    """Stable key for a set of lanes, for use in incident keys."""
    return "+".join(sorted(la.agent_id for la in lanes))
