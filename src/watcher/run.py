"""Governed watcher — main polling loop.

Entry point: ``python -m watcher.run``

Loop behaviour
--------------
Every WATCHER_POLL_INTERVAL seconds (default 15 s):

1. Fetch GET /runs/current.
2. For each open run, fetch GET /runs/{run_id}/lanes to get fresh lane state
   (the /runs/current payload already embeds lanes, but the per-run endpoint
   is the authoritative detail view and is fetched to stay consistent with
   the event contract).
3. Pass each run through the Analyzer to detect:
   - Correlated stalls (multiple lanes idle together)
   - Individual lane stalls, failures, quarantines, telemetry degradation
4. For closed runs that were previously open, produce an end-of-run summary.
5. For each finding, run it through the GovernanceEngine:
   - Record the decision in the AuditLog regardless of outcome.
   - If ALLOWED: execute the action (currently only Telegram alerts; always
     a no-op in dry-run mode).
   - Otherwise: log the governance block and move on.

Governance scaffold is fully wired.  In dry-run mode (the default) no
actions are dispatched and every decision is recorded in the audit log with
``governance_outcome=dry_run_only``.

To enable governed auto-reaction later:
  1. Set WATCHER_DRY_RUN=false
  2. Add action names to WATCHER_ALLOWED_ACTIONS (comma-separated)
  3. Configure WATCHER_TELEGRAM_TOKEN + WATCHER_TELEGRAM_CHAT_ID
  4. Review destination permissions and notification policy before live use.
"""

from __future__ import annotations

import logging
import signal
import sys
import time
from typing import Dict, Optional, Set

from .analyzer import Analyzer, LaneAnomaly, RunSummary, StallGroup
from .api_client import ApiError, SnapshotApiClient
from .audit import AuditLog
from .config import WatcherConfig
from .governance import GovernanceDecision, GovernanceEngine
from .models import Run
from .telegram import format_run_summary, send_telegram

logger = logging.getLogger(__name__)


class WatcherLoop:
    """The main polling loop."""

    def __init__(self, config: WatcherConfig) -> None:
        self._config = config
        self._client = SnapshotApiClient(config)
        self._analyzer = Analyzer(config)
        self._governance = GovernanceEngine(config=config)
        self._audit = AuditLog(config.audit_log_path)

        # Track which run_ids were open last poll so we can detect closures
        self._previously_open: Set[str] = set()
        # Cache the last-seen snapshot_version to detect staleness
        self._last_snapshot_version: Optional[int] = None

        self._running = False

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        """Run the poll loop until interrupted."""
        self._running = True
        logger.info(
            "Watcher started. dry_run=%s, poll_interval=%ss, api=%s",
            self._config.dry_run,
            self._config.poll_interval_seconds,
            self._config.api_base,
        )
        if self._config.dry_run:
            logger.info(
                "DRY-RUN MODE ACTIVE — no actions will be dispatched. "
                "Set WATCHER_DRY_RUN=false to enable governed auto-reaction."
            )
        try:
            while self._running:
                self._tick()
                time.sleep(self._config.poll_interval_seconds)
        except KeyboardInterrupt:
            logger.info("Watcher interrupted by user.")
        finally:
            self._audit.close()
            logger.info("Watcher stopped.")

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------ #
    # Poll tick
    # ------------------------------------------------------------------ #

    def _tick(self) -> None:
        now = time.monotonic()
        try:
            snapshot = self._client.fetch_current()
        except ApiError as exc:
            logger.warning("Snapshot API unavailable (watcher is blind this tick): %s", exc)
            return

        # Log if the snapshot version hasn't advanced (reducer may be stuck)
        if self._last_snapshot_version is not None and snapshot.snapshot_version == self._last_snapshot_version:
            logger.debug(
                "Snapshot version unchanged (%d); no new events.",
                snapshot.snapshot_version,
            )
        self._last_snapshot_version = snapshot.snapshot_version

        current_open: Set[str] = set()
        run_map: Dict[str, Run] = {}

        for run in snapshot.runs:
            run_map[run.run_id] = run
            if not run.is_closed:
                current_open.add(run.run_id)

        # Detect run closures (was open last tick, now closed / absent)
        just_closed = self._previously_open - current_open
        for run_id in just_closed:
            closed_run = run_map.get(run_id)
            if closed_run is not None:
                self._handle_run_closed(closed_run)
            else:
                logger.info("Run %r disappeared from snapshot (assumed closed).", run_id)
            self._governance.reset_run(run_id)

        # Analyze each currently-open run
        for run in snapshot.runs:
            if run.is_closed:
                continue
            # Fetch per-run lane detail for authoritative fresh state
            try:
                fresh_lanes = self._client.fetch_lanes(run.run_id)
                # Merge fresh lanes into the run object for analysis
                if fresh_lanes:
                    run.lanes = fresh_lanes
            except ApiError as exc:
                logger.warning(
                    "Could not fetch lanes for run %r: %s (using cached lanes)",
                    run.run_id,
                    exc,
                )

            stall_groups, anomalies = self._analyzer.analyze_run(run, now)
            for sg in stall_groups:
                self._handle_stall(run, sg)
            for anomaly in anomalies:
                self._handle_anomaly(run, anomaly)

        self._previously_open = current_open

    # ------------------------------------------------------------------ #
    # Event handlers
    # ------------------------------------------------------------------ #

    def _handle_stall(self, run: Run, sg: StallGroup) -> None:
        """Process a detected stall group."""
        action = "telegram_alert"
        decision = self._governance.check(
            action=action,
            incident_key=sg.incident_key,
            run_id=run.run_id,
            agent_id=sg.agent_ids[0] if len(sg.agent_ids) == 1 else None,
        )

        corr_label = "CORRELATED" if sg.is_correlated else "isolated"
        reasoning = (
            f"{corr_label} stall detected in run {run.run_id}: "
            f"agents={sg.agent_ids}, "
            f"max_idle={sg.max_stall_seconds:.0f}s, "
            f"classification={sg.classification}"
        )
        logger.log(
            logging.WARNING if sg.classification == "escalate" else logging.INFO,
            "%s",
            reasoning,
        )

        self._audit.record(
            incident_key=sg.incident_key,
            run_id=run.run_id,
            agent_id=sg.agent_ids[0] if len(sg.agent_ids) == 1 else None,
            decision_type=("correlated_stall" if sg.is_correlated else "isolated_stall"),
            reasoning=reasoning,
            dry_run=self._config.dry_run,
            governance_outcome=decision.value,
            extra={
                "agents": sg.agent_ids,
                "max_stall_seconds": sg.max_stall_seconds,
                "classification": sg.classification,
                "quorum_met": sg.is_correlated,
            },
        )

        if decision == GovernanceDecision.ALLOWED:
            msg = _build_stall_message(run, sg)
            send_telegram(msg, config=self._config)
            self._governance.record_action(
                incident_key=sg.incident_key,
                run_id=run.run_id,
                agent_id=sg.agent_ids[0] if len(sg.agent_ids) == 1 else None,
            )
        elif decision != GovernanceDecision.DRY_RUN_ONLY:
            logger.debug(
                "Stall alert for %r blocked by governance: %s",
                sg.incident_key,
                decision.value,
            )

    def _handle_anomaly(self, run: Run, anomaly: LaneAnomaly) -> None:
        """Process a lane-level anomaly."""
        action = "telegram_alert"
        decision = self._governance.check(
            action=action,
            incident_key=anomaly.incident_key,
            run_id=run.run_id,
            agent_id=anomaly.lane.agent_id,
        )

        logger.warning(
            "Lane anomaly [%s] in run %r: %s",
            anomaly.anomaly_type,
            run.run_id,
            anomaly.description,
        )

        self._audit.record(
            incident_key=anomaly.incident_key,
            run_id=run.run_id,
            agent_id=anomaly.lane.agent_id,
            decision_type=anomaly.anomaly_type,
            reasoning=anomaly.description,
            dry_run=self._config.dry_run,
            governance_outcome=decision.value,
            extra={
                "lane_state": anomaly.lane.state,
                "declared_outcome": anomaly.lane.declared_outcome,
                "quarantine": anomaly.lane.quarantine,
            },
        )

        if decision == GovernanceDecision.ALLOWED:
            msg = _build_anomaly_message(run, anomaly)
            send_telegram(msg, config=self._config)
            self._governance.record_action(
                incident_key=anomaly.incident_key,
                run_id=run.run_id,
                agent_id=anomaly.lane.agent_id,
            )
        elif decision != GovernanceDecision.DRY_RUN_ONLY:
            logger.debug(
                "Anomaly alert for %r blocked by governance: %s",
                anomaly.incident_key,
                decision.value,
            )

    def _handle_run_closed(self, run: Run) -> None:
        """Produce and dispatch an end-of-run summary."""
        summary: RunSummary = self._analyzer.produce_run_summary(run)
        incident_key = summary.incident_key

        action = "telegram_alert"
        decision = self._governance.check(
            action=action,
            incident_key=incident_key,
            run_id=run.run_id,
        )

        outcome_label = "FAILURES" if summary.had_failures else "QUARANTINES" if summary.had_quarantines else "OK"
        reasoning = (
            f"End-of-run summary for {run.run_id}: state={run.state}, "
            f"outcome={outcome_label}, cost=${summary.total_cost_usd:.4f}, "
            f"lanes={len(run.lanes)}"
        )
        logger.info("%s", reasoning)

        self._audit.record(
            incident_key=incident_key,
            run_id=run.run_id,
            agent_id=None,
            decision_type="end_of_run_summary",
            reasoning=reasoning,
            dry_run=self._config.dry_run,
            governance_outcome=decision.value,
            extra={
                "state": run.state,
                "had_failures": summary.had_failures,
                "had_quarantines": summary.had_quarantines,
                "total_cost_usd": summary.total_cost_usd,
                "lane_count": len(run.lanes),
            },
        )

        if decision in (GovernanceDecision.ALLOWED, GovernanceDecision.DRY_RUN_ONLY):
            # End-of-run summaries are always sent (or dry-run logged);
            # they are informational, not interventional.
            tg_msg = format_run_summary(
                run_id=summary.run_id,
                project=summary.project,
                state=summary.state,
                lane_summaries=summary.lane_summaries,
                total_cost_usd=summary.total_cost_usd,
                dry_run=self._config.dry_run,
                config=self._config,
            )
            send_telegram(tg_msg, config=self._config)
            if decision == GovernanceDecision.ALLOWED:
                self._governance.record_action(
                    incident_key=incident_key,
                    run_id=run.run_id,
                )
        else:
            logger.debug(
                "End-of-run summary for %r blocked by governance: %s",
                run.run_id,
                decision.value,
            )


# ------------------------------------------------------------------ #
# Message builders (no raw event content in shell calls — text only)
# ------------------------------------------------------------------ #


def _build_stall_message(run: Run, sg: StallGroup) -> str:
    from .telegram import escape_markdown_v2

    corr = "Correlated stall" if sg.is_correlated else "Stall"
    cls_label = sg.classification.upper()
    agents = ", ".join(f"`{escape_markdown_v2(a)}`" for a in sg.agent_ids)
    return (
        f"*{escape_markdown_v2(corr)}* \\[{escape_markdown_v2(cls_label)}\\]\n"
        f"run\\_id: `{escape_markdown_v2(run.run_id)}`\n"
        f"project: {escape_markdown_v2(run.project or 'unknown')}\n"
        f"agents: {agents}\n"
        f"idle: {sg.max_stall_seconds:.0f}s"
    )


def _build_anomaly_message(run: Run, anomaly: LaneAnomaly) -> str:
    from .telegram import escape_markdown_v2

    return (
        f"*Lane anomaly* \\[{escape_markdown_v2(anomaly.anomaly_type)}\\]\n"
        f"run\\_id: `{escape_markdown_v2(run.run_id)}`\n"
        f"agent: `{escape_markdown_v2(anomaly.lane.agent_id)}`\n"
        f"detail: {escape_markdown_v2(anomaly.description[:300])}"
    )


# ------------------------------------------------------------------ #
# Entry point
# ------------------------------------------------------------------ #


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        stream=sys.stderr,
    )

    config = WatcherConfig()
    loop = WatcherLoop(config)

    def _handle_sigterm(_signum, _frame):
        logger.info("Received SIGTERM, stopping watcher.")
        loop.stop()

    signal.signal(signal.SIGTERM, _handle_sigterm)

    loop.start()


if __name__ == "__main__":
    main()
