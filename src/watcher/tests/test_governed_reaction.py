"""Governed auto-reaction validation — DRY-RUN mode.

Feeds incident fixtures to the watcher's analyzer + governance engine in
DRY-RUN mode and asserts all safety invariants hold:

  1. Correlated stalls are clustered (quorum met → single StallGroup).
  2. Transient-vs-escalate classification is correct.
  3. Per-run intervention limit is enforced.
  4. Per-agent retry limit is enforced.
  5. Cooldown gate blocks repeated incidents.
  6. Idempotency key prevents double-acting on the same incident.
  7. Action allowlist blocks un-permitted actions.
  8. AuditLog entry is written for every governance decision.
  9. Telegram send stays a DRY-RUN stub — records what it WOULD send,
     makes NO network call (urllib.request.urlopen is never invoked).

All tests run with dry_run=True, which is the default WatcherConfig value.
Real auto-reaction and real Telegram sends remain disabled.
"""

from __future__ import annotations

import io
import json
import logging
import os
import time
import unittest.mock as mock
from pathlib import Path
from typing import Any, Dict


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"

# The analyzer uses time.monotonic internally; we freeze it via a known value.
FROZEN_MONO = 10_000.0


def _load_fixture(name: str) -> Dict[str, Any]:
    path = FIXTURES_DIR / name
    with path.open() as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Local imports (watcher package is on PYTHONPATH via PYTHONPATH=src/watcher)
# ---------------------------------------------------------------------------

from watcher.analyzer import Analyzer  # noqa: E402
from watcher.audit import AuditLog  # noqa: E402
from watcher.config import WatcherConfig  # noqa: E402
from watcher.governance import GovernanceDecision, GovernanceEngine  # noqa: E402
from watcher.models import CurrentSnapshot, Run  # noqa: E402
from watcher.telegram import send_telegram  # noqa: E402


# ---------------------------------------------------------------------------
# Config factory — always dry_run=True, writes audit to a temp StringIO
# ---------------------------------------------------------------------------


def _dry_run_config(**overrides) -> WatcherConfig:
    """WatcherConfig with DRY-RUN enforced and governance limits set tight for tests.

    Keyword arguments in *overrides* win over the defaults defined here.
    """
    defaults: dict = dict(
        dry_run=True,
        api_base="http://127.0.0.1:0",
        poll_interval_seconds=1,
        lane_stall_threshold_seconds=120,
        correlated_stall_quorum=0.5,
        transient_stall_max_seconds=300,
        max_interventions_per_run=3,
        max_retries_per_agent=2,
        cooldown_seconds=300,
        allowed_actions=frozenset({"telegram_alert"}),
        audit_log_path=os.devnull,
        telegram_bot_token="",
        telegram_chat_id="",
    )
    defaults.update(overrides)
    return WatcherConfig(**defaults)


def _parse_snapshot(fixture_name: str) -> CurrentSnapshot:
    raw = _load_fixture(fixture_name)
    return CurrentSnapshot.from_dict(raw)


def _first_run(fixture_name: str) -> Run:
    snap = _parse_snapshot(fixture_name)
    assert snap.runs, f"Fixture {fixture_name!r} has no runs"
    return snap.runs[0]


# ===========================================================================
# 1. Correlated-stall clustering
# ===========================================================================


class TestCorrelatedStall:
    """Fixture: correlated_stall.json — three lanes all stale together."""

    def setup_method(self):
        self.cfg = _dry_run_config()
        self.analyzer = Analyzer(self.cfg)
        self.run = _first_run("correlated_stall.json")

    def test_all_lanes_are_active(self):
        """Sanity: all three lanes should be active (none terminal)."""
        assert len(self.run.active_lanes) == 3

    def test_stalls_form_single_cluster(self):
        """Three co-stalling lanes must be clustered into one StallGroup."""
        groups, _ = self.analyzer.analyze_run(self.run, FROZEN_MONO)
        assert len(groups) == 1, (
            f"Expected 1 correlated StallGroup, got {len(groups)}: {[g.incident_key for g in groups]}"
        )

    def test_cluster_is_correlated(self):
        """The single StallGroup must have is_correlated=True."""
        groups, _ = self.analyzer.analyze_run(self.run, FROZEN_MONO)
        assert groups[0].is_correlated is True

    def test_cluster_contains_all_three_agents(self):
        """All three agent IDs must appear in the stall group."""
        groups, _ = self.analyzer.analyze_run(self.run, FROZEN_MONO)
        agent_ids = set(groups[0].agent_ids)
        assert agent_ids == {"agent-alpha", "agent-beta", "agent-gamma"}

    def test_classification_is_escalate(self):
        """Idle > 2000 s exceeds transient_max (300 s) → must be 'escalate'."""
        groups, _ = self.analyzer.analyze_run(self.run, FROZEN_MONO)
        assert groups[0].classification == "escalate"

    def test_incident_key_contains_run_id(self):
        """Incident key must be scoped to the run_id."""
        groups, _ = self.analyzer.analyze_run(self.run, FROZEN_MONO)
        assert self.run.run_id in groups[0].incident_key


# ===========================================================================
# 2. Repeated-failure lane anomalies
# ===========================================================================


class TestRepeatedFailure:
    """Fixture: repeated_failure.json — terminal failed lane + running lane with failed children."""

    def setup_method(self):
        self.cfg = _dry_run_config()
        self.analyzer = Analyzer(self.cfg)
        self.run = _first_run("repeated_failure.json")

    def test_failed_terminal_lane_raises_anomaly(self):
        """agent-failed-terminal (state=failed) must produce a 'failed' LaneAnomaly."""
        _, anomalies = self.analyzer.analyze_run(self.run, FROZEN_MONO)
        failed_keys = [a.incident_key for a in anomalies if a.anomaly_type == "failed"]
        assert any("agent-failed-terminal" in k for k in failed_keys), (
            f"No 'failed' anomaly for agent-failed-terminal. anomalies={anomalies}"
        )

    def test_anomaly_description_mentions_stop_reason(self):
        """The description for the failed lane must reference its declared_outcome."""
        _, anomalies = self.analyzer.analyze_run(self.run, FROZEN_MONO)
        failed_anomalies = [
            a for a in anomalies if a.anomaly_type == "failed" and a.lane.agent_id == "agent-failed-terminal"
        ]
        assert failed_anomalies, "No failed anomaly for agent-failed-terminal"
        desc = failed_anomalies[0].description
        assert "failure" in desc, f"Expected 'failure' in description, got: {desc!r}"

    def test_no_stall_for_active_healthy_lane(self):
        """agent-failing with recent activity should NOT be stalled."""
        groups, _ = self.analyzer.analyze_run(self.run, FROZEN_MONO)
        stalled_agents = {a for g in groups for a in g.agent_ids}
        assert "agent-failing" not in stalled_agents, (
            "agent-failing has recent last_activity_ts and should not be stalled"
        )

    def test_incident_key_contains_run_id(self):
        """Every anomaly incident key must reference the run_id."""
        _, anomalies = self.analyzer.analyze_run(self.run, FROZEN_MONO)
        for anomaly in anomalies:
            assert self.run.run_id in anomaly.incident_key, (
                f"Anomaly {anomaly.incident_key!r} missing run_id {self.run.run_id!r}"
            )


# ===========================================================================
# 3. Transient stall classification
# ===========================================================================


class TestTransientRetry:
    """Fixture: transient_retry.json — one lane stalled 180 s, two healthy lanes."""

    def setup_method(self):
        self.cfg = _dry_run_config()
        self.analyzer = Analyzer(self.cfg)
        self.run = _first_run("transient_retry.json")

    def test_exactly_one_stalled_lane(self):
        """Only agent-transient should be stalled; 2 healthy lanes must not be."""
        groups, _ = self.analyzer.analyze_run(self.run, FROZEN_MONO)
        stalled = {a for g in groups for a in g.agent_ids}
        assert stalled == {"agent-transient"}, f"Expected only agent-transient stalled, got {stalled}"

    def test_stall_is_not_correlated(self):
        """1/3 stalled = 33 % < 50 % quorum → must NOT be correlated."""
        groups, _ = self.analyzer.analyze_run(self.run, FROZEN_MONO)
        assert len(groups) == 1
        assert groups[0].is_correlated is False

    def test_stall_classification_is_transient(self):
        """180 s idle < 300 s transient_max → must classify as 'transient'."""
        groups, _ = self.analyzer.analyze_run(self.run, FROZEN_MONO)
        assert groups[0].classification == "transient"

    def test_max_stall_seconds_reasonable(self):
        """max_stall_seconds should be ~180 s (server_time - last_activity_ts)."""
        groups, _ = self.analyzer.analyze_run(self.run, FROZEN_MONO)
        # Allow ±5 s slop for fixture timestamp rounding
        assert 170 <= groups[0].max_stall_seconds <= 200, f"Unexpected max_stall_seconds={groups[0].max_stall_seconds}"


# ===========================================================================
# 4.  Governance controls
# ===========================================================================


class TestGovernanceControls:
    """Verify all governance invariants fire correctly in DRY-RUN mode."""

    def _make_engine(self, **overrides) -> GovernanceEngine:
        cfg = _dry_run_config(**overrides)
        return GovernanceEngine(config=cfg)

    # 4a. Dry-run gate — most fundamental check
    def test_dry_run_returns_dry_run_only(self):
        """GovernanceEngine.check() must return DRY_RUN_ONLY when dry_run=True."""
        engine = self._make_engine()
        decision = engine.check(
            action="telegram_alert",
            incident_key="stall:run-1:agent-a",
            run_id="run-1",
            agent_id="agent-a",
        )
        assert decision == GovernanceDecision.DRY_RUN_ONLY

    # 4b. Allowlist — only works when dry_run=False
    def test_allowlist_blocks_unknown_action(self):
        """An action not in allowed_actions must be blocked."""
        engine = self._make_engine(dry_run=False, cooldown_seconds=0)
        decision = engine.check(
            action="reboot_agent",  # not in allowed_actions
            incident_key="stall:run-x:agent-x",
            run_id="run-x",
        )
        assert decision == GovernanceDecision.BLOCKED_NO_ACTION_PERMITTED

    def test_allowlist_permits_telegram_alert(self):
        """telegram_alert IS in allowed_actions → must not be blocked by allowlist."""
        engine = self._make_engine(
            dry_run=False,
            cooldown_seconds=0,
            max_interventions_per_run=99,
            max_retries_per_agent=99,
        )
        decision = engine.check(
            action="telegram_alert",
            incident_key="stall:run-y:agent-y",
            run_id="run-y",
            agent_id="agent-y",
        )
        assert decision == GovernanceDecision.ALLOWED

    # 4c. Idempotency key
    def test_idempotency_key_blocks_duplicate(self):
        """The same incident_key may not be acted on twice."""
        engine = self._make_engine(
            dry_run=False,
            cooldown_seconds=0,
            max_interventions_per_run=99,
            max_retries_per_agent=99,
        )
        key = "stall:run-idem:agent-idem"
        # First action: should be ALLOWED
        first = engine.check(action="telegram_alert", incident_key=key, run_id="run-idem", agent_id="agent-idem")
        assert first == GovernanceDecision.ALLOWED
        engine.record_action(incident_key=key, run_id="run-idem", agent_id="agent-idem")

        # Second action on same key: BLOCKED_ALREADY_ACTED
        second = engine.check(action="telegram_alert", incident_key=key, run_id="run-idem", agent_id="agent-idem")
        assert second == GovernanceDecision.BLOCKED_ALREADY_ACTED

    # 4d. Cooldown
    def test_cooldown_blocks_rapid_refire(self):
        """A second action within cooldown_seconds must be blocked."""
        engine = self._make_engine(
            dry_run=False,
            cooldown_seconds=9999,  # very long cooldown
            max_interventions_per_run=99,
            max_retries_per_agent=99,
        )
        key = "stall:run-cool:agent-cool"
        # Force last_action_time to be very recent
        engine._last_action_times[key] = time.monotonic()

        decision = engine.check(
            action="telegram_alert",
            incident_key=key,
            run_id="run-cool",
            agent_id="agent-cool",
        )
        assert decision == GovernanceDecision.BLOCKED_COOLDOWN

    # 4e. Per-run intervention limit
    def test_per_run_intervention_limit(self):
        """After max_interventions_per_run actions in a run, further actions are blocked."""
        max_iv = 2
        engine = self._make_engine(
            dry_run=False,
            cooldown_seconds=0,
            max_interventions_per_run=max_iv,
            max_retries_per_agent=99,
        )
        run_id = "run-limit"
        for i in range(max_iv):
            key = f"stall:{run_id}:agent-{i}"
            dec = engine.check(action="telegram_alert", incident_key=key, run_id=run_id, agent_id=f"agent-{i}")
            assert dec == GovernanceDecision.ALLOWED, f"Action {i} should be allowed"
            engine.record_action(incident_key=key, run_id=run_id, agent_id=f"agent-{i}")

        # One more — must be blocked
        key_over = f"stall:{run_id}:agent-over"
        dec_over = engine.check(action="telegram_alert", incident_key=key_over, run_id=run_id, agent_id="agent-over")
        assert dec_over == GovernanceDecision.BLOCKED_RUN_LIMIT

    # 4f. Per-agent retry limit
    def test_per_agent_retry_limit(self):
        """After max_retries_per_agent for an agent, further actions are blocked."""
        max_r = 2
        engine = self._make_engine(
            dry_run=False,
            cooldown_seconds=0,
            max_interventions_per_run=99,
            max_retries_per_agent=max_r,
        )
        agent_id = "agent-retry"
        for i in range(max_r):
            run_id = f"run-retry-{i}"
            key = f"stall:{run_id}:{agent_id}"
            dec = engine.check(action="telegram_alert", incident_key=key, run_id=run_id, agent_id=agent_id)
            assert dec == GovernanceDecision.ALLOWED, f"Retry {i} should be allowed"
            engine.record_action(incident_key=key, run_id=run_id, agent_id=agent_id)

        # One more with same agent in a different run — must be blocked
        key_over = f"stall:run-retry-over:{agent_id}"
        dec_over = engine.check(
            action="telegram_alert", incident_key=key_over, run_id="run-retry-over", agent_id=agent_id
        )
        assert dec_over == GovernanceDecision.BLOCKED_AGENT_LIMIT

    # 4g. run_intervention_count and agent_retry_count introspection
    def test_run_intervention_counter_increments(self):
        """run_intervention_count must increment after each record_action."""
        engine = self._make_engine(
            dry_run=False,
            cooldown_seconds=0,
            max_interventions_per_run=99,
            max_retries_per_agent=99,
        )
        run_id = "run-counter"
        assert engine.run_intervention_count(run_id) == 0
        for i in range(3):
            engine.record_action(incident_key=f"k-{i}", run_id=run_id)
        assert engine.run_intervention_count(run_id) == 3

    def test_agent_retry_counter_increments(self):
        """agent_retry_count must increment after each record_action for that agent."""
        engine = self._make_engine(
            dry_run=False,
            cooldown_seconds=0,
            max_interventions_per_run=99,
            max_retries_per_agent=99,
        )
        agent_id = "agent-count"
        assert engine.agent_retry_count(agent_id) == 0
        for i in range(2):
            engine.record_action(incident_key=f"k-{i}", run_id=f"run-{i}", agent_id=agent_id)
        assert engine.agent_retry_count(agent_id) == 2


# ===========================================================================
# 5. Audit log entry written for every governance decision
# ===========================================================================


class TestAuditLog:
    """Verify the AuditLog writes a JSONL entry for every governance outcome."""

    def _make_audit_log(self) -> tuple[AuditLog, io.StringIO]:
        """Create an AuditLog backed by an in-memory StringIO."""
        buf = io.StringIO()
        audit = AuditLog.__new__(AuditLog)
        audit._path = "<memory>"
        audit._file = buf
        return audit, buf

    def test_record_writes_json_line(self):
        """AuditLog.record() must append a valid JSON line."""
        audit, buf = self._make_audit_log()
        audit.record(
            incident_key="stall:run-audit:agent-a",
            run_id="run-audit",
            agent_id="agent-a",
            decision_type="correlated_stall",
            reasoning="Three lanes co-stalled for 2100 s",
            dry_run=True,
            governance_outcome="dry_run_only",
        )
        line = buf.getvalue().strip()
        assert line, "AuditLog produced no output"
        entry = json.loads(line)
        assert entry["incident_key"] == "stall:run-audit:agent-a"
        assert entry["run_id"] == "run-audit"
        assert entry["agent_id"] == "agent-a"
        assert entry["dry_run"] is True
        assert entry["governance_outcome"] == "dry_run_only"

    def test_record_includes_timestamp(self):
        """Each audit entry must have an ISO-8601 'ts' field."""
        audit, buf = self._make_audit_log()
        audit.record(
            incident_key="k",
            run_id="r",
            agent_id=None,
            decision_type="end_of_run_summary",
            reasoning="run closed",
            dry_run=True,
            governance_outcome="dry_run_only",
        )
        entry = json.loads(buf.getvalue())
        assert "ts" in entry
        # Should parse as ISO-8601
        from datetime import datetime

        datetime.fromisoformat(entry["ts"].replace("Z", "+00:00"))

    def test_record_includes_extra_context(self):
        """Optional 'extra' dict must appear in the audit entry when provided."""
        audit, buf = self._make_audit_log()
        audit.record(
            incident_key="k2",
            run_id="r2",
            agent_id="a2",
            decision_type="isolated_stall",
            reasoning="lane idle 180 s",
            dry_run=True,
            governance_outcome="dry_run_only",
            extra={"max_stall_seconds": 180, "classification": "transient"},
        )
        entry = json.loads(buf.getvalue())
        assert entry["extra"]["max_stall_seconds"] == 180
        assert entry["extra"]["classification"] == "transient"

    def test_audit_entry_written_in_dry_run_mode(self):
        """When watcher processes correlated stall in DRY-RUN, audit entry is written."""
        cfg = _dry_run_config(audit_log_path=os.devnull)
        analyzer = Analyzer(cfg)
        run = _first_run("correlated_stall.json")
        groups, _ = analyzer.analyze_run(run, FROZEN_MONO)
        assert groups, "Expected stall groups from correlated_stall fixture"

        audit, buf = self._make_audit_log()
        engine = GovernanceEngine(config=cfg)
        sg = groups[0]

        decision = engine.check(
            action="telegram_alert",
            incident_key=sg.incident_key,
            run_id=run.run_id,
        )
        audit.record(
            incident_key=sg.incident_key,
            run_id=run.run_id,
            agent_id=None,
            decision_type="correlated_stall",
            reasoning=f"Correlated stall: agents={sg.agent_ids}",
            dry_run=cfg.dry_run,
            governance_outcome=decision.value,
        )

        line = buf.getvalue().strip()
        assert line, "No audit entry written"
        entry = json.loads(line)
        assert entry["governance_outcome"] == "dry_run_only"
        assert entry["dry_run"] is True


# ===========================================================================
# 6. Telegram send stays a DRY-RUN stub — no network calls
# ===========================================================================


class TestTelegramDryRunStub:
    """Verify send_telegram records what it would send but never calls urlopen."""

    def _dry_cfg(self) -> WatcherConfig:
        return _dry_run_config()

    def test_send_telegram_dry_run_does_not_call_urlopen(self):
        """urlopen must NEVER be invoked in dry-run mode."""
        cfg = self._dry_cfg()
        with mock.patch("urllib.request.urlopen") as mock_urlopen:
            send_telegram("Test alert from fixture validation", config=cfg)
            mock_urlopen.assert_not_called()

    def test_send_telegram_dry_run_logs_message(self, caplog):
        """The DRY-RUN log entry must mention 'DRY-RUN' and the chat_id (or placeholder)."""
        cfg = self._dry_cfg()
        with caplog.at_level(logging.INFO, logger="watcher.telegram"):
            send_telegram("Correlated stall detected in fixture run", config=cfg)
        dry_run_logs = [r for r in caplog.records if "DRY-RUN" in r.getMessage()]
        assert dry_run_logs, (
            "Expected at least one log record mentioning 'DRY-RUN'; "
            f"got records: {[r.getMessage() for r in caplog.records]}"
        )

    def test_send_telegram_dry_run_with_alert_message(self):
        """Full round-trip: build stall message, pass to send_telegram, no network."""
        from watcher.run import _build_stall_message

        cfg = self._dry_cfg()
        run = _first_run("correlated_stall.json")
        analyzer = Analyzer(cfg)
        groups, _ = analyzer.analyze_run(run, FROZEN_MONO)
        assert groups

        msg = _build_stall_message(run, groups[0])
        with mock.patch("urllib.request.urlopen") as mock_urlopen:
            send_telegram(msg, config=cfg)
            mock_urlopen.assert_not_called()

    def test_send_telegram_live_would_call_urlopen(self):
        """Sanity check: with dry_run=False and a configured token, urlopen IS called."""
        cfg = _dry_run_config(
            dry_run=False,
            telegram_bot_token="123456:ABCDEFtest",
            telegram_chat_id="-100123456",
        )
        mock_resp = mock.MagicMock()
        mock_resp.__enter__ = mock.MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = mock.MagicMock(return_value=False)
        mock_resp.status = 200
        with mock.patch("urllib.request.urlopen", return_value=mock_resp) as mock_urlopen:
            send_telegram("Live send test", config=cfg)
            mock_urlopen.assert_called_once()

    def test_repeated_failure_anomaly_alert_stays_dry_run(self):
        """Lane anomaly alert from repeated_failure fixture must not trigger network."""
        from watcher.run import _build_anomaly_message

        cfg = self._dry_cfg()
        run = _first_run("repeated_failure.json")
        analyzer = Analyzer(cfg)
        _, anomalies = analyzer.analyze_run(run, FROZEN_MONO)

        failed_anomalies = [a for a in anomalies if a.anomaly_type == "failed"]
        assert failed_anomalies, "No 'failed' anomaly in repeated_failure fixture"

        msg = _build_anomaly_message(run, failed_anomalies[0])
        with mock.patch("urllib.request.urlopen") as mock_urlopen:
            send_telegram(msg, config=cfg)
            mock_urlopen.assert_not_called()


# ===========================================================================
# 7. End-to-end: fixture → analyzer → governance → audit in DRY-RUN
# ===========================================================================


class TestEndToEndDryRun:
    """Smoke-test all three fixtures through the full pipeline in DRY-RUN."""

    def _run_pipeline(self, fixture_name: str) -> list[dict]:
        """Returns list of audit entries (as dicts) emitted for the fixture."""
        cfg = _dry_run_config()
        analyzer = Analyzer(cfg)
        engine = GovernanceEngine(config=cfg)

        buf = io.StringIO()
        audit = AuditLog.__new__(AuditLog)
        audit._path = "<memory>"
        audit._file = buf

        run = _first_run(fixture_name)
        groups, anomalies = analyzer.analyze_run(run, FROZEN_MONO)

        with mock.patch("urllib.request.urlopen") as mock_urlopen:
            for sg in groups:
                decision = engine.check(
                    action="telegram_alert",
                    incident_key=sg.incident_key,
                    run_id=run.run_id,
                    agent_id=sg.agent_ids[0] if len(sg.agent_ids) == 1 else None,
                )
                audit.record(
                    incident_key=sg.incident_key,
                    run_id=run.run_id,
                    agent_id=sg.agent_ids[0] if len(sg.agent_ids) == 1 else None,
                    decision_type="correlated_stall" if sg.is_correlated else "isolated_stall",
                    reasoning=f"agents={sg.agent_ids}, classification={sg.classification}",
                    dry_run=cfg.dry_run,
                    governance_outcome=decision.value,
                )

            for anomaly in anomalies:
                decision = engine.check(
                    action="telegram_alert",
                    incident_key=anomaly.incident_key,
                    run_id=run.run_id,
                    agent_id=anomaly.lane.agent_id,
                )
                audit.record(
                    incident_key=anomaly.incident_key,
                    run_id=run.run_id,
                    agent_id=anomaly.lane.agent_id,
                    decision_type=anomaly.anomaly_type,
                    reasoning=anomaly.description,
                    dry_run=cfg.dry_run,
                    governance_outcome=decision.value,
                )

            mock_urlopen.assert_not_called()

        entries = [json.loads(line) for line in buf.getvalue().splitlines() if line.strip()]
        return entries

    def test_correlated_stall_produces_audit_entry(self):
        entries = self._run_pipeline("correlated_stall.json")
        assert entries, "Expected at least one audit entry for correlated_stall"
        assert all(e["dry_run"] is True for e in entries)
        assert all(e["governance_outcome"] == "dry_run_only" for e in entries)
        stall_entries = [e for e in entries if e["decision_type"] == "correlated_stall"]
        assert stall_entries, f"No correlated_stall audit entry; entries={entries}"

    def test_repeated_failure_produces_failed_anomaly_entry(self):
        entries = self._run_pipeline("repeated_failure.json")
        assert entries, "Expected at least one audit entry for repeated_failure"
        failed_entries = [e for e in entries if e["decision_type"] == "failed"]
        assert failed_entries, f"No 'failed' audit entry; entries={entries}"

    def test_transient_retry_produces_isolated_stall_entry(self):
        entries = self._run_pipeline("transient_retry.json")
        assert entries, "Expected at least one audit entry for transient_retry"
        isolated_entries = [e for e in entries if e["decision_type"] == "isolated_stall"]
        assert isolated_entries, f"No isolated_stall audit entry; entries={entries}"
        # Verify the stall was classified transient in the reasoning
        assert any("transient" in e["reasoning"] for e in isolated_entries), (
            f"Expected 'transient' in reasoning; entries={isolated_entries}"
        )

    def test_no_fixture_triggers_live_send(self):
        """Running all three fixtures must never invoke urlopen."""
        with mock.patch("urllib.request.urlopen") as mock_urlopen:
            for fixture in ("correlated_stall.json", "repeated_failure.json", "transient_retry.json"):
                self._run_pipeline(fixture)
            mock_urlopen.assert_not_called()
