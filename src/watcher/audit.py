"""Audit log — append-only JSONL record of every watcher decision.

Every decision that *would* trigger an action, whether in dry-run or live
mode, is written here with:
  - ISO-8601 timestamp
  - incident idempotency key
  - run_id and agent_id
  - decision type and reasoning
  - dry_run flag
  - governance outcome (allowed / blocked / cooldown / limit_reached)

The audit log is append-only.  The file is opened in append mode; if the
parent directory does not exist the log falls back to stderr so no audit
record is ever silently lost.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional, TextIO

logger = logging.getLogger(__name__)


class AuditLog:
    """Append-only JSONL audit log for watcher governance decisions."""

    def __init__(self, path: str) -> None:
        self._path = path
        self._file: TextIO | None = None
        self._open()

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def _open(self) -> None:
        """Open (or create) the audit log file, falling back to stderr."""
        try:
            parent = os.path.dirname(self._path)
            if parent and not os.path.isdir(parent):
                os.makedirs(parent, mode=0o750, exist_ok=True)
            self._file = open(self._path, "a", encoding="utf-8", buffering=1)
        except OSError as exc:
            logger.warning(
                "Audit log at %r not writable (%s); falling back to stderr.",
                self._path,
                exc,
            )
            self._file = sys.stderr

    def close(self) -> None:
        if self._file and self._file is not sys.stderr:
            self._file.close()

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def record(
        self,
        *,
        incident_key: str,
        run_id: str,
        agent_id: Optional[str],
        decision_type: str,
        reasoning: str,
        dry_run: bool,
        governance_outcome: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write one audit entry.

        Parameters
        ----------
        incident_key:
            Idempotency key for this incident (e.g. ``stall:run-1:agent-1``).
        run_id:
            The run being watched.
        agent_id:
            The agent involved, or ``None`` for run-level events.
        decision_type:
            Short label, e.g. ``"correlated_stall"``, ``"lane_failed"``,
            ``"end_of_run_summary"``.
        reasoning:
            Human-readable explanation of why this decision was reached.
        dry_run:
            Whether the watcher was in dry-run mode at decision time.
        governance_outcome:
            One of: ``"allowed"``, ``"blocked_no_action_permitted"``,
            ``"blocked_cooldown"``, ``"blocked_run_limit"``,
            ``"blocked_agent_limit"``, ``"dry_run_only"``.
        extra:
            Optional additional context (kept shallow; not recursed into).
        """
        entry: Dict[str, Any] = {
            "ts": datetime.now(tz=timezone.utc).isoformat(),
            "incident_key": incident_key,
            "run_id": run_id,
            "agent_id": agent_id,
            "decision_type": decision_type,
            "reasoning": reasoning,
            "dry_run": dry_run,
            "governance_outcome": governance_outcome,
        }
        if extra:
            entry["extra"] = extra
        if self._file is None:
            return
        try:
            self._file.write(json.dumps(entry) + "\n")
        except OSError as exc:
            logger.error("Failed to write audit entry: %s", exc)
