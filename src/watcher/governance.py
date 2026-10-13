"""Action governance engine.

Enforces all safety invariants before any action is taken:

1. **Per-run intervention limit** — at most N interventions per run_id.
2. **Per-agent retry limit** — at most M retries per agent_id (session-scoped).
3. **Cooldown window** — an incident key may not be re-acted on within C seconds.
4. **Idempotency key per incident** — the same incident is never acted on twice.
5. **Allowlist of permitted actions** — only explicitly listed action types may
   be executed; everything else is blocked regardless of other limits.
6. **Dry-run mode** — when active, no action is ever dispatched; every call
   returns ``GovernanceDecision.DRY_RUN``.

The governance engine holds all its state in memory.  On watcher restart the
counters reset.  This is acceptable for the read-only first phase; persistent
state can be added later when auto-reaction is enabled.

No action is constructed from raw event content here.  The ``action``
parameter is a short symbolic name (e.g. ``"telegram_alert"``); the caller is
responsible for constructing the safe, redacted payload separately.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Set

from .config import WatcherConfig

logger = logging.getLogger(__name__)


class GovernanceDecision(str, Enum):
    ALLOWED = "allowed"
    DRY_RUN_ONLY = "dry_run_only"
    BLOCKED_NO_ACTION_PERMITTED = "blocked_no_action_permitted"
    BLOCKED_COOLDOWN = "blocked_cooldown"
    BLOCKED_RUN_LIMIT = "blocked_run_limit"
    BLOCKED_AGENT_LIMIT = "blocked_agent_limit"
    BLOCKED_ALREADY_ACTED = "blocked_already_acted"


@dataclass
class GovernanceEngine:
    """Stateful governance guard for watcher actions."""

    config: WatcherConfig

    # incident keys that have already been acted on (idempotency)
    _acted_keys: Set[str] = field(default_factory=set)
    # run_id → intervention count
    _run_intervention_counts: Dict[str, int] = field(default_factory=dict)
    # agent_id → retry count
    _agent_retry_counts: Dict[str, int] = field(default_factory=dict)
    # incident_key → last action timestamp (monotonic)
    _last_action_times: Dict[str, float] = field(default_factory=dict)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def check(
        self,
        *,
        action: str,
        incident_key: str,
        run_id: str,
        agent_id: Optional[str] = None,
    ) -> GovernanceDecision:
        """Evaluate whether *action* may proceed for the given incident.

        This method is SIDE-EFFECT FREE — it does not record anything.  Call
        ``record_action`` after executing to update internal state.
        """
        # 1. Dry-run gate (cheapest check first)
        if self.config.dry_run:
            return GovernanceDecision.DRY_RUN_ONLY

        # 2. Allowlist gate
        if action not in self.config.allowed_actions:
            return GovernanceDecision.BLOCKED_NO_ACTION_PERMITTED

        # 3. Idempotency gate
        if incident_key in self._acted_keys:
            return GovernanceDecision.BLOCKED_ALREADY_ACTED

        # 4. Cooldown gate
        last = self._last_action_times.get(incident_key, 0.0)
        elapsed = time.monotonic() - last
        if elapsed < self.config.cooldown_seconds:
            return GovernanceDecision.BLOCKED_COOLDOWN

        # 5. Per-run intervention limit
        run_count = self._run_intervention_counts.get(run_id, 0)
        if run_count >= self.config.max_interventions_per_run:
            return GovernanceDecision.BLOCKED_RUN_LIMIT

        # 6. Per-agent retry limit
        if agent_id is not None:
            agent_count = self._agent_retry_counts.get(agent_id, 0)
            if agent_count >= self.config.max_retries_per_agent:
                return GovernanceDecision.BLOCKED_AGENT_LIMIT

        return GovernanceDecision.ALLOWED

    def record_action(
        self,
        *,
        incident_key: str,
        run_id: str,
        agent_id: Optional[str] = None,
    ) -> None:
        """Record that an action was executed for *incident_key*.

        Must only be called when the action was actually dispatched (i.e.
        ``check`` returned ``ALLOWED``).
        """
        self._acted_keys.add(incident_key)
        self._last_action_times[incident_key] = time.monotonic()
        self._run_intervention_counts[run_id] = self._run_intervention_counts.get(run_id, 0) + 1
        if agent_id is not None:
            self._agent_retry_counts[agent_id] = self._agent_retry_counts.get(agent_id, 0) + 1

    def reset_run(self, run_id: str) -> None:
        """Discard intervention state for a completed run.

        Called when the watcher sees a run close so the next run starts with
        fresh counters.  Agent retry counts are session-scoped and are NOT
        reset here.
        """
        self._run_intervention_counts.pop(run_id, None)
        # Remove acted keys and cooldown entries scoped to this run
        scoped = [k for k in self._acted_keys if k.startswith(f"run:{run_id}:")]
        for k in scoped:
            self._acted_keys.discard(k)
            self._last_action_times.pop(k, None)

    # ------------------------------------------------------------------ #
    # Introspection helpers (for logging / audit context)
    # ------------------------------------------------------------------ #

    def run_intervention_count(self, run_id: str) -> int:
        return self._run_intervention_counts.get(run_id, 0)

    def agent_retry_count(self, agent_id: str) -> int:
        return self._agent_retry_counts.get(agent_id, 0)
