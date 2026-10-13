"""Watcher configuration — all tunable knobs in one place.

Runtime defaults keep the watcher fully read-only and safe.
Override via environment variables or by constructing a WatcherConfig
directly.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import FrozenSet
from urllib.parse import urlsplit


@dataclass
class WatcherConfig:
    # ------------------------------------------------------------------ #
    # Snapshot API endpoint
    # ------------------------------------------------------------------ #
    api_base: str = field(default_factory=lambda: os.environ.get("WATCHER_API_BASE", "http://127.0.0.1:8723"))
    poll_interval_seconds: float = field(default_factory=lambda: float(os.environ.get("WATCHER_POLL_INTERVAL", "15")))
    api_timeout_seconds: float = field(default_factory=lambda: float(os.environ.get("WATCHER_API_TIMEOUT", "10")))

    # ------------------------------------------------------------------ #
    # Dry-run (default ON — no side-effects until explicitly disabled)
    # ------------------------------------------------------------------ #
    dry_run: bool = field(
        default_factory=lambda: os.environ.get("WATCHER_DRY_RUN", "true").lower() not in ("0", "false", "no")
    )

    # ------------------------------------------------------------------ #
    # Action governance limits
    # ------------------------------------------------------------------ #
    # Maximum interventions per run_id (across all agents in that run).
    max_interventions_per_run: int = field(
        default_factory=lambda: int(os.environ.get("WATCHER_MAX_INTERVENTIONS_PER_RUN", "3"))
    )
    # Maximum retries per agent_id within a single watcher session.
    max_retries_per_agent: int = field(
        default_factory=lambda: int(os.environ.get("WATCHER_MAX_RETRIES_PER_AGENT", "2"))
    )
    # Minimum seconds between interventions for the same incident key.
    cooldown_seconds: float = field(default_factory=lambda: float(os.environ.get("WATCHER_COOLDOWN_SECONDS", "300")))

    # ------------------------------------------------------------------ #
    # Stall detection thresholds
    # ------------------------------------------------------------------ #
    # Seconds of no activity before a lane is considered stalled.
    lane_stall_threshold_seconds: float = field(
        default_factory=lambda: float(os.environ.get("WATCHER_LANE_STALL_THRESHOLD", "120"))
    )
    # Fraction of active lanes that must stall together to trigger
    # correlated-stall classification (0.5 = 50 %).
    correlated_stall_quorum: float = field(
        default_factory=lambda: float(os.environ.get("WATCHER_CORRELATED_STALL_QUORUM", "0.5"))
    )
    # Seconds that classify a stall as transient (< threshold → transient,
    # >= threshold → escalate).
    transient_stall_max_seconds: float = field(
        default_factory=lambda: float(os.environ.get("WATCHER_TRANSIENT_STALL_MAX", "300"))
    )

    # ------------------------------------------------------------------ #
    # Allowlist of permitted auto-reaction actions
    # (empty by default — no actions permitted until explicitly unlocked)
    # ------------------------------------------------------------------ #
    allowed_actions: FrozenSet[str] = field(
        default_factory=lambda: frozenset(
            a.strip() for a in os.environ.get("WATCHER_ALLOWED_ACTIONS", "").split(",") if a.strip()
        )
    )

    # ------------------------------------------------------------------ #
    # Telegram (stub/dry-run by default)
    # ------------------------------------------------------------------ #
    telegram_bot_token: str = field(default_factory=lambda: os.environ.get("WATCHER_TELEGRAM_TOKEN", ""))
    telegram_chat_id: str = field(default_factory=lambda: os.environ.get("WATCHER_TELEGRAM_CHAT_ID", ""))
    # Hard cap on Telegram message length before truncation.
    telegram_max_length: int = 4000

    # ------------------------------------------------------------------ #
    # Audit log path
    # ------------------------------------------------------------------ #
    audit_log_path: str = field(
        default_factory=lambda: os.environ.get(
            "WATCHER_AUDIT_LOG",
            "./data/watcher-audit.jsonl",
        )
    )

    # ------------------------------------------------------------------ #
    # Redaction — field names whose values are blanked before any send
    # ------------------------------------------------------------------ #
    redact_fields: FrozenSet[str] = field(
        default_factory=lambda: frozenset(
            {
                "final_message_preview",
                "summary",
                "prompt",
                "response",
                "content",
                "db.statement",
                "http.request.body",
                "http.response.body",
            }
        )
    )

    def __post_init__(self) -> None:
        parts = urlsplit(self.api_base)
        if parts.scheme not in ("http", "https") or not parts.netloc:
            raise ValueError(f"WATCHER_API_BASE must be an http(s) URL, got {self.api_base!r}")
