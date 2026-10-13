from __future__ import annotations

import os
from pathlib import Path


def telemetry_home() -> Path:
    return Path(os.getenv("CLAUDE_TELEMETRY_HOME", "~/.claude/telemetry")).expanduser()


def outbox_dir() -> Path:
    return Path(os.getenv("CC_HOST_OUTBOX_DIR", str(telemetry_home() / "outbox"))).expanduser()


def outbox_db_path() -> Path:
    return Path(os.getenv("CC_HOST_OUTBOX_DB", str(outbox_dir() / "events.db"))).expanduser()


def sessions_dir() -> Path:
    return telemetry_home() / "sessions"


def runs_dir() -> Path:
    return telemetry_home() / "runs"


def spool_dir() -> Path:
    return telemetry_home() / "spool"


def pricing_file() -> Path:
    return Path(os.getenv("CC_HOST_PRICING_FILE", str(telemetry_home() / "pricing" / "models.yaml"))).expanduser()


def log_dir() -> Path:
    return telemetry_home() / "logs"
