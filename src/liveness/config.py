from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def _float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return float(value)


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class LivenessConfig:
    db_path: Path = Path(os.getenv("LIVENESS_DB_PATH", "./data/liveness/events.db"))
    ingest_token: str = os.getenv("LIVENESS_INGEST_TOKEN", "")
    bind_host: str = os.getenv("LIVENESS_BIND_HOST", "127.0.0.1")
    bind_port: int = _int_env("LIVENESS_BIND_PORT", 8723)

    max_body_bytes: int = _int_env("LIVENESS_MAX_BODY_BYTES", 262_144)
    timestamp_past_window_sec: int = _int_env("LIVENESS_TS_PAST_WINDOW_SEC", 86_400)
    timestamp_future_window_sec: int = _int_env("LIVENESS_TS_FUTURE_WINDOW_SEC", 86_400)
    run_id_max_len: int = _int_env("LIVENESS_RUN_ID_MAX_LEN", 256)
    agent_id_max_len: int = _int_env("LIVENESS_AGENT_ID_MAX_LEN", 128)
    event_id_max_len: int = _int_env("LIVENESS_EVENT_ID_MAX_LEN", 64)

    summary_max_chars: int = _int_env("LIVENESS_SUMMARY_MAX_CHARS", 512)
    worktree_max_chars: int = _int_env("LIVENESS_WORKTREE_MAX_CHARS", 256)
    model_max_chars: int = _int_env("LIVENESS_MODEL_MAX_CHARS", 160)
    activity_max_chars: int = _int_env("LIVENESS_ACTIVITY_MAX_CHARS", 512)
    error_max_chars: int = _int_env("LIVENESS_ERROR_MAX_CHARS", 1024)
    final_message_preview_chars: int = _int_env("LIVENESS_FINAL_MESSAGE_PREVIEW_CHARS", 256)
    store_full_final_message: bool = _bool_env("LIVENESS_STORE_FULL_FINAL_MESSAGE", False)

    stale_threshold_sec: float = _float_env("LIVENESS_STALE_THRESHOLD_SEC", 60.0)
    stuck_threshold_sec: float = _float_env("LIVENESS_STUCK_THRESHOLD_SEC", 300.0)
    gap_timeout_sec: float = _float_env("LIVENESS_GAP_TIMEOUT_SEC", 30.0)
    run_abandon_threshold_sec: float = _float_env("LIVENESS_RUN_ABANDON_THRESHOLD_SEC", 3600.0)
    stall_threshold_sec: float = _float_env("LIVENESS_STALL_THRESHOLD_SEC", 600.0)
    max_events_page_size: int = _int_env("LIVENESS_MAX_EVENTS_PAGE_SIZE", 1000)

    retention_days: int = _int_env("LIVENESS_RETENTION_DAYS", 14)
    archive_dir: Path | None = Path(os.environ["LIVENESS_ARCHIVE_DIR"]) if os.getenv("LIVENESS_ARCHIVE_DIR") else None


def config_from_env() -> LivenessConfig:
    return LivenessConfig()
