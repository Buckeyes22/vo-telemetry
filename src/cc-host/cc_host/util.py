from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


IDENTITY_RE = re.compile(r"^[A-Za-z0-9._:@/-]+$")


def utc_now() -> datetime:
    return datetime.now(UTC)


def rfc3339(value: datetime | str | None = None) -> str:
    if value is None:
        value = utc_now()
    if isinstance(value, str):
        return value
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def ensure_private_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)


def atomic_write_json(path: Path, payload: dict[str, Any], mode: int = 0o600) -> None:
    atomic_write_text(path, json.dumps(payload, sort_keys=True, indent=2) + "\n", mode=mode)


def atomic_write_text(path: Path, text: str, mode: int = 0o600) -> None:
    ensure_private_dir(path.parent)
    fd = -1
    tmp_name = ""
    try:
        fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            fd = -1
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp_name, mode)
        os.replace(tmp_name, path)
        directory_fd = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        if tmp_name and os.path.exists(tmp_name):
            os.unlink(tmp_name)


def safe_path_component(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._:@-]+", "_", value.strip())
    return sanitized[:160] or "unknown"


def valid_identity(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip()) and bool(IDENTITY_RE.match(value.strip()))


def read_json_stdin() -> dict[str, Any]:
    import sys

    raw = sys.stdin.read()
    if not raw.strip():
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("hook input must be a JSON object")
    return parsed
