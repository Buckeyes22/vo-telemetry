from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any

from .events import prompt_hash
from .paths import sessions_dir
from .util import atomic_write_json, ensure_private_dir, rfc3339, safe_path_component


def current_run_path(session_id: str) -> Path:
    return sessions_dir() / safe_path_component(session_id) / "current_run.json"


def write_current_run(
    *,
    session_id: str,
    user_prompt: str,
    cwd: str,
    project: str | None,
    operator_label: str | None,
    epoch: int | None = None,
) -> dict[str, Any]:
    prompt_epoch = int(epoch if epoch is not None else time.time())
    run_id = os.getenv("CLAUDE_TELEMETRY_RUN_ID") or f"{session_id}:{prompt_epoch}"
    payload: dict[str, Any] = {
        "run_id": run_id,
        "session_id": session_id,
        "started_ts": rfc3339(),
        "prompt_hash": prompt_hash(user_prompt),
        "cwd": cwd,
        "project": project or Path(cwd).name,
    }
    if operator_label:
        payload["operator_label"] = operator_label
    atomic_write_json(current_run_path(session_id), payload, mode=0o600)
    return payload


def read_current_run(session_id: str) -> dict[str, Any]:
    override = os.getenv("CLAUDE_TELEMETRY_RUN_ID")
    path = current_run_path(session_id)
    if override and not path.exists():
        return {
            "run_id": override,
            "session_id": session_id,
            "started_ts": rfc3339(),
            "prompt_hash": "manual-override",
            "cwd": os.getcwd(),
            "project": Path(os.getcwd()).name,
        }
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict) or not parsed.get("run_id") or parsed.get("session_id") != session_id:
        raise ValueError(f"malformed current run file: {path}")
    return parsed


def expire_current_run(session_id: str) -> None:
    path = current_run_path(session_id)
    if path.exists():
        path.unlink()


def register_lane(run_id: str, agent_id: str, payload: dict[str, Any]) -> None:
    from .paths import runs_dir

    lane_dir = runs_dir() / safe_path_component(run_id) / "agents" / safe_path_component(agent_id)
    ensure_private_dir(lane_dir)
    atomic_write_json(lane_dir / "lane.json", payload, mode=0o600)


def git_branch(cwd: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=cwd,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=0.3,
            check=False,
        )
    except Exception:
        return None
    branch = result.stdout.strip()
    return branch if result.returncode == 0 and branch else None
