from __future__ import annotations

import hashlib
import os
import shlex
import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .paths import runs_dir
from .state import read_current_run
from .util import safe_path_component


TARGETS = {"codex", "opencode"}
SEPARATORS = {"&&", "||", ";", "|"}

# error.class / identity_error / stop_reason marker for a guard-blocked bare CLI.
GUARD_BLOCK_CLASS = "guard_blocked_bare_cli"


def check_pre_tool_use(payload: dict[str, Any]) -> tuple[int, str]:
    if payload.get("tool_name") != "Bash":
        return 0, ""
    command = _extract_command(payload.get("tool_input"))
    if not command:
        return 0, ""
    blocked = find_blocked_invocation(command, cwd=str(payload.get("cwd") or os.getcwd()))
    if blocked is None:
        return 0, ""
    session_id = str(payload.get("session_id") or "unknown-session")
    try:
        run_id = str(read_current_run(session_id)["run_id"])
    except Exception:
        run_id = "<run_id>"
    agent_id = os.getenv("AGENT_ID", "<agent_id>")
    run_component = safe_path_component(run_id) if not run_id.startswith("<") else run_id
    agent_component = safe_path_component(agent_id) if not agent_id.startswith("<") else agent_id
    base = runs_dir() / run_component / "agents" / agent_component / "bin"
    message = (
        "Blocked: bare CLI invocation detected.\n"
        "Use the telemetry wrapper path instead:\n"
        f"  {base / 'codex'}\n"
        f"  {base / 'opencode-glm'}\n"
    )
    # A bare CLI blocked by the guard must produce a lane_stopped event whose
    # error.class marks it guard-blocked. The guard cannot attribute the Bash call to a subagent,
    # so the lane is routed to a deterministically-keyed quarantine lane (never a null agent_id).
    # Fail-open: a telemetry-side failure must NEVER change the block — exit 2 still happens.
    _emit_guard_block_event(session_id=session_id, run_id=run_id, blocked=str(blocked), command=command)
    return 2, message


def _emit_guard_block_event(*, session_id: str, run_id: str, blocked: str, command: str) -> None:
    """Enqueue a lane_stopped event marking a guard-blocked bare CLI (best-effort, never raises)."""
    try:
        from .events import build_event
        from .outbox import Outbox

        # run_id must be a real, non-null value for the event to land (schema requires it). If we
        # could not resolve the current run, the block still happens; we just skip the event rather
        # than emit a placeholder that the backend would reject.
        if not run_id or run_id.startswith("<"):
            return
        source_instance_id = f"hook:{session_id}:PreToolUseGuard:{uuid.uuid4()}"
        agent_id = _guard_quarantine_agent_id(session_id=session_id, run_id=run_id, blocked=blocked)
        event = build_event(
            event_type="lane_stopped",
            run_id=run_id,
            session_id=session_id,
            orchestrator="claude-code",
            agent_id=agent_id,
            quarantine=True,
            identity_error=GUARD_BLOCK_CLASS,
            source="hook",
            source_instance_id=source_instance_id,
            source_seq=1,
            stop_reason=GUARD_BLOCK_CLASS,
            declared_outcome="failure",
            error={
                "class": GUARD_BLOCK_CLASS,
                "message": f"PreToolUse guard blocked bare CLI invocation: {blocked}",
                "blocked_executable": blocked,
            },
        )
        Outbox(max_events=int(os.getenv("CC_HOST_OUTBOX_MAX_EVENTS", "10000"))).append(event)
    except Exception:
        # Telemetry must never block the guard. Swallow everything; the exit-2 block is unaffected.
        return


def _guard_quarantine_agent_id(*, session_id: str, run_id: str, blocked: str) -> str:
    """Deterministic quarantine agent_id keyed on (session || run || blocked path || ts_bucket).

    ts_bucket is a coarse (minute) bucket so transient repeated blocks of the same command group
    into one quarantine lane. Identical inputs in the same bucket yield the same hash.
    """
    bucket = datetime.now(UTC).isoformat(timespec="minutes")
    material = "|".join([session_id, run_id, blocked, bucket])
    return f"quarantine:{hashlib.sha256(material.encode('utf-8')).hexdigest()[:16]}"


def find_blocked_invocation(command: str, cwd: str | None = None) -> Path | str | None:
    cwd_path = Path(cwd or os.getcwd())
    try:
        tokens = shlex.split(command, posix=True)
    except ValueError:
        tokens = command.split()
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token in SEPARATORS:
            i += 1
            continue
        if token == "cd" and i + 1 < len(tokens):
            candidate = Path(tokens[i + 1])
            cwd_path = candidate if candidate.is_absolute() else (cwd_path / candidate)
            i += 2
            continue
        while i < len(tokens) and _is_env_assignment(tokens[i]):
            i += 1
        if i >= len(tokens):
            break
        executable = tokens[i]
        resolved = _resolve_executable(executable, cwd_path)
        target_name = _target_name(executable, resolved)
        if target_name in TARGETS and not _is_allowed_wrapper(resolved):
            return resolved or executable
        i += 1
        while i < len(tokens) and tokens[i] not in SEPARATORS:
            i += 1
    return None


def _extract_command(tool_input: object) -> str:
    if isinstance(tool_input, str):
        return tool_input
    if isinstance(tool_input, dict):
        value = tool_input.get("command") or tool_input.get("cmd")
        return str(value) if value is not None else ""
    return ""


def _resolve_executable(executable: str, cwd: Path) -> Path | None:
    raw = Path(executable)
    if raw.name not in TARGETS and executable not in TARGETS:
        return None
    if raw.parent != Path("."):
        candidate = raw if raw.is_absolute() else cwd / raw
        return candidate.resolve(strict=False)
    found = shutil.which(executable)
    return Path(found).resolve(strict=False) if found else None


def _target_name(executable: str, resolved: Path | None) -> str:
    raw_name = Path(executable).name
    if raw_name in TARGETS:
        return raw_name
    if resolved is not None and resolved.name in TARGETS:
        return resolved.name
    return raw_name


def _is_allowed_wrapper(resolved: Path | None) -> bool:
    if resolved is None:
        return False
    try:
        resolved.relative_to(runs_dir().resolve(strict=False))
    except ValueError:
        return False
    return "/bin/" in resolved.as_posix()


def _is_env_assignment(token: str) -> bool:
    if "=" not in token or token.startswith("="):
        return False
    key = token.split("=", 1)[0]
    return key.replace("_", "").isalnum()
