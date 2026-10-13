from __future__ import annotations

import json
import os
import sys
import traceback
import uuid
from pathlib import Path
from typing import Any

from .events import build_event, final_message_fields
from .guard import check_pre_tool_use
from .outbox import Outbox
from .state import expire_current_run, git_branch, read_current_run, register_lane, write_current_run
from .util import read_json_stdin, rfc3339
from .wrappers import additional_context, failure_context, generate_wrappers


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    payload = read_json_stdin()
    event_name = args[0] if args else str(payload.get("hook_event_name") or "")
    try:
        if event_name == "UserPromptSubmit":
            output = user_prompt_submit(payload)
        elif event_name == "SubagentStart":
            output = subagent_start(payload)
        elif event_name == "SubagentStop":
            output = subagent_stop(payload)
        elif event_name == "PreToolUse":
            code, message = check_pre_tool_use(payload)
            if code:
                sys.stderr.write(message)
                return code
            output = {}
        elif event_name == "SessionEnd":
            output = session_end(payload)
        else:
            output = {}
    except Exception as exc:
        _log_hook_error(event_name, exc)
        output = {}
    if output:
        print(json.dumps(output, sort_keys=True))
    return 0


def user_prompt_submit(payload: dict[str, Any]) -> dict[str, Any]:
    session_id = _required(payload, "session_id")
    cwd = str(payload.get("cwd") or os.getcwd())
    prompt = str(payload.get("user_prompt") or "")
    prior_run_id = _prior_run_id(session_id)
    run = write_current_run(
        session_id=session_id,
        user_prompt=prompt,
        cwd=cwd,
        project=_project_name(cwd),
        operator_label=os.getenv("CLAUDE_TELEMETRY_OPERATOR_LABEL"),
    )
    # A new prompt mints a new run_id, but run_closed otherwise fires only once per session (at
    # SessionEnd) — so every earlier turn's run leaked as a perpetually-open run, accumulating
    # orphan lanes in the reducer. Close the prior run now. Guard on inequality: a same-second
    # prompt or a pinned CLAUDE_TELEMETRY_RUN_ID yields run_id == prior, and closing then would
    # terminalize a run that run_started can no longer reopen (terminal never regresses).
    if prior_run_id and prior_run_id != run["run_id"]:
        enqueue(
            build_event(
                event_type="run_closed",
                run_id=prior_run_id,
                session_id=session_id,
                orchestrator="claude-code",
                source="hook",
                source_instance_id=_hook_source(session_id, "UserPromptSubmitClosesPrior"),
                source_seq=1,
            )
        )
    enqueue(
        build_event(
            event_type="run_started",
            run_id=run["run_id"],
            session_id=session_id,
            orchestrator="claude-code",
            source="hook",
            source_instance_id=_hook_source(session_id, "UserPromptSubmit"),
            source_seq=1,
            prompt_hash=run["prompt_hash"],
            cwd=run["cwd"],
            project=run["project"],
            operator_label=run.get("operator_label"),
        )
    )
    return {
        "hookSpecificOutput": {
            "hookEventName": "UserPromptSubmit",
            "additionalContext": f"vo-telemetry run_id={run['run_id']}",
        }
    }


def subagent_start(payload: dict[str, Any]) -> dict[str, Any]:
    session_id = _required(payload, "session_id")
    agent_id = _required(payload, "agent_id")
    agent_type = str(payload.get("agent_type") or "unknown")
    cwd = str(payload.get("cwd") or os.getcwd())
    try:
        run = read_current_run(session_id)
        run_id = str(run["run_id"])
        model = _model(payload)
        wrappers = generate_wrappers(run_id=run_id, agent_id=agent_id, agent_type=agent_type, model=model)
        lane = {
            "run_id": run_id,
            "session_id": session_id,
            "agent_id": agent_id,
            "agent_type": agent_type,
            "wrapper_bin": str(wrappers.bin_dir),
            "started_ts": rfc3339(),
            "transcript_status": "pending",
        }
        register_lane(run_id, agent_id, lane)
        enqueue(
            build_event(
                event_type="lane_started",
                run_id=run_id,
                session_id=session_id,
                orchestrator="claude-code",
                agent_id=agent_id,
                agent_type=agent_type,
                provider=_provider_for_agent(agent_type),
                model=model,
                source="hook",
                source_instance_id=_hook_source(session_id, "SubagentStart"),
                source_seq=1,
                worktree=cwd,
                branch=git_branch(cwd),
            )
        )
        context = additional_context(wrappers)
    except Exception as exc:
        _log_hook_error("SubagentStart", exc)
        context = failure_context(str(exc))
        try:
            run_id = str(read_current_run(session_id)["run_id"])
            enqueue(
                build_event(
                    event_type="lane_stopped",
                    run_id=run_id,
                    session_id=session_id,
                    orchestrator="claude-code",
                    agent_id=agent_id,
                    agent_type=agent_type,
                    provider=_provider_for_agent(agent_type),
                    source="hook",
                    source_instance_id=_hook_source(session_id, "SubagentStartFailure"),
                    source_seq=1,
                    stop_reason="wrapper_generation_failed",
                    declared_outcome="failure",
                    error={"class": "wrapper_generation_failed", "message": str(exc)},
                )
            )
        except Exception:
            pass
    return {"hookSpecificOutput": {"hookEventName": "SubagentStart", "additionalContext": context}}


def subagent_stop(payload: dict[str, Any]) -> dict[str, Any]:
    session_id = _required(payload, "session_id")
    agent_id = _required(payload, "agent_id")
    agent_type = str(payload.get("agent_type") or "unknown")
    run = read_current_run(session_id)
    final_message = str(payload.get("last_assistant_message") or "")
    outcome, summary = _parse_outcome(final_message)
    fields = final_message_fields(final_message)
    _reconcile_transcript(payload, run_id=str(run["run_id"]), agent_id=agent_id)
    enqueue(
        build_event(
            event_type="lane_stopped",
            run_id=run["run_id"],
            session_id=session_id,
            orchestrator="claude-code",
            agent_id=agent_id,
            agent_type=agent_type,
            provider=_provider_for_agent(agent_type),
            source="hook",
            source_instance_id=_hook_source(session_id, "SubagentStop"),
            source_seq=1,
            stop_reason="normal_stop",
            declared_outcome=outcome,
            summary=summary,
            **fields,
            ext={
                "agent_transcript_path": payload.get("agent_transcript_path"),
                "stop_hook_active": payload.get("stop_hook_active"),
            },
        )
    )
    return {}


def session_end(payload: dict[str, Any]) -> dict[str, Any]:
    session_id = _required(payload, "session_id")
    try:
        run = read_current_run(session_id)
    except Exception:
        return {}
    enqueue(
        build_event(
            event_type="run_closed",
            run_id=run["run_id"],
            session_id=session_id,
            orchestrator="claude-code",
            source="hook",
            source_instance_id=_hook_source(session_id, "SessionEnd"),
            source_seq=1,
        )
    )
    expire_current_run(session_id)
    return {}


def enqueue(event: dict[str, Any]) -> None:
    Outbox(max_events=int(os.getenv("CC_HOST_OUTBOX_MAX_EVENTS", "10000"))).append(event)


def _prior_run_id(session_id: str) -> str | None:
    """The run_id currently recorded for this session, if any — i.e. the previous turn's run,
    read BEFORE this prompt overwrites it. None on the first prompt or any read error (best-effort;
    never fail the hook on it)."""
    try:
        prior = read_current_run(session_id)
    except Exception:
        return None
    run_id = prior.get("run_id") if isinstance(prior, dict) else None
    return run_id if isinstance(run_id, str) and run_id else None


def _reconcile_transcript(payload: dict[str, Any], *, run_id: str, agent_id: str) -> None:
    """: reconcile the lane against the authoritative ``agent_transcript_path``
    delivered at SubagentStop, flipping the lane file's ``transcript_status`` off
    ``pending``. Never blocks or fails the hook on a tailer error."""
    try:
        from .transcript_tailer import TranscriptTailer

        TranscriptTailer().reconcile(
            agent_id=agent_id,
            agent_transcript_path=payload.get("agent_transcript_path"),
            run_id=run_id,
        )
    except Exception as exc:
        _log_hook_error("SubagentStopReconcile", exc)


def _parse_outcome(message: str) -> tuple[str, str | None]:
    outcome = "unknown"
    summary: str | None = None
    for line in message.splitlines():
        if line.startswith("TELEMETRY_OUTCOME:"):
            candidate = line.split(":", 1)[1].strip()
            if candidate in {"success", "failure", "partial"}:
                outcome = candidate
        elif line.startswith("TELEMETRY_SUMMARY:"):
            summary = line.split(":", 1)[1].strip()[:512]
    return outcome, summary


def _hook_source(session_id: str, hook_name: str) -> str:
    return f"hook:{session_id}:{hook_name}:{uuid.uuid4()}"


def _required(payload: dict[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} is required")
    return value


def _project_name(cwd: str) -> str:
    return os.getenv("CLAUDE_PROJECT_NAME") or Path(cwd).name


def _model(payload: dict[str, Any]) -> str | None:
    value = payload.get("model") or os.getenv("MODEL") or os.getenv("CLAUDE_TELEMETRY_MODEL")
    return str(value) if value else None


def _provider_for_agent(agent_type: str) -> str:
    lowered = agent_type.lower()
    if "glm" in lowered:
        return "glm"
    if "kimi" in lowered:
        return "kimi"
    if "minimax" in lowered:
        return "minimax"
    if "codex" in lowered or "openai" in lowered:
        return "openai"
    if "opencode" in lowered:
        # The opencode CLI family. The concrete sub-provider (kimi/glm/minimax) is NOT known at
        # SubagentStart — it lives in the shim command args, not the subagent metadata — and is
        # captured on the shim's own self-attributed lane. "opencode" beats "unknown" here.
        return "opencode"
    return os.getenv("PROVIDER", "unknown")


def _log_hook_error(event_name: str, exc: Exception) -> None:
    from .paths import log_dir
    from .util import ensure_private_dir

    ensure_private_dir(log_dir())
    with (log_dir() / "hooks.log").open("a", encoding="utf-8") as handle:
        handle.write(f"[{rfc3339()}] {event_name}: {exc}\n")
        handle.write(traceback.format_exc())


if __name__ == "__main__":
    raise SystemExit(main())
