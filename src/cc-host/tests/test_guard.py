from __future__ import annotations

import os

from cc_host.guard import (
    GUARD_BLOCK_CLASS,
    _guard_quarantine_agent_id,
    check_pre_tool_use,
    find_blocked_invocation,
)
from cc_host.outbox import Outbox
from cc_host.state import write_current_run


def _setup_home(tmp_path, monkeypatch) -> str:
    home = tmp_path / "telemetry"
    monkeypatch.setenv("CLAUDE_TELEMETRY_HOME", str(home))
    monkeypatch.setenv("CC_HOST_OUTBOX_DB", str(tmp_path / "events.db"))
    run = write_current_run(
        session_id="sess-guard",
        user_prompt="p",
        cwd=str(tmp_path),
        project="t",
        operator_label=None,
    )
    return str(run["run_id"])


def _block(command: str) -> tuple[int, str]:
    return check_pre_tool_use(
        {
            "tool_name": "Bash",
            "session_id": "sess-guard",
            "cwd": "/tmp",
            "tool_input": {"command": command},
        }
    )


def test_guard_blocks_bare_clis_with_exit_2(tmp_path, monkeypatch) -> None:
    _setup_home(tmp_path, monkeypatch)
    for command in ("codex exec hi", "opencode run task", "/usr/local/bin/codex exec x"):
        code, message = _block(command)
        assert code == 2, command
        assert "Blocked" in message


def test_guard_block_emits_lane_stopped_with_error_class(tmp_path, monkeypatch) -> None:
    run_id = _setup_home(tmp_path, monkeypatch)
    code, _ = _block("codex exec hi")
    assert code == 2

    rows = Outbox().unsent_rows()
    blocks = [r.event for r in rows if (r.event.get("error") or {}).get("class") == GUARD_BLOCK_CLASS]
    assert len(blocks) == 1
    event = blocks[0]
    assert event["event_type"] == "lane_stopped"
    assert event["run_id"] == run_id
    assert event["source"] == "hook"
    assert event["source_instance_id"].startswith("hook:sess-guard:PreToolUseGuard:")
    assert event["source_seq"] == 1
    # The guard cannot attribute the Bash call to a subagent: it routes to a quarantine lane,
    # never a null agent_id (keeps the "every lane event carries non-null agent_id" invariant).
    assert event["agent_id"].startswith("quarantine:")
    assert event["quarantine"] is True
    assert event["identity_error"] == GUARD_BLOCK_CLASS
    assert event["error"]["class"] == GUARD_BLOCK_CLASS
    assert event["stop_reason"] == GUARD_BLOCK_CLASS
    assert event["declared_outcome"] == "failure"


def test_allowed_wrapper_passes_and_emits_nothing(tmp_path, monkeypatch) -> None:
    _setup_home(tmp_path, monkeypatch)
    from cc_host.paths import runs_dir

    wrapper = runs_dir() / "RID" / "agents" / "AID" / "bin" / "codex"
    os.makedirs(wrapper.parent, exist_ok=True)
    wrapper.touch()

    code, message = _block(f"{wrapper} exec ok")
    assert code == 0
    assert message == ""
    assert Outbox().unsent_rows() == []


def test_guard_quarantine_agent_id_is_deterministic() -> None:
    a1 = _guard_quarantine_agent_id(session_id="s", run_id="r", blocked="/usr/local/bin/codex")
    a2 = _guard_quarantine_agent_id(session_id="s", run_id="r", blocked="/usr/local/bin/codex")
    a3 = _guard_quarantine_agent_id(session_id="s", run_id="r", blocked="/opt/homebrew/bin/opencode")
    assert a1 == a2
    assert a1 != a3
    assert a1.startswith("quarantine:")


def test_find_blocked_invocation_resists_env_and_cd_evasion() -> None:
    assert find_blocked_invocation("FOO=bar codex exec x") is not None
    assert find_blocked_invocation("ls && codex exec x") is not None
    assert find_blocked_invocation("echo hi") is None
