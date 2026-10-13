from __future__ import annotations

import json
import os
import sys

import pytest

from cc_host.pricing import PricingTable
from cc_host.shim import ShimSupervisor, parse_args
from cc_host.stream_parser import StreamParser


# --------------------------------------------------------------------------------------
# StreamParser: final agent message capture (feeds lane_stopped's preview/sha + outcome)
# --------------------------------------------------------------------------------------


def test_codex_final_agent_message_captured() -> None:
    parser = StreamParser("codex", PricingTable.load())
    parser.consume_line(json.dumps({"type": "item.started", "item": {"type": "agent_message"}}))
    parser.consume_line(
        json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "type": "agent_message",
                    "text": "Done.\nTELEMETRY_OUTCOME: success\nTELEMETRY_SUMMARY: refactored auth",
                },
            }
        )
    )
    assert "TELEMETRY_OUTCOME: success" in parser.state.final_message


def test_opencode_final_message_captured() -> None:
    parser = StreamParser("opencode", PricingTable.load())
    parser.consume_line(json.dumps({"type": "message.completed", "text": "all green"}))
    assert parser.state.final_message == "all green"


# --------------------------------------------------------------------------------------
# Identity: self-derivation (owns-run) vs wrapper path (env wins) vs quarantine
# --------------------------------------------------------------------------------------


def _clear_identity_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("RUN_ID", "AGENT_ID", "SESSION_ID", "CLAUDE_CODE_SESSION_ID", "CC_HOST_TASK_ID", "PROVIDER"):
        monkeypatch.delenv(key, raising=False)


def _supervisor(tmp_path, monkeypatch, argv: list[str], env: dict[str, str]) -> ShimSupervisor:
    monkeypatch.setenv("CLAUDE_TELEMETRY_HOME", str(tmp_path))
    _clear_identity_env(monkeypatch)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return ShimSupervisor(parse_args(argv))


def test_identity_self_derives_dedicated_run_from_session(tmp_path, monkeypatch) -> None:
    sup = _supervisor(
        tmp_path,
        monkeypatch,
        ["--cli", "codex", "--provider", "openai", "--self-attribute", "--task-id", "codex-20260601-001"],
        {"CLAUDE_CODE_SESSION_ID": "sess-1"},
    )
    assert sup.owns_run is True
    assert sup.identity.quarantine is False
    assert sup.identity.run_id == "sess-1:shim:codex-20260601-001"
    assert sup.identity.agent_id == "codex:codex-20260601-001"


def test_identity_detached_when_no_session(tmp_path, monkeypatch) -> None:
    sup = _supervisor(
        tmp_path,
        monkeypatch,
        ["--cli", "codex", "--provider", "openai", "--self-attribute", "--task-id", "t9"],
        {},
    )
    assert sup.owns_run is True
    assert sup.identity.quarantine is False
    assert sup.identity.run_id == "detached:shim:t9"
    assert sup.identity.agent_id == "codex:t9"


def test_wrapper_path_env_identity_wins_and_not_owns_run(tmp_path, monkeypatch) -> None:
    sup = _supervisor(
        tmp_path,
        monkeypatch,
        ["--cli", "codex", "--provider", "openai"],
        {"RUN_ID": "sess-9:123", "AGENT_ID": "feedfacefeedface", "SESSION_ID": "sess-9"},
    )
    assert sup.owns_run is False
    assert sup.identity.quarantine is False
    assert sup.identity.run_id == "sess-9:123"
    assert sup.identity.agent_id == "feedfacefeedface"


def test_identity_quarantines_when_provider_missing(tmp_path, monkeypatch) -> None:
    # Self-attribute mints run+agent, but a missing provider is still fail-closed → quarantine lane,
    # never a silent drop. parse_args default for --provider is "unknown" only if PROVIDER env
    # set; we pass it explicitly empty by omitting --provider AND clearing PROVIDER.
    monkeypatch.setenv("CLAUDE_TELEMETRY_HOME", str(tmp_path))
    _clear_identity_env(monkeypatch)
    args = parse_args(["--cli", "codex", "--self-attribute", "--task-id", "t1"])
    args.provider = ""  # simulate no provider resolved
    sup = ShimSupervisor(args)
    assert sup.identity.quarantine is True
    assert sup.identity.agent_id.startswith("quarantine:")
    assert "missing_provider" in (sup.identity.identity_error or "")


# --------------------------------------------------------------------------------------
# run(): full lane lifecycle in owns-run mode; tool-only under wrapper path
# --------------------------------------------------------------------------------------


@pytest.fixture
def devnull_stdio(monkeypatch):
    # run() wires the child's stdio to the process stdio and writes passthrough to sys.stdout.buffer;
    # pytest's captured streams lack .buffer, so point them at /dev/null real fds.
    stdin = open(os.devnull, "r")
    stdout = open(os.devnull, "w")
    stderr = open(os.devnull, "w")
    monkeypatch.setattr(sys, "stdin", stdin)
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(sys, "stderr", stderr)
    yield
    for handle in (stdin, stdout, stderr):
        handle.close()


def _run_with_fake_child(sup: ShimSupervisor, child_cmd: list[str]) -> tuple[int, list[dict]]:
    captured: list[dict] = []

    class _Recorder:
        def append(self, event: dict) -> bool:
            captured.append(event)
            return True

    sup.outbox = _Recorder()  # type: ignore[assignment]
    sup._child_command = lambda: child_cmd  # type: ignore[method-assign]
    rc = sup.run()
    return rc, captured


def test_run_owns_run_full_lane_lifecycle_success(tmp_path, monkeypatch, devnull_stdio) -> None:
    sup = _supervisor(
        tmp_path,
        monkeypatch,
        ["--cli", "codex", "--provider", "openai", "--self-attribute", "--task-id", "tA"],
        {"CLAUDE_CODE_SESSION_ID": "sess-2"},
    )
    line = json.dumps(
        {
            "type": "item.completed",
            "item": {"type": "agent_message", "text": "ok\nTELEMETRY_OUTCOME: success\nTELEMETRY_SUMMARY: did it"},
        }
    )
    rc, events = _run_with_fake_child(sup, ["bash", "-c", f"printf '%s\\n' {json.dumps(line)}"])
    assert rc == 0
    types = [e["event_type"] for e in events]
    assert types[0] == "run_started"
    assert types[1] == "lane_started"
    assert "tool_started" in types
    assert types[-2] == "lane_stopped"
    assert types[-1] == "run_closed"
    stop = next(e for e in events if e["event_type"] == "lane_stopped")
    assert stop["declared_outcome"] == "success"
    assert stop["stop_reason"] == "normal_stop"
    assert stop["summary"] == "did it"
    assert stop["agent_id"] == "codex:tA"
    assert "final_message_sha256" in stop


def test_run_owns_run_failure_on_nonzero_exit(tmp_path, monkeypatch, devnull_stdio) -> None:
    sup = _supervisor(
        tmp_path,
        monkeypatch,
        ["--cli", "codex", "--provider", "openai", "--self-attribute", "--task-id", "tB"],
        {"CLAUDE_CODE_SESSION_ID": "sess-3"},
    )
    rc, events = _run_with_fake_child(sup, ["bash", "-c", "exit 7"])
    assert rc == 7  # child exit code preserved
    stop = next(e for e in events if e["event_type"] == "lane_stopped")
    assert stop["declared_outcome"] == "failure"
    assert stop["stop_reason"] == "child_exit"
    assert stop["exit_code"] == 7
    assert events[-1]["event_type"] == "run_closed"


def test_run_wrapper_path_emits_tool_events_only(tmp_path, monkeypatch, devnull_stdio) -> None:
    sup = _supervisor(
        tmp_path,
        monkeypatch,
        ["--cli", "codex", "--provider", "openai"],
        {"RUN_ID": "sess-4:1", "AGENT_ID": "abadidea0abadide", "SESSION_ID": "sess-4"},
    )
    rc, events = _run_with_fake_child(sup, ["bash", "-c", "true"])
    assert rc == 0
    types = {e["event_type"] for e in events}
    assert "tool_started" in types
    assert types.isdisjoint({"run_started", "lane_started", "lane_stopped", "run_closed"})


def test_run_command_not_found_closes_lane(tmp_path, monkeypatch, devnull_stdio) -> None:
    sup = _supervisor(
        tmp_path,
        monkeypatch,
        ["--cli", "codex", "--provider", "openai", "--self-attribute", "--task-id", "tC"],
        {"CLAUDE_CODE_SESSION_ID": "sess-5"},
    )
    captured: list[dict] = []

    class _Recorder:
        def append(self, event: dict) -> bool:
            captured.append(event)
            return True

    sup.outbox = _Recorder()  # type: ignore[assignment]
    sup._child_command = lambda: None  # type: ignore[method-assign]
    rc = sup.run()
    assert rc == 127
    types = [e["event_type"] for e in captured]
    assert "tool_failed" in types
    assert types[-1] == "run_closed"
    stop = next(e for e in captured if e["event_type"] == "lane_stopped")
    assert stop["declared_outcome"] == "failure"
    assert stop["stop_reason"] == "command_not_found"
