"""Transcript tailer and shim/transcript stream-agreement tests.

These tests build a synthetic Claude-Code subagent transcript
(``subagents/agent-<agent_id>.jsonl``) — the exact record shape observed on the
installed binary (fields ``agentId``/``type``/``timestamp``/``uuid``/``message``
with assistant ``stop_reason``) — and assert that the tailer's derived
lane-start / lane-finish records agree with the shim stream's
``tool_started`` / ``tool_completed`` timestamps and ``exit_code`` for the same
``agent_id``.
"""

from __future__ import annotations

import json
from pathlib import Path


from cc_host.events import build_event
from cc_host.transcript_tailer import (
    TailerConfig,
    TranscriptReader,
    TranscriptTailer,
    encode_project_dir,
    transcript_for,
)


AGENT_ID = "a0000000000000001"
SESSION_ID = "00000000-0000-4000-8000-000000000001"
CWD = "/workspace/example-project"

START_TS = "2026-01-01T12:00:00.000Z"
MID_TS = "2026-01-01T12:01:00.000Z"
FINISH_TS = "2026-01-01T12:02:00.000Z"


def _user_record(ts: str, uuid: str) -> dict[str, object]:
    return {
        "parentUuid": None,
        "isSidechain": True,
        "agentId": AGENT_ID,
        "type": "user",
        "message": {"role": "user", "content": "do the thing"},
        "uuid": uuid,
        "timestamp": ts,
        "sessionId": SESSION_ID,
        "cwd": CWD,
        "gitBranch": "example/test-branch",
    }


def _assistant_record(ts: str, uuid: str, stop_reason: str) -> dict[str, object]:
    return {
        "parentUuid": None,
        "isSidechain": True,
        "agentId": AGENT_ID,
        "type": "assistant",
        "message": {
            "role": "assistant",
            "model": "claude-opus",
            "content": [{"type": "text", "text": "done"}],
            "stop_reason": stop_reason,
            "usage": {"input_tokens": 10, "output_tokens": 5},
        },
        "uuid": uuid,
        "timestamp": ts,
        "sessionId": SESSION_ID,
        "cwd": CWD,
    }


def _write_transcript(root: Path, *, records: list[dict[str, object]], partial_tail: str | None = None) -> Path:
    path = transcript_for(cwd=CWD, session_id=SESSION_ID, agent_id=AGENT_ID, root=root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")
        if partial_tail is not None:
            handle.write(partial_tail)  # deliberately NO trailing newline
    return path


def _shim_event(event_type: str, ts: str, source_seq: int, **extra: object) -> dict[str, object]:
    return build_event(
        event_type=event_type,
        ts=ts,
        run_id="run-1",
        session_id=SESSION_ID,
        orchestrator="claude-code",
        agent_id=AGENT_ID,
        agent_type="codex",
        provider="openai",
        tool_run_id="tool-1",
        source="shim",
        source_instance_id="shim:tool-1",
        source_seq=source_seq,
        **extra,
    )


def test_encode_project_dir_matches_installed_layout() -> None:
    assert encode_project_dir(CWD) == "-workspace-example-project"


def test_tailer_derives_lane_window_from_transcript(tmp_path) -> None:
    root = tmp_path / "projects"
    _write_transcript(
        root,
        records=[
            _user_record(START_TS, "u1"),
            _assistant_record(MID_TS, "a1", "tool_use"),
            _user_record(MID_TS, "u2"),
            _assistant_record(FINISH_TS, "a2", "end_turn"),
        ],
    )
    tailer = TranscriptTailer(TailerConfig(root=root))
    assert tailer.discover() == 1
    assert tailer.poll_once() is True

    window = tailer.window(AGENT_ID)
    assert window is not None
    assert window.start_ts == START_TS
    assert window.finish_ts == FINISH_TS
    assert window.exit_code == 0  # end_turn -> clean
    assert window.stop_reason == "end_turn"
    assert window.record_count == 4
    tailer.close()


# --- shim tool_started ts == tailer lane-start ts (same agent_id) -----
def test_shim_tool_started_ts_agrees_with_tailer_lane_start(tmp_path) -> None:
    root = tmp_path / "projects"
    _write_transcript(
        root,
        records=[
            _user_record(START_TS, "u1"),
            _assistant_record(FINISH_TS, "a1", "end_turn"),
        ],
    )
    tailer = TranscriptTailer(TailerConfig(root=root))
    tailer.poll_once()
    window = tailer.window(AGENT_ID)
    assert window is not None

    shim_started = _shim_event("tool_started", ts=START_TS, source_seq=1)

    lane_start = window.as_lane_start()
    assert shim_started["agent_id"] == lane_start["agent_id"] == AGENT_ID
    assert shim_started["ts"] == lane_start["ts"]  # AGREEMENT
    tailer.close()


# --- shim tool_completed ts/exit_code == tailer lane-finish record ----
def test_shim_tool_completed_agrees_with_tailer_lane_finish_clean(tmp_path) -> None:
    root = tmp_path / "projects"
    _write_transcript(
        root,
        records=[
            _user_record(START_TS, "u1"),
            _assistant_record(FINISH_TS, "a1", "end_turn"),
        ],
    )
    tailer = TranscriptTailer(TailerConfig(root=root))
    tailer.poll_once()
    window = tailer.window(AGENT_ID)
    assert window is not None

    shim_completed = _shim_event("tool_completed", ts=FINISH_TS, source_seq=2, exit_code=0)

    lane_finish = window.as_lane_finish()
    assert shim_completed["agent_id"] == lane_finish["agent_id"] == AGENT_ID
    assert shim_completed["ts"] == lane_finish["ts"]  # AGREEMENT on finish ts
    assert shim_completed["exit_code"] == lane_finish["exit_code"] == 0  # AGREEMENT on exit_code
    tailer.close()


def test_shim_tool_failed_agrees_with_tailer_lane_finish_nonclean(tmp_path) -> None:
    root = tmp_path / "projects"
    _write_transcript(
        root,
        records=[
            _user_record(START_TS, "u1"),
            _assistant_record(FINISH_TS, "a1", "max_tokens"),
        ],
    )
    tailer = TranscriptTailer(TailerConfig(root=root))
    tailer.poll_once()
    window = tailer.window(AGENT_ID)
    assert window is not None

    shim_failed = _shim_event("tool_failed", ts=FINISH_TS, source_seq=2, exit_code=1)

    lane_finish = window.as_lane_finish()
    assert shim_failed["ts"] == lane_finish["ts"]
    assert shim_failed["exit_code"] == lane_finish["exit_code"] == 1  # non-clean stop -> 1
    tailer.close()


# ---  robustness: partial JSONL trailing line is held, not mis-parsed --
def test_partial_trailing_jsonl_is_not_consumed_until_complete(tmp_path) -> None:
    root = tmp_path / "projects"
    path = _write_transcript(
        root,
        records=[_user_record(START_TS, "u1")],
        partial_tail='{"type":"assist',  # torn half-line, no newline
    )
    reader = TranscriptReader(path, AGENT_ID)
    reader.poll()
    assert reader.window.start_ts == START_TS
    assert reader.window.finish_ts == START_TS  # partial line not yet counted
    assert reader.window.record_count == 1

    # complete the torn line + append the real assistant record.
    with path.open("a", encoding="utf-8") as handle:
        handle.write('ant","timestamp":"' + MID_TS + '","uuid":"x","agentId":"' + AGENT_ID + '"}\n')
        handle.write(json.dumps(_assistant_record(FINISH_TS, "a1", "end_turn")) + "\n")
    reader.poll()
    assert reader.window.finish_ts == FINISH_TS
    assert reader.window.exit_code == 0
    assert reader.window.record_count == 3


# ---  duplicate suppression by uuid ------------------------------------
def test_duplicate_uuid_records_are_suppressed(tmp_path) -> None:
    root = tmp_path / "projects"
    path = _write_transcript(root, records=[_user_record(START_TS, "u1")])
    reader = TranscriptReader(path, AGENT_ID)
    reader.poll()
    # append the SAME uuid again (replay) plus a genuinely new record.
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(_user_record(MID_TS, "u1")) + "\n")  # dup uuid
        handle.write(json.dumps(_assistant_record(FINISH_TS, "a1", "end_turn")) + "\n")
    reader.poll()
    assert reader.window.record_count == 2  # dup not counted
    assert reader.window.finish_ts == FINISH_TS


# ---  file-not-found until first write ---------------------------------
def test_reader_tolerates_missing_file_then_late_first_write(tmp_path) -> None:
    root = tmp_path / "projects"
    path = transcript_for(cwd=CWD, session_id=SESSION_ID, agent_id=AGENT_ID, root=root)
    path.parent.mkdir(parents=True, exist_ok=True)
    reader = TranscriptReader(path, AGENT_ID)
    assert reader.poll() is False  # no file yet
    assert reader.window.start_ts is None
    path.write_text(json.dumps(_user_record(START_TS, "u1")) + "\n", encoding="utf-8")
    assert reader.poll() is True
    assert reader.window.start_ts == START_TS


# ---  reconciliation flips lane.json transcript_status off 'pending' ----
def test_reconcile_flips_lane_status_off_pending(tmp_path) -> None:
    root = tmp_path / "projects"
    _write_transcript(
        root,
        records=[
            _user_record(START_TS, "u1"),
            _assistant_record(FINISH_TS, "a1", "end_turn"),
        ],
    )
    runs_dir = tmp_path / "runs"
    lane_dir = runs_dir / "run-1" / "agents" / AGENT_ID
    lane_dir.mkdir(parents=True)
    lane_path = lane_dir / "lane.json"
    lane_path.write_text(
        json.dumps({"agent_id": AGENT_ID, "run_id": "run-1", "transcript_status": "pending"}),
        encoding="utf-8",
    )

    tailer = TranscriptTailer(TailerConfig(root=root, lane_state_dir=runs_dir))
    transcript_path = transcript_for(cwd=CWD, session_id=SESSION_ID, agent_id=AGENT_ID, root=root)
    window = tailer.reconcile(
        agent_id=AGENT_ID,
        agent_transcript_path=str(transcript_path),
        run_id="run-1",
    )
    assert window is not None and window.reconciled is True

    updated = json.loads(lane_path.read_text(encoding="utf-8"))
    assert updated["transcript_status"] == "reconciled"
    assert updated["transcript_status"] != "pending"
    assert updated["transcript_start_ts"] == START_TS
    assert updated["transcript_finish_ts"] == FINISH_TS
    assert updated["transcript_exit_code"] == 0
    tailer.close()
