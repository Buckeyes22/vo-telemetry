"""Transcript tailer.

Each Claude-Code subagent writes its own transcript at
``~/.claude/projects/<encoded-cwd>/<session-id>/subagents/agent-<agent_id>.jsonl``.
``agent_transcript_path`` is NOT delivered at ``SubagentStart`` — only at
``SubagentStop`` — so the tailer must discover the file late, tolerate it not
existing until first write, handle partial trailing JSONL, suppress duplicate
records by ``uuid``, and reconcile against ``SubagentStop``.

The tailer is *secondary* to the shim stream for child progress. Its job here is
to produce, per ``agent_id``, two authoritative records derived purely from the
transcript:

* **lane-start**  — timestamp of the first transcript record.
* **lane-finish** — timestamp of the last transcript record, plus an
  ``exit_code`` derived from the terminal assistant ``stop_reason``.

These are the records the stream-agreement tests compare against the shim stream's
``tool_started`` / ``tool_completed`` timestamps and exit code. The reducer must
still render a lane that has shim heartbeats but *no* transcript yet — hence the
``pending`` state on the lane file until the tailer reconciles it.

inotify is used when available (via ``ctypes`` against libc; no third-party
dependency) and a polling loop is always run as a fallback / secondary net.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import json
import os
import select
import struct
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .paths import telemetry_home
from .util import atomic_write_json, rfc3339, utc_now


# ---------------------------------------------------------------------------
# Transcript record / lane-window model
# ---------------------------------------------------------------------------

# stop_reason values that we treat as a non-clean finish -> non-zero exit_code.
# `end_turn` and `stop_sequence` are clean completions; everything else
# (`max_tokens`, `refusal`, `error`, ...) is reconciled as a failure.
_CLEAN_STOP_REASONS = {"end_turn", "stop_sequence", "tool_use"}


@dataclass
class LaneWindow:
    """Authoritative lane-start / lane-finish window derived from a transcript."""

    agent_id: str
    transcript_path: str
    start_ts: str | None = None
    finish_ts: str | None = None
    record_count: int = 0
    stop_reason: str | None = None
    exit_code: int | None = None
    session_id: str | None = None
    cwd: str | None = None
    git_branch: str | None = None
    reconciled: bool = False

    def as_lane_start(self) -> dict[str, Any]:
        return {
            "kind": "lane-start",
            "agent_id": self.agent_id,
            "ts": self.start_ts,
            "source": "transcript-tailer",
        }

    def as_lane_finish(self) -> dict[str, Any]:
        return {
            "kind": "lane-finish",
            "agent_id": self.agent_id,
            "ts": self.finish_ts,
            "exit_code": self.exit_code,
            "stop_reason": self.stop_reason,
            "source": "transcript-tailer",
        }


def _record_exit_code(record: dict[str, Any]) -> int | None:
    """Derive an exit code from a terminal assistant record, else None."""
    if record.get("type") != "assistant":
        return None
    message = record.get("message")
    if not isinstance(message, dict):
        return None
    stop_reason = message.get("stop_reason")
    if stop_reason is None:
        return None
    return 0 if stop_reason in _CLEAN_STOP_REASONS else 1


def _record_stop_reason(record: dict[str, Any]) -> str | None:
    if record.get("type") != "assistant":
        return None
    message = record.get("message")
    if isinstance(message, dict):
        sr = message.get("stop_reason")
        if isinstance(sr, str):
            return sr
    return None


class TranscriptReader:
    """Incremental, partial-JSONL-tolerant reader of one ``agent-<id>.jsonl``.

    Tracks the byte offset already consumed and a residual buffer for a partial
    trailing line, so re-reads after appends never reprocess or mis-parse a
    half-written record. Duplicate records are suppressed by their ``uuid``.
    """

    def __init__(self, path: Path, agent_id: str):
        self.path = path
        self.agent_id = agent_id
        self._offset = 0
        self._residual = ""
        self._seen_uuids: set[str] = set()
        self.window = LaneWindow(agent_id=agent_id, transcript_path=str(path))

    def poll(self) -> bool:
        """Consume any newly-written complete records. Returns True if the window changed."""
        try:
            size = self.path.stat().st_size
        except FileNotFoundError:
            # file-not-found until first write — perfectly normal, not an error.
            return False
        if size < self._offset:
            # truncation/rotation: restart from the top.
            self._offset = 0
            self._residual = ""
            self._seen_uuids.clear()
            self.window = LaneWindow(agent_id=self.agent_id, transcript_path=str(self.path))
        if size == self._offset and not self._residual:
            return False
        try:
            with self.path.open("rb") as handle:
                handle.seek(self._offset)
                chunk = handle.read()
        except FileNotFoundError:
            return False
        self._offset += len(chunk)
        text = self._residual + chunk.decode("utf-8", errors="replace")
        # Keep an incomplete trailing line (no newline yet) as residual.
        if text.endswith("\n"):
            lines = text.split("\n")
            self._residual = ""
        else:
            lines = text.split("\n")
            self._residual = lines.pop()  # partial trailing line, retry next poll
        changed = False
        for line in lines:
            if self._consume_line(line):
                changed = True
        return changed

    def _consume_line(self, line: str) -> bool:
        stripped = line.strip()
        if not stripped:
            return False
        try:
            record = json.loads(stripped)
        except json.JSONDecodeError:
            # A complete-but-corrupt line: skip it (partial lines are held as
            # residual, so this is genuine corruption, not a torn write).
            return False
        if not isinstance(record, dict):
            return False
        uuid = record.get("uuid")
        if isinstance(uuid, str):
            if uuid in self._seen_uuids:
                return False
            self._seen_uuids.add(uuid)
        ts = record.get("timestamp")
        if not isinstance(ts, str) or not ts:
            return False

        win = self.window
        if win.start_ts is None:
            win.start_ts = ts
            win.session_id = _opt_str(record.get("sessionId"))
            win.cwd = _opt_str(record.get("cwd"))
            win.git_branch = _opt_str(record.get("gitBranch"))
        # last record wins for finish.
        win.finish_ts = ts
        win.record_count += 1
        exit_code = _record_exit_code(record)
        if exit_code is not None:
            win.exit_code = exit_code
            win.stop_reason = _record_stop_reason(record)
        return True


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def encode_project_dir(cwd: str) -> str:
    """Claude Code encodes a cwd into a project dir name by replacing each
    path separator with ``-`` (example encoding:
    ``/workspace/example-project`` -> ``-workspace-example-project``)."""
    return cwd.replace(os.sep, "-")


def projects_root() -> Path:
    return Path(os.getenv("CLAUDE_PROJECTS_DIR", str(Path("~/.claude/projects").expanduser()))).expanduser()


def agent_id_from_transcript(path: Path) -> str | None:
    name = path.name
    if name.startswith("agent-") and name.endswith(".jsonl"):
        return name[len("agent-") : -len(".jsonl")]
    return None


def discover_subagent_dirs(root: Path | None = None) -> list[Path]:
    """Find every ``subagents`` directory under the projects root.

    The canonical layout is ``<project>/<session-id>/subagents`` (two levels),
    but resumed sessions nest deeper, so we match ``subagents`` at any depth
    (including resumed-session layouts). ``rglob`` is bounded by
    the projects tree and only matches the literal ``subagents`` directory name.
    """
    root = root or projects_root()
    if not root.exists():
        return []
    return sorted(p for p in root.rglob("subagents") if p.is_dir())


def transcript_for(*, cwd: str, session_id: str, agent_id: str, root: Path | None = None) -> Path:
    root = root or projects_root()
    return root / encode_project_dir(cwd) / session_id / "subagents" / f"agent-{agent_id}.jsonl"


# ---------------------------------------------------------------------------
# inotify (ctypes, no third-party dependency) with polling fallback
# ---------------------------------------------------------------------------

_IN_CREATE = 0x00000100
_IN_MODIFY = 0x00000002
_IN_MOVED_TO = 0x00000080
_EVENT_HEADER = struct.Struct("iIII")  # wd, mask, cookie, len


class _Inotify:
    """Thin ctypes wrapper over libc inotify. Returns None-capable on platforms
    without inotify so the caller transparently falls back to polling."""

    def __init__(self) -> None:
        self._fd = -1
        self._libc = None
        libc_name = ctypes.util.find_library("c")
        if not libc_name:
            return
        try:
            libc = ctypes.CDLL(libc_name, use_errno=True)
            fd = libc.inotify_init1(os.O_NONBLOCK)
            if fd < 0:
                return
            self._libc = libc
            self._fd = fd
        except Exception:
            self._fd = -1
            self._libc = None

    @property
    def available(self) -> bool:
        return self._fd >= 0 and self._libc is not None

    def add_watch(self, path: str, mask: int) -> int:
        libc = self._libc
        if self._fd < 0 or libc is None:
            return -1
        return int(libc.inotify_add_watch(self._fd, path.encode(), mask))

    def wait(self, timeout: float) -> bool:
        """Block up to ``timeout`` seconds for any event; drain and discard them
        (we re-poll the actual files, so the event payload only signals 'wake up')."""
        if not self.available:
            return False
        ready, _, _ = select.select([self._fd], [], [], timeout)
        if not ready:
            return False
        try:
            data = os.read(self._fd, 4096)
        except (BlockingIOError, OSError):
            return False
        return len(data) > 0

    def close(self) -> None:
        if self._fd >= 0:
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = -1


# ---------------------------------------------------------------------------
# Tailer
# ---------------------------------------------------------------------------


@dataclass
class TailerConfig:
    poll_interval: float = 1.0
    root: Path | None = None
    lane_state_dir: Path | None = None  # where lane.json files live (runs_dir)


class TranscriptTailer:
    """Watches every ``subagents`` dir, late-discovers new transcripts, tails
    them, and exposes a per-agent :class:`LaneWindow`. Reconciliation against a
    ``SubagentStop`` (which carries the authoritative ``agent_transcript_path``)
    flips the lane file's ``transcript_status`` off ``pending``."""

    def __init__(self, config: TailerConfig | None = None):
        self.config = config or TailerConfig()
        self.root = self.config.root or projects_root()
        self._readers: dict[str, TranscriptReader] = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._inotify = _Inotify()
        self._watched_dirs: set[str] = set()

    # -- discovery + polling ------------------------------------------------

    def discover(self) -> int:
        """Late-discover any not-yet-tracked ``agent-<id>.jsonl`` files. Returns
        the number of newly discovered transcripts."""
        new = 0
        with self._lock:
            for sub_dir in discover_subagent_dirs(self.root):
                self._add_dir_watch(sub_dir)
                # transcripts live directly under subagents/, and some harnesses
                # nest them under subagents/workflows/wf_*/ (observed on-box), so
                # search recursively within each subagents dir.
                for transcript in sorted(sub_dir.rglob("agent-*.jsonl")):
                    agent_id = agent_id_from_transcript(transcript)
                    if agent_id is None or agent_id in self._readers:
                        continue
                    self._readers[agent_id] = TranscriptReader(transcript, agent_id)
                    self._add_dir_watch(transcript.parent)
                    new += 1
        return new

    def _add_dir_watch(self, sub_dir: Path) -> None:
        key = str(sub_dir)
        if key in self._watched_dirs:
            return
        if self._inotify.available:
            self._inotify.add_watch(key, _IN_CREATE | _IN_MODIFY | _IN_MOVED_TO)
        self._watched_dirs.add(key)

    def poll_once(self) -> bool:
        """Discover new transcripts and drain pending appends. Returns True if any
        window changed."""
        self.discover()
        changed = False
        with self._lock:
            readers = list(self._readers.values())
        for reader in readers:
            if reader.poll():
                changed = True
        return changed

    # -- introspection ------------------------------------------------------

    def window(self, agent_id: str) -> LaneWindow | None:
        with self._lock:
            reader = self._readers.get(agent_id)
            return reader.window if reader else None

    def windows(self) -> dict[str, LaneWindow]:
        with self._lock:
            return {aid: r.window for aid, r in self._readers.items()}

    # -- reconciliation against SubagentStop --------------------------------

    def reconcile(
        self,
        *,
        agent_id: str,
        agent_transcript_path: str | None,
        run_id: str | None = None,
        lane_path: Path | None = None,
    ) -> LaneWindow | None:
        """Reconcile a lane against the authoritative ``agent_transcript_path``
        delivered at ``SubagentStop``. Drains the transcript to end, marks the
        window reconciled, and flips the lane file's ``transcript_status``."""
        with self._lock:
            reader = self._readers.get(agent_id)
            if reader is None and agent_transcript_path:
                path = Path(agent_transcript_path)
                reader = TranscriptReader(path, agent_id)
                self._readers[agent_id] = reader
            if reader is None:
                window = LaneWindow(agent_id=agent_id, transcript_path=agent_transcript_path or "")
            else:
                # ensure we've drained to the current end of file.
                reader.poll()
                window = reader.window
                window.reconciled = True

        status = self._status_for(window)
        if lane_path is None and run_id is not None:
            lane_path = self._lane_path(run_id, agent_id)
        if lane_path is not None:
            self._update_lane_file(lane_path, window, status)
        return window

    def _status_for(self, window: LaneWindow) -> str:
        if window.finish_ts is not None:
            return "reconciled"
        if window.start_ts is not None:
            return "tailing"
        return "missing"

    def _lane_path(self, run_id: str, agent_id: str) -> Path:
        base = self.config.lane_state_dir or (telemetry_home() / "runs")
        from .util import safe_path_component

        return base / safe_path_component(run_id) / "agents" / safe_path_component(agent_id) / "lane.json"

    def _update_lane_file(self, lane_path: Path, window: LaneWindow, status: str) -> None:
        try:
            existing = json.loads(lane_path.read_text(encoding="utf-8")) if lane_path.exists() else {}
        except Exception:
            existing = {}
        if not isinstance(existing, dict):
            existing = {}
        existing["transcript_status"] = status
        existing["transcript_path"] = window.transcript_path
        if window.start_ts is not None:
            existing["transcript_start_ts"] = window.start_ts
        if window.finish_ts is not None:
            existing["transcript_finish_ts"] = window.finish_ts
        if window.exit_code is not None:
            existing["transcript_exit_code"] = window.exit_code
        if window.stop_reason is not None:
            existing["transcript_stop_reason"] = window.stop_reason
        existing["transcript_record_count"] = window.record_count
        existing["transcript_reconciled_at"] = rfc3339(utc_now())
        try:
            atomic_write_json(lane_path, existing, mode=0o600)
        except Exception:
            # never let a tailer write failure crash the producer.
            pass

    # -- run loop -----------------------------------------------------------

    def run_forever(self) -> None:
        self.discover()
        while not self._stop.is_set():
            self.poll_once()
            if self._inotify.available:
                # wake early on inotify activity, else fall through on timeout.
                self._inotify.wait(self.config.poll_interval)
            else:
                self._stop.wait(self.config.poll_interval)

    def stop(self) -> None:
        self._stop.set()

    def close(self) -> None:
        self._inotify.close()


def _opt_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="vo-telemetry subagent transcript tailer ")
    parser.add_argument("--once", action="store_true", help="discover + drain once, print windows, exit")
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--root", default=None, help="override projects root (default ~/.claude/projects)")
    args = parser.parse_args(argv)

    config = TailerConfig(
        poll_interval=args.poll_interval,
        root=Path(args.root).expanduser() if args.root else None,
    )
    tailer = TranscriptTailer(config)
    if args.once:
        tailer.poll_once()
        out = {
            aid: {
                "start_ts": w.start_ts,
                "finish_ts": w.finish_ts,
                "exit_code": w.exit_code,
                "stop_reason": w.stop_reason,
                "records": w.record_count,
                "status": tailer._status_for(w),
            }
            for aid, w in sorted(tailer.windows().items())
        }
        print(json.dumps({"discovered": len(out), "windows": out}, indent=2))
        tailer.close()
        return 0
    try:
        tailer.run_forever()
    finally:
        tailer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
