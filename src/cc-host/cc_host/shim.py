from __future__ import annotations

import argparse
import hashlib
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO

from .events import build_event, final_message_fields
from .outbox import Outbox
from .paths import spool_dir
from .pricing import PricingTable
from .state import git_branch
from .stream_parser import ParsedEmission, StreamParser
from .util import ensure_private_dir, rfc3339, utc_now, valid_identity

_unrestricted_warned = False


def _unrestricted() -> bool:
    """Whether to inject the sandbox/approval BYPASS flags into child CLIs.

    OFF by default — the operator must opt in per-run via CC_HOST_ALLOW_UNRESTRICTED=1, and a
    prominent warning is logged. Default behavior keeps the child sandboxed (non-interactive, but
    not unrestricted), so a permission prompt cannot hang the lane and look stuck, without disabling the sandbox.
    """
    on = os.getenv("CC_HOST_ALLOW_UNRESTRICTED", "").lower() in ("1", "true", "yes")
    global _unrestricted_warned
    if on and not _unrestricted_warned:
        print(
            "vo-telemetry shim WARNING: CC_HOST_ALLOW_UNRESTRICTED=1 — injecting sandbox/approval BYPASS "
            "(--dangerously-bypass-approvals-and-sandbox / --dangerously-skip-permissions). The child "
            "CLI runs with NO sandbox and NO approvals. Unset to run sandboxed.",
            file=sys.stderr,
        )
        _unrestricted_warned = True
    return on


def _has_cli_option(args: list[str], *names: str) -> bool:
    for arg in args:
        if arg in names:
            return True
        for name in names:
            if name.startswith("--") and arg.startswith(f"{name}="):
                return True
    return False


@dataclass
class Identity:
    run_id: str
    session_id: str
    agent_id: str
    quarantine: bool
    identity_error: str | None


class BoundedSpool:
    def __init__(self, path: Path, max_bytes: int):
        ensure_private_dir(path.parent)
        self.path = path
        self.max_bytes = max_bytes
        self._handle = path.open("ab")
        self._written = path.stat().st_size if path.exists() else 0
        self._truncated = False
        os.chmod(path, 0o600)
        self._lock = threading.Lock()

    def write(self, prefix: bytes, data: bytes) -> None:
        with self._lock:
            if self._written >= self.max_bytes:
                if not self._truncated:
                    marker = b"\n[vo-telemetry spool truncated]\n"
                    self._handle.write(marker)
                    self._written += len(marker)
                    self._truncated = True
                return
            chunk = prefix + data
            remaining = self.max_bytes - self._written
            self._handle.write(chunk[:remaining])
            self._written += min(len(chunk), remaining)
            self._handle.flush()

    def close(self) -> None:
        with self._lock:
            self._handle.close()


class ShimSupervisor:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.tool_run_id = str(uuid.uuid4())
        self.source_instance_id = f"shim:{self.tool_run_id}"
        self.pid = os.getpid()
        # Stable id used to mint a dedicated run/lane in self-attribute mode. The bg launcher passes
        # the /tmp/codex-tasks/<id> basename (human-traceable); otherwise derive from tool_run_id.
        self.task_id = (getattr(args, "task_id", None) or "").strip() or _short_hash(self.tool_run_id)
        # Own the run/lane lifecycle when launched as a standalone Bash shim (no SubagentStart wrapper
        # injected RUN_ID/AGENT_ID). Under a wrapper, the hooks own the lane and we emit only tool_*.
        self.owns_run = bool(getattr(args, "self_attribute", False)) or not (
            os.getenv("RUN_ID", "").strip() or os.getenv("AGENT_ID", "").strip()
        )
        self.identity = self._identity()
        self.outbox = Outbox(max_events=int(os.getenv("CC_HOST_OUTBOX_MAX_EVENTS", "10000")))
        self.pricing = PricingTable.load()
        self.parser = StreamParser(args.cli, self.pricing)
        self.source_seq = 0
        self.seq_lock = threading.Lock()
        self.stop_heartbeats = threading.Event()
        self.emissions: queue.Queue[ParsedEmission] = queue.Queue()

    def run(self) -> int:
        self._child_killed_by_budget = False
        if self.owns_run:
            # Dedicated run + lane for this standalone shim task.
            self._emit("run_started", **self._run_fields())
            self._emit("lane_started", **self._lane_fields())
        self._emit("tool_started")  # _emit already injects pid + model (don't pass them again — TypeError)
        command = self._child_command()
        if command is None:
            self._emit(
                "tool_failed",
                exit_code=127,
                error={"class": "command_not_found", "message": f"{self.args.cli} executable not found"},
                **self._terminal_fields(),
            )
            self._close_lane(127, stop_reason_override="command_not_found")
            return 127

        child_env = self._child_env()
        spool = BoundedSpool(self._spool_path(), int(os.getenv("CC_HOST_SPOOL_MAX_BYTES", "10485760")))
        process = subprocess.Popen(
            command,
            stdin=sys.stdin.buffer,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=child_env,
            bufsize=0,
        )
        heartbeat = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat.start()
        self._child_killed_by_budget = False
        budget_thread = self._start_child_budget(process)
        threads = [
            threading.Thread(
                target=self._reader,
                args=(process.stdout, sys.stdout.buffer, b"stdout: ", spool, True),
                daemon=True,
            ),
            threading.Thread(
                target=self._reader,
                args=(process.stderr, sys.stderr.buffer, b"stderr: ", spool, False),
                daemon=True,
            ),
        ]
        for thread in threads:
            thread.start()
        return_code = process.wait()
        self.stop_heartbeats.set()  # also stops the budget watchdog's wait()
        for thread in threads:
            thread.join(timeout=2)
        heartbeat.join(timeout=2)
        if budget_thread is not None:
            budget_thread.join(timeout=2)
        self._drain_emissions()
        spool.close()
        terminal = "tool_completed" if return_code == 0 else "tool_failed"
        fields = self._terminal_fields()
        if return_code != 0:
            if self._child_killed_by_budget:
                fields["error"] = {
                    "class": "child_budget_exceeded",
                    "message": f"child terminated after CC_HOST_CHILD_TIMEOUT_SEC budget; exit {return_code}",
                }
            else:
                fields["error"] = {"class": "child_exit", "message": f"child exited with {return_code}"}
        self._emit(terminal, exit_code=return_code, **fields)
        self._close_lane(return_code)
        return int(return_code)

    def _start_child_budget(self, process: "subprocess.Popen[bytes]") -> threading.Thread | None:
        """Optional hard wall-clock budget for the CHILD CLI. If CC_HOST_CHILD_TIMEOUT_SEC is set and elapses before the child exits,
        SIGTERM (then SIGKILL) the child so process.wait() returns and we emit a real tool_failed
        terminal event. The shim itself is never killed, so the never-drop terminal is preserved."""
        raw = os.getenv("CC_HOST_CHILD_TIMEOUT_SEC", "").strip()
        if not raw:
            return None
        try:
            budget = float(raw)
        except ValueError:
            return None
        if budget <= 0:
            return None

        def watchdog() -> None:
            # stop_heartbeats is set the instant the child exits; reuse it as the cancel signal.
            if self.stop_heartbeats.wait(budget):
                return  # child already exited cleanly
            if process.poll() is not None:
                return
            self._child_killed_by_budget = True
            try:
                process.terminate()
            except ProcessLookupError:
                return
            for _ in range(20):  # up to ~2s grace, then hard kill
                if process.poll() is not None:
                    return
                time.sleep(0.1)
            try:
                process.kill()
            except ProcessLookupError:
                return

        thread = threading.Thread(target=watchdog, daemon=True)
        thread.start()
        return thread

    def _reader(
        self,
        stream: BinaryIO | None,
        passthrough: BinaryIO,
        prefix: bytes,
        spool: BoundedSpool,
        parse: bool,
    ) -> None:
        if stream is None:
            return
        while True:
            line = stream.readline()
            if not line:
                break
            spool.write(prefix, line)
            try:
                passthrough.write(line)
                passthrough.flush()
            except BrokenPipeError:
                pass
            if parse:
                try:
                    for emission in self.parser.consume_line(line.decode("utf-8", errors="replace")):
                        self.emissions.put(emission)
                except Exception:
                    continue
                self._drain_emissions()

    def _drain_emissions(self) -> None:
        while True:
            try:
                emission = self.emissions.get_nowait()
            except queue.Empty:
                return
            self._emit(emission.event_type, **emission.fields)

    def _heartbeat_loop(self) -> None:
        interval = float(os.getenv("CC_HOST_HEARTBEAT_SEC", "15"))
        while not self.stop_heartbeats.wait(interval):
            self._emit("tool_heartbeat")

    def _emit(self, event_type: str, **fields: object) -> None:
        with self.seq_lock:
            self.source_seq += 1
            seq = self.source_seq
        event = build_event(
            event_type=event_type,
            run_id=self.identity.run_id,
            session_id=self.identity.session_id,
            orchestrator="claude-code",
            agent_id=self.identity.agent_id,
            quarantine=self.identity.quarantine,
            identity_error=self.identity.identity_error,
            agent_type=os.getenv("AGENT_TYPE") or self.args.cli,
            provider=self.args.provider or os.getenv("PROVIDER") or "unknown",
            model=self.parser.state.model or os.getenv("MODEL") or self.args.model,
            tool_run_id=self.tool_run_id,
            trace_id=_trace_ids().get("trace_id"),
            span_id=_trace_ids().get("span_id"),
            source="shim",
            source_instance_id=self.source_instance_id,
            source_seq=seq,
            pid=self.pid,
            **fields,
        )
        try:
            self.outbox.append(event)
        except Exception as exc:
            _log_shim_error(f"outbox append failed: {exc}")

    def _terminal_fields(self) -> dict[str, object]:
        fields: dict[str, object] = {"tokens_total": self.parser.state.tokens_total}
        fields.update(self.parser.terminal_cost_fields())
        return fields

    def _run_fields(self) -> dict[str, object]:
        cwd = os.getcwd()
        return {"cwd": cwd, "project": Path(cwd).name, "operator_label": f"shim:{self.args.cli}"}

    def _lane_fields(self) -> dict[str, object]:
        cwd = os.getcwd()
        return {"worktree": cwd, "branch": git_branch(cwd)}

    def _close_lane(self, return_code: int, stop_reason_override: str | None = None) -> None:
        """Emit lane_stopped + run_closed when this shim owns the run. No-op under a SubagentStart
        wrapper (the hooks own the lane there). Never regresses a terminal."""
        if not self.owns_run:
            return
        outcome, summary, stop_reason = self._declared_outcome(return_code)
        if stop_reason_override:
            stop_reason, outcome = stop_reason_override, "failure"
        fields: dict[str, object] = {
            "declared_outcome": outcome,
            "stop_reason": stop_reason,
            "exit_code": return_code,
            **self._lane_fields(),
        }
        if summary:
            fields["summary"] = summary
        final_msg = self.parser.state.final_message
        if final_msg:
            fields.update(final_message_fields(final_msg))
        self._emit("lane_stopped", **fields)
        self._emit("run_closed", **self._run_fields())

    def _declared_outcome(self, return_code: int) -> tuple[str, str | None, str]:
        """Honest mapping: codex/opencode don't emit TELEMETRY_OUTCOME, so derive from the real exit
        code; an inline TELEMETRY_OUTCOME marker in the final message (if any) wins."""
        outcome, summary = _parse_telemetry_markers(self.parser.state.final_message)
        if getattr(self, "_child_killed_by_budget", False):
            stop_reason = "child_budget_exceeded"
        elif return_code == 0:
            stop_reason = "normal_stop"
        else:
            stop_reason = "child_exit"
        if outcome == "unknown":
            outcome = "success" if return_code == 0 else "failure"
        return outcome, summary, stop_reason

    def _identity(self) -> Identity:
        run_id = os.getenv("RUN_ID", "").strip()
        session_id = (
            os.getenv("SESSION_ID", "").strip()
            or os.getenv("CLAUDE_CODE_SESSION_ID", "").strip()
            or _session_from_run(run_id)
        )
        agent_id = os.getenv("AGENT_ID", "").strip()
        provider = self.args.provider or os.getenv("PROVIDER", "").strip()
        # Self-attribute mode: mint a DEDICATED run + lane for this task (no SubagentStart wrapper to
        # inject identity). Env RUN_ID/AGENT_ID, when present (wrapper path), always win — unchanged.
        if self.owns_run and not run_id:
            if not session_id:
                session_id = "detached"
            run_id = f"{session_id}:shim:{self.task_id}"
            if not valid_identity(agent_id):
                minted = f"{self.args.cli}:{self.task_id}"
                if valid_identity(minted):
                    agent_id = minted
        errors: list[str] = []
        if not run_id:
            errors.append("missing_run_id")
            run_id = f"quarantine-run:{_short_hash(self.source_instance_id + str(self.pid))}"
        if not session_id:
            session_id = "unknown-session"
        if not valid_identity(agent_id):
            errors.append("missing_or_malformed_agent_id")
        if not provider:
            errors.append("missing_provider")
        if errors:
            quarantine_agent = _quarantine_agent_id(self.source_instance_id, self.tool_run_id, self.pid)
            return Identity(
                run_id=run_id,
                session_id=session_id,
                agent_id=quarantine_agent,
                quarantine=True,
                identity_error=",".join(errors),
            )
        return Identity(run_id=run_id, session_id=session_id, agent_id=agent_id, quarantine=False, identity_error=None)

    def _child_env(self) -> dict[str, str]:
        env = os.environ.copy()
        if self.args.cli == "qwen":
            # Qwen supports OPENAI_API_KEY for OpenAI-compatible authentication.
            # Translate our scoped variable only in the child environment.
            key = os.getenv("QWEN_OPENAI_API_KEY", "").strip()
            if key:
                env["OPENAI_API_KEY"] = key
        endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
        env.update(
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": endpoint,
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": f"{endpoint.rstrip('/')}/v1/traces",
                "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": f"{endpoint.rstrip('/')}/v1/metrics",
                "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": f"{endpoint.rstrip('/')}/v1/logs",
                "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
                "OTEL_TRACES_EXPORTER": "otlp",
                "OTEL_METRICS_EXPORTER": "otlp",
                "OTEL_LOGS_EXPORTER": "otlp",
            }
        )
        return env

    def _child_command(self) -> list[str] | None:
        if self.args.cli == "codex":
            binary = shutil.which(os.getenv("CODEX_BIN", "codex"))
            if binary is None:
                return None
            child_args = list(self.args.child_args)
            if not child_args or child_args[0] != "exec":
                child_args.insert(0, "exec")
            insert_at = 1
            if "--json" not in child_args:
                child_args.insert(insert_at, "--json")
                insert_at += 1
            # `codex exec` is non-interactive by nature (no approval prompt to freeze on).
            # Flags verified against codex-cli 0.135.0: `exec` takes `-s/--sandbox <mode>` and
            # `--skip-git-repo-check`, and `--dangerously-bypass-approvals-and-sandbox`; there is NO
            # `--ask-for-approval` on `exec`. DEFAULT = sandboxed workspace-write; full BYPASS is opt-in
            # via CC_HOST_ALLOW_UNRESTRICTED=1. --skip-git-repo-check so exec runs outside a git repo.
            if "--skip-git-repo-check" not in child_args:
                child_args.insert(insert_at, "--skip-git-repo-check")
                insert_at += 1
            _sandbox_flags = ("--sandbox", "-s", "--full-auto", "--dangerously-bypass-approvals-and-sandbox")
            if _unrestricted():
                if "--dangerously-bypass-approvals-and-sandbox" not in child_args:
                    child_args.insert(insert_at, "--dangerously-bypass-approvals-and-sandbox")
            elif not any(a in child_args for a in _sandbox_flags):
                child_args[insert_at:insert_at] = ["--sandbox", "workspace-write"]
            return [binary, *child_args]
        if self.args.cli == "qwen":
            binary = shutil.which(os.getenv("QWEN_BIN", "qwen"))
            if binary is None:
                return None
            child_args = list(self.args.child_args)
            prefix: list[str] = []
            if not _has_cli_option(child_args, "--input-format"):
                prefix.extend(["--input-format", "text"])
            if not _has_cli_option(child_args, "--output-format", "-o"):
                prefix.extend(["--output-format", "stream-json"])
            approval_flags = ("--approval-mode", "--yolo", "-y")
            if _unrestricted() and not _has_cli_option(child_args, *approval_flags):
                prefix.extend(["--approval-mode", "yolo"])
            auth_type = os.getenv("QWEN_AUTH_TYPE", "").strip()
            if auth_type and not _has_cli_option(child_args, "--auth-type"):
                prefix.extend(["--auth-type", auth_type])
            base_url = os.getenv("QWEN_OPENAI_BASE_URL", "").strip()
            if base_url and not _has_cli_option(child_args, "--openai-base-url"):
                prefix.extend(["--openai-base-url", base_url])
            if _has_cli_option(child_args, "--openai-api-key"):
                raise ValueError("Qwen API keys must use QWEN_OPENAI_API_KEY, not command arguments")
            return [binary, *prefix, *child_args]
        binary = shutil.which(os.getenv("OPENCODE_BIN", "opencode"))
        if binary is None:
            return None
        child_args = list(self.args.child_args)
        if child_args and child_args[0] == "run":
            child_args = child_args[1:]
        prefix = ["run"]
        if "--format" not in child_args:
            prefix.extend(["--format", "json"])
        # opencode `run` is already the non-interactive subcommand. --dangerously-skip-permissions
        # fully disables its permission gate, so it is opt-in only (CC_HOST_ALLOW_UNRESTRICTED=1).
        if _unrestricted() and "--dangerously-skip-permissions" not in child_args:
            prefix.append("--dangerously-skip-permissions")
        return [binary, *prefix, *child_args]

    def _spool_path(self) -> Path:
        ensure_private_dir(spool_dir())
        return spool_dir() / f"{self.tool_run_id}.log"


def _trace_ids() -> dict[str, str]:
    traceparent = os.getenv("TRACEPARENT", "")
    parts = traceparent.split("-")
    if len(parts) >= 4 and len(parts[1]) == 32 and len(parts[2]) == 16:
        return {"trace_id": parts[1], "span_id": parts[2]}
    return {}


def _session_from_run(run_id: str) -> str:
    if ":" in run_id:
        return run_id.split(":", 1)[0]
    return os.getenv("SESSION_ID", "")


def _parse_telemetry_markers(message: str) -> tuple[str, str | None]:
    """Honor an inline TELEMETRY_OUTCOME/TELEMETRY_SUMMARY marker if the child agent emitted one
    (mirrors hooks._parse_outcome). codex/opencode normally don't, so the caller falls back to the
    exit code."""
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


def _quarantine_agent_id(source_instance_id: str, tool_run_id: str, pid: int) -> str:
    bucket = datetime.now(UTC).isoformat(timespec="minutes")
    return f"quarantine:{_short_hash('|'.join([source_instance_id, tool_run_id, str(pid), bucket]))}"


def _short_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _log_shim_error(message: str) -> None:
    ensure_private_dir(spool_dir())
    with (spool_dir() / "shim-errors.log").open("a", encoding="utf-8") as handle:
        handle.write(f"[{rfc3339(utc_now())}] {message}\n")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="vo-telemetry supervised subagent CLI shim")
    parser.add_argument("--cli", choices=["codex", "opencode", "qwen"], required=True)
    parser.add_argument("--provider", default=os.getenv("PROVIDER", "unknown"))
    parser.add_argument("--model", default=os.getenv("MODEL"))
    parser.add_argument("--allow-unattributed", action="store_true")
    parser.add_argument(
        "--self-attribute",
        action="store_true",
        help="Own the run/lane lifecycle (emit run_started/lane_started/lane_stopped/run_closed). "
        "Auto-enabled when RUN_ID/AGENT_ID env are absent (standalone Bash-launched shim).",
    )
    parser.add_argument(
        "--task-id",
        default=os.getenv("CC_HOST_TASK_ID"),
        help="Stable task id used to mint the dedicated run_id/agent_id (the bg launcher passes the "
        "codex-tasks basename so the run is human-traceable).",
    )
    parser.add_argument("child_args", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if args.child_args and args.child_args[0] == "--":
        args.child_args = args.child_args[1:]
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return ShimSupervisor(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
