from __future__ import annotations

import hashlib
import json
import os
import shlex
import sys
from dataclasses import dataclass
from pathlib import Path

from .paths import runs_dir
from .util import atomic_write_text, ensure_private_dir, safe_path_component


@dataclass(frozen=True)
class WrapperSet:
    agent_dir: Path
    bin_dir: Path
    codex: Path
    opencode_glm: Path
    audit_log: Path
    hashes: dict[str, str]


def generate_wrappers(
    *,
    run_id: str,
    agent_id: str,
    agent_type: str,
    model: str | None = None,
) -> WrapperSet:
    agent_dir = runs_dir() / safe_path_component(run_id) / "agents" / safe_path_component(agent_id)
    bin_dir = agent_dir / "bin"
    ensure_private_dir(agent_dir)
    ensure_private_dir(bin_dir)
    _reject_symlinks(bin_dir)

    python = Path(sys.executable).resolve(strict=True)
    shim = Path(__file__).resolve(strict=True).parent / "shim.py"
    if shim.is_symlink() or not shim.exists():
        raise RuntimeError(f"shim path is invalid: {shim}")
    if python.is_symlink() or not os.access(python, os.X_OK):
        raise RuntimeError(f"python executable is invalid: {python}")
    # The shim uses package-relative imports (`from .events import ...`); it MUST be launched as a
    # module (`python -m cc_host.shim`) with the package root on PYTHONPATH, exactly like the
    # installed hooks (install.py) and the forwarder unit. Running it as a bare script
    # (`python shim.py`) raises "attempted relative import with no known parent package" and the
    # child CLI never starts, so tool_started is never written. package_root is the dir
    # that *contains* the cc_host package (i.e. src/cc-host).
    package_root = shim.parent.parent
    if not (package_root / "cc_host" / "__init__.py").exists():
        raise RuntimeError(f"cc_host package root is invalid: {package_root}")

    common = {
        "RUN_ID": run_id,
        "AGENT_ID": agent_id,
        "ORCHESTRATOR": "claude-code",
        "AGENT_TYPE": agent_type,
    }
    if model:
        common["MODEL"] = model

    codex = bin_dir / "codex"
    opencode_glm = bin_dir / "opencode-glm"
    hashes = {
        "codex": _write_wrapper(
            codex,
            python=python,
            package_root=package_root,
            env={**common, "PROVIDER": "openai"},
            args=["--cli", "codex", "--provider", "openai"],
        ),
        "opencode-glm": _write_wrapper(
            opencode_glm,
            python=python,
            package_root=package_root,
            env={**common, "PROVIDER": "glm"},
            args=["--cli", "opencode", "--provider", "glm"],
        ),
    }
    audit_log = agent_dir / "wrapper-audit.jsonl"
    _append_audit(audit_log, run_id, agent_id, hashes)
    return WrapperSet(
        agent_dir=agent_dir,
        bin_dir=bin_dir,
        codex=codex,
        opencode_glm=opencode_glm,
        audit_log=audit_log,
        hashes=hashes,
    )


def additional_context(wrappers: WrapperSet) -> str:
    return "\n".join(
        [
            "Use these telemetry wrappers for subagent CLI calls; do not invoke bare codex or opencode directly:",
            f"- Codex: {wrappers.codex}",
            f"- opencode GLM: {wrappers.opencode_glm}",
            "",
            "At the end of your work include these markers:",
            "TELEMETRY_OUTCOME: success | failure | partial",
            "TELEMETRY_SUMMARY: <one line>",
        ]
    )


def failure_context(reason: str) -> str:
    return "\n".join(
        [
            "Telemetry wrapper generation failed. Do not invoke codex or opencode directly.",
            f"Failure: {reason}",
            "Return a concise failure summary for the orchestrator instead.",
        ]
    )


def _write_wrapper(path: Path, *, python: Path, package_root: Path, env: dict[str, str], args: list[str]) -> str:
    exports = "\n".join(f"export {key}={shlex.quote(value)}" for key, value in sorted(env.items()))
    # Prepend the package root to PYTHONPATH so `-m cc_host.shim` resolves; preserve any inherited
    # PYTHONPATH so the wrapper does not clobber a caller's environment.
    pp = shlex.quote(str(package_root))
    argv = " ".join(shlex.quote(part) for part in [str(python), "-m", "cc_host.shim", *args, "--"])
    text = f"""#!/usr/bin/env bash
set -euo pipefail
# vo-telemetry wrapper; identity is embedded at generation time.
{exports}
export PYTHONPATH={pp}${{PYTHONPATH:+:$PYTHONPATH}}
exec {argv} "$@"
"""
    atomic_write_text(path, text, mode=0o700)
    os.chmod(path, 0o700)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return digest


def _append_audit(path: Path, run_id: str, agent_id: str, hashes: dict[str, str]) -> None:
    record = json.dumps({"run_id": run_id, "agent_id": agent_id, "hashes": hashes}, sort_keys=True)
    ensure_private_dir(path.parent)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(record + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(path, 0o600)


def _reject_symlinks(path: Path) -> None:
    for item in path.iterdir():
        if item.is_symlink():
            raise RuntimeError(f"wrapper directory contains symlink: {item}")
