"""Regression checks use fake interpreters and synthetic prompts only."""

from __future__ import annotations

import os
import stat
import subprocess
import time
from pathlib import Path

import pytest

SHIMS = Path(__file__).resolve().parents[1] / "shims"


def spy_environment(tmp_path):
    spy = tmp_path / "fake-python"
    spy.write_text(
        '#!/usr/bin/env bash\nprintf "%s\\n" "$@" > "$SPY_ARGS"\nprintf "synthetic stdout\\n"\nprintf "synthetic stderr\\n" >&2\n'
    )
    spy.chmod(0o700)
    env = dict(
        os.environ,
        CC_HOST_PYTHON=str(spy),
        SPY_ARGS=str(tmp_path / "args.txt"),
        CC_HOST_CODEX_CONFIG_FILE=str(tmp_path / "missing-config.toml"),
        TMPDIR=str(tmp_path),
    )
    env.pop("CODEX_MODEL", None)
    return env


@pytest.mark.parametrize("model_args,model", [([], "gpt-5.5"), (["-m", "example-explicit"], "example-explicit")])
def test_foreground_codex_tolerates_missing_config(tmp_path, model_args, model):
    env = spy_environment(tmp_path)
    result = subprocess.run(
        ["bash", str(SHIMS / "codex-shim.sh"), "-", *model_args],
        input="synthetic prompt",
        text=True,
        capture_output=True,
        env=env,
    )
    assert result.returncode == 0
    args = (tmp_path / "args.txt").read_text().splitlines()
    assert args[args.index("--model") + 1] == model


@pytest.mark.parametrize("cli", ["codex", "opencode"])
@pytest.mark.parametrize("stdin_prompt", [False, True])
def test_background_capture_is_private_from_creation(tmp_path, cli, stdin_prompt):
    env = spy_environment(tmp_path)
    env[cli.upper() + "_BG_LAUNCHER"] = "nohup"
    prompt = tmp_path / "public-source.txt"
    prompt.write_text("synthetic private prompt")
    prompt.chmod(0o644)
    args = ["bash", str(SHIMS / f"{cli}-shim-bg.sh")]
    if cli == "opencode":
        args.append("demo/example-model")
    args.append("-" if stdin_prompt else str(prompt))
    if cli == "codex":
        args += ["-m", "example-explicit"]
    # Explicit permissive parent umask must not influence captures.
    result = subprocess.run(args, input="synthetic stdin prompt", text=True, capture_output=True, env=env, umask=0o022)
    assert result.returncode == 0, result.stderr
    fields = dict(line.split(": ", 1) for line in result.stdout.splitlines() if ": " in line)
    task = Path(fields["task_dir"])
    assert task.parent == tmp_path
    assert stat.S_IMODE(task.stat().st_mode) == 0o700
    deadline = time.monotonic() + 5
    while not (task / "exit_code").exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert (task / "exit_code").read_text().strip() == "0"
    for name in ("prompt.txt", "stdout.log", "stderr.log", f"{cli}_args.txt", "exit_code"):
        assert stat.S_IMODE((task / name).stat().st_mode) == 0o600
    assert not task.is_symlink()
    args = (tmp_path / "args.txt").read_text().splitlines()
    if cli == "codex":
        assert args[args.index("--model") + 1] == "example-explicit"


@pytest.mark.parametrize("cli", ["codex", "opencode"])
def test_background_exit_code_is_published_atomically(cli):
    # Callers poll for exit_code to exist and then read it, so it must appear with its
    # content already written (rename into place), after every other output file.
    body = (SHIMS / f"{cli}-shim-bg.sh").read_text()
    assert '> "$SHIM_TASK_DIR/exit_code"' not in body
    assert 'mv "$SHIM_TASK_DIR/exit_code.tmp" "$SHIM_TASK_DIR/exit_code"' in body
    assert body.index("completed_at") < body.index('mv "$SHIM_TASK_DIR/exit_code.tmp"')
