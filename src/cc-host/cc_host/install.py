from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

from .paths import pricing_file
from .util import atomic_write_json, atomic_write_text, ensure_private_dir


SERVICE_NAME = "vo-telemetry-liveness-forwarder.service"
TAILER_SERVICE_NAME = "vo-telemetry-transcript-tailer.service"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Install vo-telemetry CC-host liveness producers")
    parser.add_argument("--settings", default=str(Path("~/.claude/settings.json").expanduser()))
    parser.add_argument("--no-systemctl", action="store_true")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    install_hooks(Path(args.settings), root)
    install_pricing_template()
    unit_path = install_unit(root)
    tailer_unit_path = install_tailer_unit(root)
    if not args.no_systemctl:
        enable_unit(unit_path)
        enable_unit(tailer_unit_path)
    print(f"installed hooks in {Path(args.settings).expanduser()}")
    print(f"installed systemd user unit at {unit_path}")
    print(f"installed systemd user unit at {tailer_unit_path}")
    return 0


def install_hooks(settings_path: Path, root: Path) -> None:
    settings_path = settings_path.expanduser()
    ensure_private_dir(settings_path.parent)
    if settings_path.exists():
        settings = json.loads(settings_path.read_text(encoding="utf-8"))
        if not isinstance(settings, dict):
            raise ValueError(f"settings file must contain a JSON object: {settings_path}")
    else:
        settings = {}
    hooks = settings.setdefault("hooks", {})
    if not isinstance(hooks, dict):
        raise ValueError("settings.hooks must be a JSON object")

    for event_name in ("UserPromptSubmit", "SubagentStart", "SubagentStop", "SessionEnd"):
        _append_hook(hooks, event_name, _hook_entry(root, event_name))
    _append_hook(hooks, "PreToolUse", _hook_entry(root, "PreToolUse", matcher="Bash"))
    atomic_write_json(settings_path, settings, mode=0o600)


def install_pricing_template() -> None:
    target = pricing_file()
    if target.exists():
        return
    text = """# vo-telemetry liveness pricing table.
# Keys must be exact CLI-emitted model strings verified on the installed CLI.
pricing:
  effective_date: 2026-05-31
  currency: USD
  models: {}
"""
    atomic_write_text(target, text, mode=0o600)


def install_unit(root: Path) -> Path:
    unit_dir = Path("~/.config/systemd/user").expanduser()
    ensure_private_dir(unit_dir)
    unit_path = unit_dir / SERVICE_NAME
    python = Path(sys.executable).resolve()
    text = unit_text(root=root, python=python)
    atomic_write_text(unit_path, text, mode=0o600)
    return unit_path


def enable_unit(unit_path: Path) -> None:
    try:
        subprocess.run(["systemctl", "--user", "daemon-reload"], check=False, timeout=10)
        subprocess.run(["systemctl", "--user", "enable", "--now", unit_path.name], check=False, timeout=20)
    except Exception as exc:
        print(f"warning: could not enable systemd user unit: {exc}", file=sys.stderr)


def install_tailer_unit(root: Path) -> Path:
    unit_dir = Path("~/.config/systemd/user").expanduser()
    ensure_private_dir(unit_dir)
    unit_path = unit_dir / TAILER_SERVICE_NAME
    python = Path(sys.executable).resolve()
    atomic_write_text(unit_path, tailer_unit_text(root=root, python=python), mode=0o600)
    return unit_path


def unit_text(*, root: Path, python: Path) -> str:
    return f"""[Unit]
Description=vo-telemetry CC-host liveness outbox forwarder
After=network-online.target

[Service]
Type=simple
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONPATH={root}
Environment=LIVENESS_INGEST_URL=http://127.0.0.1:8723/ingest
EnvironmentFile=-%h/.claude/telemetry/liveness.env
ExecStart={python} -m cc_host.forwarder
Restart=always
RestartSec=2

[Install]
WantedBy=default.target
"""


def tailer_unit_text(*, root: Path, python: Path) -> str:
    return f"""[Unit]
Description=vo-telemetry CC-host subagent transcript tailer (11.6)

[Service]
Type=simple
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONPATH={root}
ExecStart={python} -m cc_host.transcript_tailer
Restart=always
RestartSec=2

[Install]
WantedBy=default.target
"""


def _hook_entry(root: Path, event_name: str, matcher: str | None = None) -> dict[str, Any]:
    command = f"PYTHONPATH={shlex.quote(str(root))} {shlex.quote(str(Path(sys.executable).resolve()))} -m cc_host.hooks {event_name}"
    entry: dict[str, Any] = {"hooks": [{"type": "command", "command": command}]}
    if matcher:
        entry["matcher"] = matcher
    return entry


def _append_hook(hooks: dict[str, Any], event_name: str, entry: dict[str, Any]) -> None:
    current = hooks.setdefault(event_name, [])
    if not isinstance(current, list):
        raise ValueError(f"settings.hooks.{event_name} must be a list")
    command = entry["hooks"][0]["command"]
    for existing in current:
        if not isinstance(existing, dict):
            continue
        for hook in existing.get("hooks", []):
            if isinstance(hook, dict) and hook.get("command") == command:
                return
    current.append(entry)


if __name__ == "__main__":
    raise SystemExit(main())
