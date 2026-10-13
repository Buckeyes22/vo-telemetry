from __future__ import annotations

from pathlib import Path
import pytest

from cc_host.shim import ShimSupervisor, parse_args


LOCAL_QWEN_MODEL = "example-model"
LOCAL_QWEN_BASE_URL = "http://127.0.0.1:8080/v1"
LOCAL_QWEN_API_KEY = "test-only-key"


def test_parse_args_accepts_qwen_cli() -> None:
    args = parse_args(["--cli", "qwen", "--provider", "qwen", "--model", LOCAL_QWEN_MODEL])
    assert args.cli == "qwen"
    assert args.provider == "qwen"
    assert args.model == LOCAL_QWEN_MODEL


def test_qwen_child_command_uses_headless_stream_json_and_local_openai_config(tmp_path, monkeypatch) -> None:
    qwen = tmp_path / "qwen"
    qwen.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    qwen.chmod(0o755)
    monkeypatch.setenv("QWEN_BIN", str(qwen))
    monkeypatch.setenv("CC_HOST_ALLOW_UNRESTRICTED", "1")
    monkeypatch.setenv("QWEN_AUTH_TYPE", "openai")
    monkeypatch.setenv("QWEN_OPENAI_BASE_URL", LOCAL_QWEN_BASE_URL)
    monkeypatch.setenv("QWEN_OPENAI_API_KEY", LOCAL_QWEN_API_KEY)

    args = parse_args(
        [
            "--cli",
            "qwen",
            "--provider",
            "qwen",
            "--model",
            LOCAL_QWEN_MODEL,
            "--",
            "--model",
            LOCAL_QWEN_MODEL,
        ]
    )
    supervisor = ShimSupervisor(args)
    command = supervisor._child_command()

    assert command == [
        str(qwen),
        "--input-format",
        "text",
        "--output-format",
        "stream-json",
        "--approval-mode",
        "yolo",
        "--auth-type",
        "openai",
        "--openai-base-url",
        LOCAL_QWEN_BASE_URL,
        "--model",
        LOCAL_QWEN_MODEL,
    ]
    assert LOCAL_QWEN_API_KEY not in command
    assert supervisor._child_env()["OPENAI_API_KEY"] == LOCAL_QWEN_API_KEY
    assert supervisor.outbox.db_path.is_relative_to(tmp_path)


@pytest.mark.parametrize("flag", ["--openai-api-key", "--openai-api-key=test-only-key"])
def test_qwen_rejects_credential_arguments(tmp_path, monkeypatch, flag):
    qwen = tmp_path / "qwen"
    qwen.write_text("#!/usr/bin/env bash\nexit 0\n")
    qwen.chmod(0o700)
    monkeypatch.setenv("QWEN_BIN", str(qwen))
    extra = [flag, "test-only-key"] if flag == "--openai-api-key" else [flag]
    supervisor = ShimSupervisor(parse_args(["--cli", "qwen", "--", *extra]))
    with pytest.raises(ValueError, match="must use QWEN_OPENAI_API_KEY"):
        supervisor._child_command()


def test_qwen_shim_script_routes_to_supervisor() -> None:
    script = Path(__file__).resolve().parents[1] / "shims" / "qwen-shim.sh"
    text = script.read_text(encoding="utf-8")

    assert "--cli qwen" in text
    assert "--provider qwen" in text
    assert "QWEN_BIN" in text
    assert f'QWEN_MODEL="${{QWEN_MODEL:-{LOCAL_QWEN_MODEL}}}"' in text
    assert f"QWEN_OPENAI_BASE_URL:={LOCAL_QWEN_BASE_URL}" in text
    assert "QWEN_OPENAI_API_KEY:?" in text
    assert LOCAL_QWEN_API_KEY not in text
    assert "QWEN_AUTH_TYPE:=openai" in text
    assert '--model "$QWEN_MODEL"' in text
