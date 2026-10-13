"""Generated systemd units must not depend on a fixed checkout directory."""

from pathlib import Path

from cc_host.install import tailer_unit_text, unit_text


def test_forwarder_unit_points_at_actual_checkout(tmp_path: Path) -> None:
    root = tmp_path / "any-checkout-name" / "src" / "cc-host"
    python = tmp_path / "venv" / "bin" / "python3"
    text = unit_text(root=root, python=python)
    assert f"Environment=PYTHONPATH={root}" in text
    assert f"ExecStart={python} -m cc_host.forwarder" in text
    assert "/vo-telemetry" not in text


def test_tailer_unit_points_at_actual_checkout(tmp_path: Path) -> None:
    root = tmp_path / "elsewhere" / "src" / "cc-host"
    python = tmp_path / "py"
    text = tailer_unit_text(root=root, python=python)
    assert str(root) in text
    assert str(python) in text
    assert "/vo-telemetry" not in text
