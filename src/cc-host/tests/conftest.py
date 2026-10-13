"""Every producer test uses disposable state rather than the user's account."""

import pytest


@pytest.fixture(autouse=True)
def isolated_producer_state(tmp_path, monkeypatch):
    monkeypatch.setenv("CLAUDE_TELEMETRY_HOME", str(tmp_path / "telemetry"))
    for key in ("CC_HOST_OUTBOX_DIR", "CC_HOST_OUTBOX_DB", "CC_HOST_PRICING_FILE"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("CLAUDE_PROJECTS_DIR", str(tmp_path / "projects"))
