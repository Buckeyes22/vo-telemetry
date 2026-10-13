from __future__ import annotations

import pytest

from cc_host.hooks import _provider_for_agent


def test_provider_for_agent_opencode_shim_maps_to_opencode(monkeypatch: pytest.MonkeyPatch) -> None:
    # L6: an opencode-shim subagent's agent_type contains none of glm/kimi/minimax/codex/openai, so it
    # previously fell through to "unknown". It now resolves to the opencode CLI family. (The concrete
    # sub-provider — kimi/glm/minimax — isn't known at SubagentStart; it's captured on the shim's lane.)
    monkeypatch.delenv("PROVIDER", raising=False)
    assert _provider_for_agent("opencode-shim") == "opencode"


def test_provider_for_agent_codex_shim_maps_to_openai() -> None:
    assert _provider_for_agent("codex-shim") == "openai"


def test_provider_for_agent_explicit_sub_providers_win() -> None:
    # glm/kimi/minimax are checked before the generic opencode branch, so opencode-glm → glm.
    assert _provider_for_agent("opencode-glm") == "glm"
    assert _provider_for_agent("kimi") == "kimi"
    assert _provider_for_agent("minimax") == "minimax"


def test_provider_for_agent_unknown_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PROVIDER", raising=False)
    assert _provider_for_agent("general-purpose") == "unknown"
