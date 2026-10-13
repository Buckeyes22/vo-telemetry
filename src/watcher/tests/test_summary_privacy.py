"""Sensitive structured fields must be excluded before dry-run logs or sends."""

from __future__ import annotations

import json
import logging
from unittest.mock import patch, MagicMock


from watcher.config import WatcherConfig
from watcher.telegram import format_run_summary, send_telegram
from watcher.models import Run
from watcher.run import WatcherLoop


def summary(config):
    lanes = [
        {
            "agent_id": "demo-agent",
            "declared_outcome": "success",
            "summary": "SYNTHETIC_PRIVATE_MARKER",
            "failed_children": 0,
        }
    ]
    message = format_run_summary(
        run_id="demo-run",
        project="SYNTHETIC_PROJECT_MARKER",
        state="closed",
        lane_summaries=lanes,
        total_cost_usd=0,
        dry_run=config.dry_run,
        config=config,
    )
    assert lanes[0]["summary"] == "SYNTHETIC_PRIVATE_MARKER"  # input unchanged
    return message


def test_short_summary_is_excluded_from_dry_run_log(caplog):
    config = WatcherConfig()
    message = summary(config)
    assert "SYNTHETIC_PRIVATE_MARKER" not in message
    with patch("urllib.request.urlopen") as network, caplog.at_level(logging.INFO):
        send_telegram(message, config=config)
    network.assert_not_called()
    assert "SYNTHETIC_PRIVATE_MARKER" not in caplog.text


def test_configured_fields_excluded_before_live_transport():
    config = WatcherConfig(
        dry_run=False,
        telegram_bot_token="123456:ABCDEFtest",
        telegram_chat_id="demo-chat",
        redact_fields=frozenset({"summary", "project"}),
    )
    response = MagicMock()
    response.__enter__.return_value.status = 200
    with patch("urllib.request.urlopen", return_value=response) as network:
        send_telegram(summary(config), config=config)
    payload = json.loads(network.call_args.args[0].data)
    assert "SYNTHETIC_PRIVATE_MARKER" not in payload["text"]
    assert "SYNTHETIC_PROJECT_MARKER" not in payload["text"]


def test_closed_run_path_does_not_log_raw_lane_summary(tmp_path, caplog):
    config = WatcherConfig(audit_log_path=str(tmp_path / "audit.jsonl"))
    run = Run.from_dict(
        {
            "run_id": "demo-run",
            "state": "closed",
            "lanes": [
                {
                    "agent_id": "demo-agent",
                    "declared_outcome": "success",
                    "state": "done",
                    "summary": "SYNTHETIC_PRIVATE_MARKER",
                }
            ],
        }
    )
    loop = WatcherLoop(config)
    try:
        with patch("urllib.request.urlopen") as network, caplog.at_level(logging.INFO):
            loop._handle_run_closed(run)
        network.assert_not_called()
        assert "SYNTHETIC_PRIVATE_MARKER" not in caplog.text
        assert "SYNTHETIC_PRIVATE_MARKER" not in (tmp_path / "audit.jsonl").read_text()
    finally:
        loop._audit.close()
