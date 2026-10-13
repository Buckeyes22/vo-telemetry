"""Send generated synthetic lifecycle events; never reads private run data."""

from __future__ import annotations

import json
import os
import urllib.request
import uuid
from datetime import UTC, datetime


def synthetic_events():
    run = "demo-" + uuid.uuid4().hex[:12]
    source = "shim:demo-" + uuid.uuid4().hex[:12]
    specs = [
        ("run_started", {"agent_id": None, "project": "synthetic-demo"}),
        ("lane_started", {}),
        ("tool_started", {}),
        ("token_delta", {"tokens_total": {"in": 100, "out": 25}, "tokens_delta": {"in": 100, "out": 25}}),
        ("tool_completed", {"exit_code": 0}),
        ("lane_stopped", {"declared_outcome": "success", "summary": "Synthetic example finished"}),
        ("run_closed", {"agent_id": None}),
    ]
    for seq, (kind, extra) in enumerate(specs, 1):
        event = {
            "schema_version": 1,
            "event_id": str(uuid.uuid4()),
            "event_type": kind,
            "ts": datetime.now(UTC).isoformat(),
            "run_id": run,
            "session_id": "demo-session",
            "orchestrator": "demo",
            "agent_id": "demo-agent",
            "agent_type": "synthetic",
            "provider": "demo",
            "model": "example-model",
            "source": "shim",
            "source_instance_id": source,
            "source_seq": seq,
        }
        if kind.startswith("tool_") or kind == "token_delta":
            event["tool_run_id"] = "demo-tool"
        event.update(extra)
        yield event


def send(base: str, token: str, event: dict):
    request = urllib.request.Request(
        base.rstrip("/") + "/ingest",
        json.dumps(event).encode(),
        method="POST",
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + token},
    )
    # Fixed loopback URL.
    with urllib.request.urlopen(request, timeout=5) as response:  # nosec B310
        return response.status, json.loads(response.read())


if __name__ == "__main__":
    token = os.environ.get("LIVENESS_INGEST_TOKEN", "")
    if not token:
        raise SystemExit("Set LIVENESS_INGEST_TOKEN to match your local server.")
    base = os.environ.get("DEMO_API_BASE", "http://127.0.0.1:8723")
    for event in synthetic_events():
        status, payload = send(base, token, event)
        print(event["event_type"], status)
    print("Synthetic run:", event["run_id"])
