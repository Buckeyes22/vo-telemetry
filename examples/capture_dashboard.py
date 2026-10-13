"""Capture the real Textual UI against a temporary backend and synthetic events.

Requires requirements-dashboard.txt. No model CLI, credential, or live transcript
is read. The SVG is exported directly by the running application.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import secrets
import tempfile
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path

from dashboards.subagent_dashboard import SubagentDashboard
from liveness.app import LivenessBackend
from liveness.config import LivenessConfig
from liveness.server import make_server


def seed(backend: LivenessBackend, token: str) -> None:
    def emit(kind: str, source: str, seq: int, **extra):
        event = {
            "schema_version": 1,
            "event_id": str(uuid.uuid4()),
            "event_type": kind,
            "ts": datetime.now(UTC).isoformat(),
            "run_id": "synthetic-demo",
            "session_id": "demo-session",
            "orchestrator": "demo",
            "agent_id": None,
            "source": "shim",
            "source_instance_id": "shim:" + source,
            "source_seq": seq,
        }
        event.update(extra)
        status, response = backend.ingest(json.dumps(event).encode(), "Bearer " + token)
        if status != 202:
            raise RuntimeError("synthetic ingest failed: " + str(response))

    emit("run_started", "demo-run", 1, project="synthetic-fixture", operator_label="Demo data only")
    for agent, provider, outcome, tin, tout in [
        ("demo-active", "openai", None, 1200, 240),
        ("demo-done", "anthropic", "success", 900, 180),
        ("demo-failed", "local", "failure", 500, 60),
    ]:
        fields = dict(
            agent_id=agent,
            agent_type="synthetic",
            provider=provider,
            model="example-model",
            worktree="/workspace/example",
            branch="demo",
        )
        emit("lane_started", agent, 1, **fields)
        emit("tool_started", agent, 2, tool_run_id=agent + "-tool", **fields)
        emit(
            "token_delta",
            agent,
            3,
            tool_run_id=agent + "-tool",
            tokens_total={"in": tin, "out": tout},
            tokens_delta={"in": tin, "out": tout},
            cost_source="unavailable",
            **fields,
        )
        if outcome:
            emit(
                "tool_completed" if outcome == "success" else "tool_failed",
                agent,
                4,
                tool_run_id=agent + "-tool",
                exit_code=0 if outcome == "success" else 1,
                **fields,
            )
            emit("lane_stopped", agent, 5, declared_outcome=outcome, summary="Synthetic scenario only", **fields)


async def capture(output: Path):
    with tempfile.TemporaryDirectory(prefix="vo-telemetry-ui-demo-") as tmp:
        token = secrets.token_urlsafe(32)
        backend = LivenessBackend(LivenessConfig(db_path=Path(tmp) / "events.db", ingest_token=token))
        seed(backend, token)
        server = make_server(("127.0.0.1", 0), backend)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = "http://127.0.0.1:" + str(server.server_address[1])
        app = SubagentDashboard(api_base=base)
        try:
            async with app.run_test(size=(160, 36)) as pilot:
                for _ in range(40):
                    await pilot.pause(0.2)
                    if len(app._current_lanes) == 3:
                        break
                if len(app._current_lanes) != 3:
                    raise RuntimeError("dashboard did not render all synthetic lanes")
                await pilot.pause(0.4)
                output.parent.mkdir(parents=True, exist_ok=True)
                svg = app.export_screenshot(title="vo-telemetry dashboard / synthetic data")
                # Preserve the application render; remove remote font dependencies.
                svg = re.sub(r"@font-face\s*\{[^}]*\}", "", svg)
                svg = svg.replace("Fira Code, monospace", "monospace")
                output.write_text(svg)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)
    print("Captured actual application SVG using synthetic data:", output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("docs/images/dashboard-synthetic.svg"))
    args = parser.parse_args()
    asyncio.run(capture(args.output))
