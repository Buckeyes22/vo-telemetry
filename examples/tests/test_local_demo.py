"""The documented demo must produce a discoverable run in the real dashboard."""

from __future__ import annotations

import asyncio
import json
import os
import select
import signal
import subprocess
import sys
import urllib.request
from pathlib import Path

from dashboards.subagent_dashboard import SubagentDashboard

ROOT = Path(__file__).resolve().parents[2]


def line_with_timeout(process):
    assert select.select([process.stdout], [], [], 8)[0], "demo did not become ready"
    return process.stdout.readline().strip()


def test_documented_demo_is_selectable_in_actual_dashboard():
    process = subprocess.Popen(
        [sys.executable, "-u", "examples/local_demo.py", "--port", "0"],
        cwd=ROOT,
        env=dict(os.environ, PYTHONPATH=str(ROOT / "src"), PYTHONDONTWRITEBYTECODE="1"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        ready = line_with_timeout(process)
        assert ready.startswith("Synthetic demo ready: ")
        base = ready.split(": ", 1)[1]
        # The first print proves initialization; remaining short lines follow.
        run_line = process.stdout.readline().strip()
        snapshot_line = process.stdout.readline().strip()
        assert run_line.startswith("Run ID: ")
        run_id = run_line.split(": ", 1)[1]
        assert snapshot_line == "Snapshot: " + base + "/runs/" + run_id

        def get(path):
            with urllib.request.urlopen(base + path, timeout=3) as response:
                assert response.status == 200
                return json.loads(response.read())

        assert get("/readyz")["ready"]
        current = get("/runs/current")
        assert len(current["runs"]) == 1
        assert current["runs"][0]["run_id"] == run_id
        assert current["runs"][0]["state"] == "open"
        detail = get("/runs/" + run_id)
        assert detail["lanes"][0]["state"] == "done"
        assert detail["lanes"][0]["agent_id"] == "demo-agent"

        async def verify_ui():
            app = SubagentDashboard(api_base=base)
            async with app.run_test(size=(160, 36)) as pilot:
                for _ in range(40):
                    await pilot.pause(0.2)
                    if app._selected_run_id == run_id and app._current_lanes:
                        break
                assert app._selected_run_id == run_id
                assert app._current_lanes[0]["agent_id"] == "demo-agent"
                assert app._current_lanes[0]["state"] == "done"
                assert any(run["run_id"] == run_id for run in app._all_runs)

        asyncio.run(verify_ui())
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=5)
    assert process.returncode == 0
    assert not process.stderr.read()
