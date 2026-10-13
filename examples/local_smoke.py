"""Self-contained local HTTP validation using synthetic data and temporary storage."""

from __future__ import annotations

import json
import secrets
import tempfile
import threading
import urllib.error
import urllib.request
from pathlib import Path

from liveness.app import LivenessBackend
from liveness.config import LivenessConfig
from liveness.server import make_server
from send_synthetic_run import synthetic_events, send


def main():
    with tempfile.TemporaryDirectory(prefix="vo-telemetry-smoke-") as tmp:
        token = secrets.token_urlsafe(32)
        cfg = LivenessConfig(db_path=Path(tmp) / "events.db", ingest_token=token)
        backend = LivenessBackend(cfg)
        server = make_server(("127.0.0.1", 0), backend)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = "http://127.0.0.1:" + str(server.server_address[1])
        try:
            events = list(synthetic_events())
            try:
                send(base, "incorrect-test-token", events[0])
                raise AssertionError("unauthorized ingest accepted")
            except urllib.error.HTTPError as exc:
                assert exc.code == 401
            for event in events:
                assert send(base, token, event)[0] == 202
            status, duplicate = send(base, token, events[-1])
            assert status == 200 and duplicate["duplicate"]

            def get(path):
                # Fixed loopback URL.
                with urllib.request.urlopen(base + path, timeout=3) as response:  # nosec B310
                    assert response.status == 200
                    return json.loads(response.read())

            snapshot = get("/runs/" + events[0]["run_id"])
            assert snapshot["state"] == "closed"
            assert snapshot["lanes"][0]["state"] == "done"
            assert get("/readyz")["ready"]
            assert get("/healthz")
            assert len(get("/runs/" + events[0]["run_id"] + "/events")["events"]) == 7
            # Fixed loopback URL.
            with urllib.request.urlopen(base + "/metrics", timeout=3) as response:  # nosec B310
                assert "subagent_lanes_failed_total" in response.read().decode()
            restored = LivenessBackend(cfg)
            status, replay = restored.run_snapshot(events[0]["run_id"])
            assert status == 200 and replay["state"] == "closed"
            assert replay["lanes"][0]["state"] == "done"
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)
    print("PASS: local auth, lifecycle, dedupe, snapshots, health, metrics and restart replay")


if __name__ == "__main__":
    main()
