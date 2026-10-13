"""Loopback-only backend with synthetic data, a private token and temporary storage."""

from __future__ import annotations
import argparse
import secrets
import tempfile
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from liveness.app import LivenessBackend
from liveness.config import LivenessConfig
from liveness.server import make_server
from send_synthetic_run import synthetic_events, send


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8723)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="vo-telemetry-local-demo-") as tmp:
        token = secrets.token_urlsafe(32)
        backend = LivenessBackend(LivenessConfig(db_path=Path(tmp) / "events.db", ingest_token=token))
        try:
            server = make_server(("127.0.0.1", args.port), backend)
        except OSError as exc:
            raise SystemExit("Cannot bind local demo port; choose --port <unused-port>: " + str(exc))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = "http://127.0.0.1:" + str(server.server_address[1])
        closing_event = None
        next_seq = 0
        try:
            events = list(synthetic_events())
            closing_event = events.pop()
            for event in events:
                send(base, token, event)
            next_seq = closing_event["source_seq"]
            print("Synthetic demo ready:", base, flush=True)
            print("Run ID:", closing_event["run_id"], flush=True)
            print("Snapshot:", base + "/runs/" + closing_event["run_id"], flush=True)
            print("Temporary data only. Press Ctrl-C to stop.", flush=True)
            last_refresh = time.monotonic()
            while thread.is_alive():
                thread.join(timeout=0.5)
                if time.monotonic() - last_refresh >= 30:
                    update = dict(
                        closing_event,
                        event_type="run_updated",
                        event_id=str(uuid.uuid4()),
                        source_seq=next_seq,
                        ts=datetime.now(UTC).isoformat(),
                    )
                    send(base, token, update)
                    next_seq += 1
                    last_refresh = time.monotonic()
        except KeyboardInterrupt:
            pass
        finally:
            try:
                if closing_event is not None and next_seq:
                    closing_event.update(
                        event_id=str(uuid.uuid4()), source_seq=next_seq, ts=datetime.now(UTC).isoformat()
                    )
                    send(base, token, closing_event)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)


if __name__ == "__main__":
    main()
