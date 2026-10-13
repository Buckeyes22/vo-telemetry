from __future__ import annotations

import hashlib
import json
import threading
import urllib.error
import urllib.request
from datetime import UTC, datetime


from liveness.app import LivenessBackend
from liveness.config import LivenessConfig
from liveness.server import make_server


NOW = datetime(2026, 5, 31, 14, 22, 1, tzinfo=UTC)


def valid_event(**extra: object) -> dict[str, object]:
    event: dict[str, object] = {
        "schema_version": 1,
        "event_id": "10000000-0000-4000-8000-000000000001",
        "event_type": "lane_stopped",
        "ts": "2026-05-31T14:22:01Z",
        "run_id": "run-1",
        "session_id": "session-1",
        "orchestrator": "claude-code",
        "agent_id": "agent-1",
        "quarantine": False,
        "agent_type": "codex",
        "provider": "openai",
        "model": "gpt-5.4-mini",
        "source": "hook",
        "source_instance_id": "hook:session-1:SubagentStop:one",
        "source_seq": 1,
        "declared_outcome": "success",
        "stop_reason": "normal_stop",
        "payload": {"ext": {"producer_extra": "accepted"}},
    }
    event.update(extra)
    return event


def test_ingest_rejects_unknown_top_level_fields(tmp_path) -> None:
    backend = LivenessBackend(
        LivenessConfig(db_path=tmp_path / "events.db", ingest_token="secret"),
        clock=lambda: NOW,
    )
    body = json.dumps(valid_event(unexpected="bad")).encode()

    status, payload = backend.ingest(body, "Bearer secret")

    assert status == 400
    assert "unknown top-level field" in payload["error"]


def test_ingest_auth_body_schema_time_and_id_bounds(tmp_path) -> None:
    backend = LivenessBackend(
        LivenessConfig(db_path=tmp_path / "events.db", ingest_token="secret"),
        clock=lambda: NOW,
    )
    small_body_backend = LivenessBackend(
        LivenessConfig(db_path=tmp_path / "small.db", ingest_token="secret", max_body_bytes=200),
        clock=lambda: NOW,
    )

    assert backend.ingest(json.dumps(valid_event()).encode(), None)[0] == 401
    assert small_body_backend.ingest(b"{" + b"x" * 300 + b"}", "Bearer secret")[0] == 413
    assert backend.ingest(json.dumps(valid_event(schema_version=2)).encode(), "Bearer secret")[0] == 400
    assert backend.ingest(json.dumps(valid_event(event_type="status")).encode(), "Bearer secret")[0] == 400
    assert backend.ingest(json.dumps(valid_event(ts="2026-05-29T14:22:01Z")).encode(), "Bearer secret")[0] == 400
    assert backend.ingest(json.dumps(valid_event(run_id="r" * 257)).encode(), "Bearer secret")[0] == 400
    assert backend.ingest(json.dumps(valid_event(agent_id="a" * 129)).encode(), "Bearer secret")[0] == 400


def test_ingest_dedupes_event_id_and_sanitizes_final_message(tmp_path) -> None:
    final_message = "TELEMETRY_OUTCOME: success\nTELEMETRY_SUMMARY: finished safely\n" + ("sensitive text " * 40)
    backend = LivenessBackend(
        LivenessConfig(db_path=tmp_path / "events.db", ingest_token="secret", final_message_preview_chars=32),
        clock=lambda: NOW,
    )
    body = json.dumps(valid_event(final_message=final_message, summary="x" * 600)).encode()

    first_status, first_payload = backend.ingest(body, "Bearer secret")
    second_status, second_payload = backend.ingest(body, "Bearer secret")

    assert first_status == 202
    assert first_payload["ingest_seq"] == 1
    assert second_status == 200
    assert second_payload["duplicate"] is True
    stored = json.loads(backend.store.fetch_after(0, 1)[0].payload_json)
    assert "final_message" not in stored
    assert stored["final_message_preview"] == final_message[:32]
    assert stored["final_message_sha256"] == hashlib.sha256(final_message.encode()).hexdigest()
    assert stored["summary"] == "finished safely"


def test_sqlite_schema_and_checkpoint_tables_are_exact(tmp_path) -> None:
    backend = LivenessBackend(
        LivenessConfig(db_path=tmp_path / "events.db", ingest_token="secret"),
        clock=lambda: NOW,
    )
    rows = backend.store.query("PRAGMA table_info(events)")
    assert [row["name"] for row in rows] == [
        "ingest_seq",
        "event_id",
        "run_id",
        "agent_id",
        "event_type",
        "ts",
        "source_instance_id",
        "source_seq",
        "payload_json",
    ]
    indexes = {row["name"] for row in backend.store.query("PRAGMA index_list(events)")}
    assert {"idx_events_event_id", "idx_events_run_seq", "idx_events_run_agent_seq", "idx_events_source"} <= indexes
    assert backend.store.query("PRAGMA journal_mode")[0][0].lower() == "wal"
    checkpoint_cols = backend.store.query("PRAGMA table_info(reducer_checkpoint)")
    assert [row["name"] for row in checkpoint_cols] == [
        "reducer_name",
        "last_ingest_seq_applied",
        "updated_ts",
    ]


def test_http_snapshot_readyz_events_and_metrics(tmp_path) -> None:
    backend = LivenessBackend(
        LivenessConfig(db_path=tmp_path / "events.db", ingest_token="secret"),
        clock=lambda: NOW,
    )
    server = make_server(("127.0.0.1", 0), backend)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    base = f"http://127.0.0.1:{port}"

    try:
        request = urllib.request.Request(
            f"{base}/ingest",
            data=json.dumps(valid_event()).encode(),
            headers={"Authorization": "Bearer secret", "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=3) as response:
            assert response.status == 202

        with urllib.request.urlopen(f"{base}/readyz", timeout=3) as response:
            assert response.status == 200
            ready = json.loads(response.read())
            assert ready["ready"] is True

        with urllib.request.urlopen(f"{base}/runs/run-1", timeout=3) as response:
            snapshot = json.loads(response.read())
            assert snapshot["run_id"] == "run-1"
            assert snapshot["snapshot_version"] >= 1
            assert snapshot["last_ingest_seq_applied"] == 1

        with urllib.request.urlopen(f"{base}/runs/run-1/events?since_ingest_seq=0&limit=1", timeout=3) as response:
            events = json.loads(response.read())
            assert len(events["events"]) == 1
            assert events["events"][0]["ingest_seq"] == 1

        with urllib.request.urlopen(f"{base}/metrics", timeout=3) as response:
            metrics = response.read().decode()
            assert "subagent_lanes_failed_total" in metrics
            assert "agent_id" not in metrics
    finally:
        server.shutdown()
        server.server_close()


def _corrupt_wal(db_path) -> None:
    """Overwrite the SQLite main db with garbage and remove the -wal/-shm sidecars,
    simulating an unavailable/corrupt WAL."""
    from pathlib import Path

    db_path = Path(db_path)
    size = db_path.stat().st_size
    db_path.write_bytes(b"\xde\xad\xbe\xef" * (size // 4 + 16))
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(db_path) + suffix)
        if sidecar.exists():
            sidecar.unlink()


def test_ingest_returns_clean_503_when_wal_unavailable(tmp_path) -> None:
    """Corrupt WAL -> ingest returns a clean, retryable 503 (no raised sqlite3.Error).

    The WAL must NOT advance on this path, so no terminal state is committed and the
    same event_id will be re-sent + deduped once the WAL recovers."""
    db_path = tmp_path / "events.db"
    backend = LivenessBackend(LivenessConfig(db_path=db_path, ingest_token="secret"), clock=lambda: NOW)

    status, _ = backend.ingest(json.dumps(valid_event()).encode(), "Bearer secret")
    assert status == 202

    _corrupt_wal(db_path)

    status, payload = backend.ingest(
        json.dumps(valid_event(event_id="10000000-0000-4000-8000-000000000002")).encode(),
        "Bearer secret",
    )
    assert status == 503
    assert payload["wal_reachable"] is False
    assert payload["retryable"] is True
    assert "WAL unavailable" in payload["error"]


def test_readyz_reports_unready_when_wal_unavailable(tmp_path) -> None:
    """Corrupt WAL -> /readyz returns 503 with wal_reachable=false instead of raising."""
    db_path = tmp_path / "events.db"
    backend = LivenessBackend(LivenessConfig(db_path=db_path, ingest_token="secret"), clock=lambda: NOW)
    backend.ingest(json.dumps(valid_event()).encode(), "Bearer secret")

    _corrupt_wal(db_path)

    status, payload = backend.readyz()
    assert status == 503
    assert payload["ready"] is False
    assert payload["wal_reachable"] is False


def test_http_ingest_and_readyz_survive_wal_corruption(tmp_path) -> None:
    """Over real HTTP: a corrupt WAL yields clean 503 responses (connection not torn down)."""
    db_path = tmp_path / "events.db"
    backend = LivenessBackend(LivenessConfig(db_path=db_path, ingest_token="secret"), clock=lambda: NOW)
    server = make_server(("127.0.0.1", 0), backend)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    base = f"http://127.0.0.1:{port}"
    try:
        request = urllib.request.Request(
            f"{base}/ingest",
            data=json.dumps(valid_event()).encode(),
            headers={"Authorization": "Bearer secret", "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=3) as response:
            assert response.status == 202

        _corrupt_wal(db_path)

        request = urllib.request.Request(
            f"{base}/ingest",
            data=json.dumps(valid_event(event_id="10000000-0000-4000-8000-000000000099")).encode(),
            headers={"Authorization": "Bearer secret", "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=3) as response:
                status = response.status
                body = json.loads(response.read())
        except urllib.error.HTTPError as exc:
            status = exc.code
            body = json.loads(exc.read())
        assert status == 503
        assert body["retryable"] is True

        try:
            with urllib.request.urlopen(f"{base}/readyz", timeout=3) as response:
                rstatus = response.status
                rbody = json.loads(response.read())
        except urllib.error.HTTPError as exc:
            rstatus = exc.code
            rbody = json.loads(exc.read())
        assert rstatus == 503
        assert rbody["wal_reachable"] is False
    finally:
        server.shutdown()
        server.server_close()
