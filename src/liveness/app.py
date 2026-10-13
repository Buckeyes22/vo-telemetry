from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any, Callable

from .config import LivenessConfig
from .metrics import prometheus_text
from .reducer import REDUCER_NAME, Reducer, ReducerConfig
from .schema import ValidationError, validate_event
from .store import LivenessStore


class LivenessBackend:
    def __init__(self, config: LivenessConfig, clock: Callable[[], datetime] | None = None):
        self.config = config
        self.clock = clock or (lambda: datetime.now(UTC))
        self.store = LivenessStore(config.db_path)
        self.store.initialize()
        self.reducer = Reducer(ReducerConfig.from_liveness_config(config))
        self.fatal_error: str | None = None
        try:
            self.reducer.rebuild_from_store(self.store)
        except Exception as exc:
            self.fatal_error = str(exc)
            raise

    def ingest(self, body: bytes, authorization: str | None) -> tuple[int, dict[str, Any]]:
        if not self._authorized(authorization):
            self.reducer.record_ingest("error")
            return 401, {"error": "missing or invalid Bearer token"}
        if len(body) > self.config.max_body_bytes:
            self.reducer.record_ingest("error")
            return 413, {"error": f"request body exceeds {self.config.max_body_bytes} bytes"}
        try:
            raw = json.loads(body.decode("utf-8"))
            event = validate_event(raw, self.config, self.clock())
        except UnicodeDecodeError:
            self.reducer.record_ingest("error")
            return 400, {"error": "request body must be UTF-8 JSON"}
        except json.JSONDecodeError as exc:
            self.reducer.record_ingest("error")
            return 400, {"error": f"invalid JSON: {exc.msg}"}
        except ValidationError as exc:
            self.reducer.record_ingest("error")
            return 400, {"error": str(exc)}

        # Fail-closed on a corrupt/missing/locked SQLite WAL. The store owns the
        # append-only WAL; if it cannot be opened or written, the ingest service must return
        # a clean, *retryable* error to the forwarder (503) rather than letting an unhandled
        # sqlite3.Error tear down the HTTP connection. The forwarder treats any non-2xx /
        # connection failure as a send failure -> exponential backoff (it never drops a
        # terminal event), and the child CLI is never blocked because the shim/hook only ever
        # appends to the *local* outbox. The WAL is not advanced on this path: no ingest_seq
        # is committed, so there is no terminal-state regression and the same event_id will
        # be re-sent (and deduped) once the WAL recovers.
        try:
            ingest_seq, duplicate = self.store.insert_event(event)
            if duplicate:
                self.reducer.record_ingest("duplicate")
                return 200, {"ingest_seq": ingest_seq, "duplicate": True}

            row = self.store.fetch_by_ingest_seq(ingest_seq)
        except sqlite3.Error as exc:
            self.reducer.record_ingest("error")
            return 503, {"error": f"WAL unavailable: {exc}", "wal_reachable": False, "retryable": True}

        if row is None:
            self.reducer.record_ingest("error")
            self.fatal_error = f"inserted ingest_seq {ingest_seq} could not be read back"
            return 500, {"error": self.fatal_error}
        self.reducer.apply_rows([row], now=self.clock())
        self.reducer.flush_expired_gaps(now=self.clock())
        self.store.set_checkpoint(REDUCER_NAME, self.reducer.last_ingest_seq_applied)
        self.reducer.record_ingest("accepted")
        return 202, {"ingest_seq": ingest_seq, "duplicate": False}

    def healthz(self) -> tuple[int, dict[str, Any]]:
        if self.fatal_error:
            return 503, {"ok": False, "fatal_error": self.fatal_error}
        return 200, {"ok": True}

    def readyz(self) -> tuple[int, dict[str, Any]]:
        wal_reachable = self.store.wal_reachable()
        # When the WAL is unreachable, max_ingest_seq() would itself raise sqlite3.Error and
        # crash the /readyz connection. Probe it only when the WAL is reachable so readyz can
        # honestly report wal_reachable=false / ready=false (503) instead of tearing down.
        if wal_reachable:
            try:
                max_ingest_seq = self.store.max_ingest_seq()
            except sqlite3.Error:
                wal_reachable = False
                max_ingest_seq = self.reducer.last_ingest_seq_applied
        else:
            max_ingest_seq = self.reducer.last_ingest_seq_applied
        caught_up = (
            wal_reachable
            and self.reducer.last_ingest_seq_applied >= max_ingest_seq
            and not self.reducer.has_pending_gaps()
        )
        ready = wal_reachable and self.reducer.initialized and caught_up and self.fatal_error is None
        status = 200 if ready else 503
        return status, {
            "ready": ready,
            "wal_reachable": wal_reachable,
            "reducer_initialized": self.reducer.initialized,
            "reducer_caught_up": caught_up,
            "last_ingest_seq_applied": self.reducer.last_ingest_seq_applied,
            "max_ingest_seq": max_ingest_seq,
            "fatal_error": self.fatal_error,
        }

    def runs_current(self) -> tuple[int, dict[str, Any]]:
        return 200, self.reducer.snapshot_current(now=self.clock())

    def lanes_current(self) -> tuple[int, dict[str, Any]]:
        return 200, self.reducer.snapshot_lanes_current(now=self.clock())

    def run_snapshot(self, run_id: str) -> tuple[int, dict[str, Any]]:
        if run_id not in self.reducer.runs:
            return 404, self._not_found(run_id)
        return 200, self.reducer.snapshot_run(run_id, now=self.clock())

    def lanes(self, run_id: str, since_snapshot_version: int) -> tuple[int, dict[str, Any]]:
        if run_id not in self.reducer.runs:
            return 404, self._not_found(run_id)
        return 200, self.reducer.snapshot_lanes_since(
            run_id, since_snapshot_version=since_snapshot_version, now=self.clock()
        )

    def events(
        self, run_id: str, agent_id: str | None, since_ingest_seq: int, limit: int | None
    ) -> tuple[int, dict[str, Any]]:
        if run_id not in self.reducer.runs:
            return 404, self._not_found(run_id)
        bounded_limit = min(max(limit or self.config.max_events_page_size, 1), self.config.max_events_page_size)
        rows = self.store.fetch_run_events(run_id, agent_id, since_ingest_seq, bounded_limit + 1)
        page = rows[:bounded_limit]
        events = []
        for row in page:
            payload = json.loads(row.payload_json)
            payload["ingest_seq"] = row.ingest_seq
            events.append(payload)
        next_page_token = None
        if len(rows) > bounded_limit:
            next_page_token = str(page[-1].ingest_seq)
        return 200, {
            "run_id": run_id,
            "events": events,
            "next_page_token": next_page_token,
            "snapshot_version": self.reducer.snapshot_version,
            "last_ingest_seq_applied": self.reducer.last_ingest_seq_applied,
            "server_time": self.clock().astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        }

    def metrics_text(self) -> str:
        return prometheus_text(
            self.reducer.metrics_snapshot(
                now=self.clock(),
                max_ingest_seq=self.store.max_ingest_seq(),
                wal_size_bytes=self.store.wal_size_bytes(),
            )
        )

    def _authorized(self, authorization: str | None) -> bool:
        if not self.config.ingest_token:
            return False
        return authorization == f"Bearer {self.config.ingest_token}"

    def _not_found(self, run_id: str) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "error": "run not found",
            "snapshot_version": self.reducer.snapshot_version,
            "last_ingest_seq_applied": self.reducer.last_ingest_seq_applied,
            "server_time": self.clock().astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        }
