from __future__ import annotations

import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .models import EventRow
from .schema import serialize_event


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS events (
  ingest_seq         INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id           TEXT UNIQUE NOT NULL,
  run_id             TEXT NOT NULL,
  agent_id           TEXT,
  event_type         TEXT NOT NULL,
  ts                 TEXT NOT NULL,
  source_instance_id TEXT NOT NULL,
  source_seq         INTEGER NOT NULL,
  payload_json       TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id      ON events(event_id);
CREATE INDEX IF NOT EXISTS idx_events_run_seq              ON events(run_id, ingest_seq);
CREATE INDEX IF NOT EXISTS idx_events_run_agent_seq        ON events(run_id, agent_id, ingest_seq);
CREATE INDEX IF NOT EXISTS idx_events_source               ON events(source_instance_id, source_seq);
CREATE TABLE IF NOT EXISTS reducer_checkpoint (
  reducer_name            TEXT PRIMARY KEY,
  last_ingest_seq_applied INTEGER NOT NULL,
  updated_ts              TEXT NOT NULL
);
"""


class LivenessStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self._lock = threading.Lock()
        self._initialized = False

    def initialize(self) -> None:
        if str(self.db_path) != ":memory:":
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(SCHEMA_SQL)
            conn.commit()
        self._initialized = True

    def insert_event(self, event: dict[str, Any]) -> tuple[int, bool]:
        self._ensure_initialized()
        payload_json = serialize_event(event)
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO events (
                      event_id, run_id, agent_id, event_type, ts,
                      source_instance_id, source_seq, payload_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event["event_id"],
                        event["run_id"],
                        event.get("agent_id"),
                        event["event_type"],
                        event["ts"],
                        event["source_instance_id"],
                        event["source_seq"],
                        payload_json,
                    ),
                )
                if cur.rowcount == 0:
                    row = conn.execute(
                        "SELECT ingest_seq FROM events WHERE event_id = ?", (event["event_id"],)
                    ).fetchone()
                    if row is None:
                        raise RuntimeError("event_id insert was ignored but no existing row was found")
                    conn.commit()
                    return int(row["ingest_seq"]), True
                if cur.lastrowid is None:
                    raise RuntimeError("event insert returned no rowid")
                ingest_seq = int(cur.lastrowid)
                conn.commit()
                return ingest_seq, False

    def fetch_by_ingest_seq(self, ingest_seq: int) -> EventRow | None:
        rows = self._event_rows(
            "SELECT * FROM events WHERE ingest_seq = ?",
            (ingest_seq,),
        )
        return rows[0] if rows else None

    def fetch_after(self, ingest_seq: int, limit: int) -> list[EventRow]:
        return self._event_rows(
            "SELECT * FROM events WHERE ingest_seq > ? ORDER BY ingest_seq LIMIT ?",
            (ingest_seq, limit),
        )

    def fetch_run_events(self, run_id: str, agent_id: str | None, since_ingest_seq: int, limit: int) -> list[EventRow]:
        if agent_id is None:
            return self._event_rows(
                """
                SELECT * FROM events
                WHERE run_id = ? AND ingest_seq > ?
                ORDER BY ingest_seq
                LIMIT ?
                """,
                (run_id, since_ingest_seq, limit),
            )
        return self._event_rows(
            """
            SELECT * FROM events
            WHERE run_id = ? AND agent_id = ? AND ingest_seq > ?
            ORDER BY ingest_seq
            LIMIT ?
            """,
            (run_id, agent_id, since_ingest_seq, limit),
        )

    def max_ingest_seq(self) -> int:
        row = self.query("SELECT COALESCE(MAX(ingest_seq), 0) AS max_seq FROM events")[0]
        return int(row["max_seq"])

    def get_checkpoint(self, reducer_name: str) -> int:
        rows = self.query(
            "SELECT last_ingest_seq_applied FROM reducer_checkpoint WHERE reducer_name = ?",
            (reducer_name,),
        )
        return int(rows[0]["last_ingest_seq_applied"]) if rows else 0

    def set_checkpoint(self, reducer_name: str, last_ingest_seq_applied: int) -> None:
        now = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO reducer_checkpoint (reducer_name, last_ingest_seq_applied, updated_ts)
                    VALUES (?, ?, ?)
                    ON CONFLICT(reducer_name) DO UPDATE SET
                      last_ingest_seq_applied = excluded.last_ingest_seq_applied,
                      updated_ts = excluded.updated_ts
                    """,
                    (reducer_name, last_ingest_seq_applied, now),
                )
                conn.commit()

    def prune_below_checkpoint(
        self, reducer_name: str, retain_through_ingest_seq: int | None = None
    ) -> tuple[int, int]:
        """Delete events strictly below the reducer checkpoint floor.

        Enforces the invariant: **the WAL is NEVER pruned below the reducer
        checkpoint.** The floor is `min(last_ingest_seq_applied, retain_through_ingest_seq)`
        and only events with `ingest_seq < floor` are eligible for deletion. The event AT
        the checkpoint and everything after it is always retained, so a restart can always
        replay deterministically from the checkpoint forward (`rebuild_from_store` reads
        `fetch_after(checkpoint)`).

        `retain_through_ingest_seq` lets a caller hold an even earlier floor (e.g. an
        archival/age-based retention window that has not yet copied a run to NAS JSONL);
        it can only *lower* the floor, never raise it above the checkpoint.

        Returns `(floor, deleted_count)`. Deleting nothing when the checkpoint is 0
        (reducer has applied nothing) is correct and required: with no durable high-water
        mark there is no safe floor, so we prune nothing.
        """
        self._ensure_initialized()
        checkpoint = self.get_checkpoint(reducer_name)
        floor = checkpoint
        if retain_through_ingest_seq is not None:
            floor = min(floor, int(retain_through_ingest_seq))
        if floor <= 0:
            return 0, 0
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute("DELETE FROM events WHERE ingest_seq < ?", (floor,))
                conn.commit()
                return floor, int(cur.rowcount)

    def min_ingest_seq(self) -> int:
        row = self.query("SELECT COALESCE(MIN(ingest_seq), 0) AS min_seq FROM events")[0]
        return int(row["min_seq"])

    def missing_ingest_seqs(self) -> set[int]:
        """ingest_seqs absent from the WAL within [min, max] — rolled-back AUTOINCREMENT holes.

        These slots were allocated by `INSERT` then never committed (e.g. a duplicate/rolled-back
        transaction). They will never appear, so the reducer must skip them when advancing the
        global replay watermark rather than stall waiting for a row that cannot arrive.
        """
        present = {int(row["ingest_seq"]) for row in self.query("SELECT ingest_seq FROM events")}
        if not present:
            return set()
        return set(range(min(present), max(present) + 1)) - present

    def wal_size_bytes(self) -> int:
        """Total on-disk size of the SQLite database and its WAL sidecar files.

        Sums the main db, the `-wal` write-ahead log, and the `-shm` shared-memory index.
        Returns 0 for an in-memory database (no files on disk).
        """
        if str(self.db_path) == ":memory:":
            return 0
        total = 0
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_path) + suffix)
            try:
                total += path.stat().st_size
            except OSError:
                continue
        return total

    def wal_reachable(self) -> bool:
        try:
            self.query("SELECT 1")
        except sqlite3.Error:
            return False
        return True

    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        self._ensure_initialized()
        with self._connect() as conn:
            return list(conn.execute(sql, params).fetchall())

    def _event_rows(self, sql: str, params: tuple[Any, ...]) -> list[EventRow]:
        self._ensure_initialized()
        with self._connect() as conn:
            return [EventRow.from_sqlite(row) for row in conn.execute(sql, params).fetchall()]

    def _ensure_initialized(self) -> None:
        if not self._initialized:
            self.initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn
