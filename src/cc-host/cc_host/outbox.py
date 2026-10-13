from __future__ import annotations

import json
import random
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .events import DROP_ORDER, DROPPABLE_EVENT_TYPES, build_event
from .paths import outbox_db_path
from .util import ensure_private_dir, rfc3339, utc_now


# --- Outbox capacity envelope ---
# The outbox is bounded by EVENT COUNT, not bytes: apply_priority_drop() fires when the
# unsent count exceeds DEFAULT_MAX_EVENTS. The byte budget is derived from this count
# rather than configured separately, so the two limits cannot disagree.
#
#   DEFAULT_MAX_EVENTS         = 10_000 events   (binding cap; the drop loop's threshold)
#   AVG_EVENT_BYTES_JSON       = 756  B/event    (mean payload_json length for typical lifecycle events)
#   AVG_EVENT_BYTES_ON_DISK    = 1281 B/event    (schema + 3 indices filled to 10k rows, then VACUUM INTO)
#   => disk_budget = 10_000 * 1281 B ≈ 12.2 MiB SQLite main + ~4 MiB WAL sidecar ≈ 17 MiB worst case.
DEFAULT_MAX_EVENTS = 10_000
AVG_EVENT_BYTES_JSON = 756
AVG_EVENT_BYTES_ON_DISK = 1281
OUTBOX_DISK_BUDGET_BYTES = DEFAULT_MAX_EVENTS * AVG_EVENT_BYTES_ON_DISK  # ≈ 12.2 MiB main DB


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS outbox_events (
  id                 INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id           TEXT UNIQUE NOT NULL,
  event_type         TEXT NOT NULL,
  run_id             TEXT NOT NULL,
  session_id         TEXT NOT NULL,
  agent_id           TEXT,
  source             TEXT NOT NULL,
  source_instance_id TEXT NOT NULL,
  source_seq         INTEGER NOT NULL,
  priority           INTEGER NOT NULL,
  payload_json       TEXT NOT NULL,
  attempts           INTEGER NOT NULL DEFAULT 0,
  next_attempt_at    TEXT NOT NULL,
  last_error         TEXT,
  created_at         TEXT NOT NULL,
  sent_at            TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_event_id ON outbox_events(event_id);
CREATE INDEX IF NOT EXISTS idx_outbox_unsent ON outbox_events(sent_at, next_attempt_at, created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_source ON outbox_events(source_instance_id, source_seq);
CREATE TABLE IF NOT EXISTS outbox_drop_reports (
  id                       INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id                   TEXT NOT NULL,
  session_id               TEXT NOT NULL,
  agent_id                 TEXT NOT NULL,
  source_instance_id       TEXT NOT NULL,
  quarantine               INTEGER NOT NULL DEFAULT 0,
  identity_error           TEXT,
  agent_type               TEXT,
  provider                 TEXT,
  model                    TEXT,
  dropped_count            INTEGER NOT NULL,
  dropped_event_types_json TEXT NOT NULL,
  created_at               TEXT NOT NULL,
  reported_at              TEXT
);
CREATE INDEX IF NOT EXISTS idx_drop_reports_pending ON outbox_drop_reports(reported_at, created_at);
"""


@dataclass(frozen=True)
class OutboxRow:
    id: int
    event: dict[str, Any]
    attempts: int


@dataclass(frozen=True)
class DropReport:
    id: int
    run_id: str
    session_id: str
    agent_id: str
    source_instance_id: str
    dropped_count: int
    dropped_event_types_json: str
    quarantine: bool = False
    identity_error: str | None = None
    agent_type: str | None = None
    provider: str | None = None
    model: str | None = None


class Outbox:
    def __init__(self, db_path: str | Path | None = None, max_events: int = DEFAULT_MAX_EVENTS):
        self.db_path = Path(db_path) if db_path is not None else outbox_db_path()
        self.max_events = max_events
        self.initialize()

    def initialize(self) -> None:
        if str(self.db_path) != ":memory:":
            ensure_private_dir(self.db_path.parent)
        with self._connect() as conn:
            conn.executescript(SCHEMA_SQL)
            conn.commit()

    def append(self, event: dict[str, Any]) -> bool:
        now = rfc3339()
        payload_json = json.dumps(event, sort_keys=True, separators=(",", ":"))
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO outbox_events (
                  event_id, event_type, run_id, session_id, agent_id, source,
                  source_instance_id, source_seq, priority, payload_json,
                  next_attempt_at, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event["event_id"],
                    event["event_type"],
                    event["run_id"],
                    event["session_id"],
                    event.get("agent_id"),
                    event["source"],
                    event["source_instance_id"],
                    int(event["source_seq"]),
                    event_priority(str(event["event_type"])),
                    payload_json,
                    "1970-01-01T00:00:00Z",
                    now,
                ),
            )
            inserted = cur.rowcount > 0
            conn.commit()
        self.apply_priority_drop()
        return inserted

    def unsent_rows(self) -> list[OutboxRow]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM outbox_events WHERE sent_at IS NULL ORDER BY created_at, id").fetchall()
        return [_row_to_outbox(row) for row in rows]

    def next_sendable(self, now: datetime | None = None) -> OutboxRow | None:
        now_text = rfc3339(now)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM outbox_events AS candidate
                WHERE candidate.sent_at IS NULL
                  AND candidate.next_attempt_at <= ?
                  AND NOT EXISTS (
                    SELECT 1
                    FROM outbox_events AS older
                    WHERE older.sent_at IS NULL
                      AND older.source_instance_id = candidate.source_instance_id
                      AND older.source_seq < candidate.source_seq
                  )
                ORDER BY candidate.created_at, candidate.id
                LIMIT 1
                """,
                (now_text,),
            ).fetchone()
        return _row_to_outbox(row) if row is not None else None

    def mark_sent(self, row_id: int, when: datetime | None = None) -> None:
        with self._connect() as conn:
            conn.execute("UPDATE outbox_events SET sent_at = ? WHERE id = ?", (rfc3339(when), row_id))
            conn.commit()

    def mark_failed(
        self,
        row_id: int,
        error: str,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        when: datetime | None = None,
    ) -> None:
        now = when or utc_now()
        with self._connect() as conn:
            row = conn.execute("SELECT attempts FROM outbox_events WHERE id = ?", (row_id,)).fetchone()
            attempts = int(row["attempts"]) + 1 if row is not None else 1
            delay = min(max_delay, initial_delay * (2 ** max(0, attempts - 1)))
            delay = delay * random.uniform(0.8, 1.2)
            next_attempt = now + timedelta(seconds=delay)
            conn.execute(
                """
                UPDATE outbox_events
                SET attempts = ?, next_attempt_at = ?, last_error = ?
                WHERE id = ?
                """,
                (attempts, rfc3339(next_attempt), error[:1024], row_id),
            )
            conn.commit()

    def apply_priority_drop(self) -> int:
        dropped_total = 0
        while self._unsent_count() > self.max_events:
            dropped = False
            for event_type in DROP_ORDER:
                row = self._oldest_droppable(event_type)
                if row is None:
                    continue
                event = json.loads(row["payload_json"])
                self._record_drop(event)
                with self._connect() as conn:
                    conn.execute("DELETE FROM outbox_events WHERE id = ?", (int(row["id"]),))
                    conn.commit()
                dropped_total += 1
                dropped = True
                break
            if not dropped:
                break
        return dropped_total

    def pending_drop_reports(self) -> list[DropReport]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM outbox_drop_reports WHERE reported_at IS NULL ORDER BY created_at, id"
            ).fetchall()
        return [_row_to_drop(row) for row in rows]

    def enqueue_drop_reports(self, now: datetime | None = None) -> list[OutboxRow]:
        queued: list[OutboxRow] = []
        for report in self.pending_drop_reports():
            event = build_event(
                event_type="producer_dropped_events",
                ts=now,
                run_id=report.run_id,
                session_id=report.session_id,
                orchestrator="claude-code",
                agent_id=report.agent_id,
                quarantine=report.quarantine,
                identity_error=report.identity_error,
                agent_type=report.agent_type,
                provider=report.provider,
                model=report.model,
                source="shim",
                source_instance_id=f"shim:producer-drop:{report.id}",
                source_seq=1,
                dropped_count=report.dropped_count,
                dropped_event_types=json.loads(report.dropped_event_types_json),
                loss_scope="non_terminal_only",
            )
            self.append(event)
            with self._connect() as conn:
                row = conn.execute("SELECT * FROM outbox_events WHERE event_id = ?", (event["event_id"],)).fetchone()
                conn.execute("UPDATE outbox_drop_reports SET reported_at = ? WHERE id = ?", (rfc3339(now), report.id))
                conn.commit()
            if row is not None:
                queued.append(_row_to_outbox(row))
        return queued

    def prune_sent(self, older_than: datetime) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM outbox_events WHERE sent_at IS NOT NULL AND sent_at < ?", (rfc3339(older_than),)
            )
            conn.commit()
            return int(cur.rowcount)

    def _unsent_count(self) -> int:
        with self._connect() as conn:
            return int(conn.execute("SELECT COUNT(*) AS c FROM outbox_events WHERE sent_at IS NULL").fetchone()["c"])

    def _oldest_droppable(self, event_type: str) -> sqlite3.Row | None:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT * FROM outbox_events
                WHERE sent_at IS NULL AND event_type = ?
                ORDER BY created_at, id
                LIMIT 1
                """,
                (event_type,),
            ).fetchone()

    def _record_drop(self, event: dict[str, Any]) -> None:
        event_type = str(event["event_type"])
        if event_type not in DROPPABLE_EVENT_TYPES:
            return
        agent_id = event.get("agent_id")
        if not isinstance(agent_id, str) or not agent_id:
            agent_id = "quarantine:unknown"
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM outbox_drop_reports
                WHERE reported_at IS NULL
                  AND run_id = ?
                  AND agent_id = ?
                  AND source_instance_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (event["run_id"], agent_id, event["source_instance_id"]),
            ).fetchone()
            if row is None:
                types = [event_type]
                conn.execute(
                    """
                    INSERT INTO outbox_drop_reports (
                      run_id, session_id, agent_id, source_instance_id, quarantine,
                      identity_error, agent_type, provider, model, dropped_count,
                      dropped_event_types_json, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event["run_id"],
                        event["session_id"],
                        agent_id,
                        event["source_instance_id"],
                        1 if event.get("quarantine") else 0,
                        event.get("identity_error"),
                        event.get("agent_type"),
                        event.get("provider"),
                        event.get("model"),
                        1,
                        json.dumps(types),
                        rfc3339(),
                    ),
                )
            else:
                types = json.loads(row["dropped_event_types_json"])
                if event_type not in types:
                    types.append(event_type)
                    types.sort(key=lambda item: DROP_ORDER.index(item) if item in DROP_ORDER else 99)
                conn.execute(
                    """
                    UPDATE outbox_drop_reports
                    SET dropped_count = dropped_count + 1,
                        dropped_event_types_json = ?
                    WHERE id = ?
                    """,
                    (json.dumps(types), int(row["id"])),
                )
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn


def event_priority(event_type: str) -> int:
    if event_type == "tool_heartbeat":
        return 30
    if event_type == "tool_activity":
        return 40
    if event_type == "token_delta":
        return 50
    return 10


def _row_to_outbox(row: sqlite3.Row) -> OutboxRow:
    return OutboxRow(id=int(row["id"]), event=json.loads(row["payload_json"]), attempts=int(row["attempts"]))


def _row_to_drop(row: sqlite3.Row) -> DropReport:
    return DropReport(
        id=int(row["id"]),
        run_id=str(row["run_id"]),
        session_id=str(row["session_id"]),
        agent_id=str(row["agent_id"]),
        source_instance_id=str(row["source_instance_id"]),
        dropped_count=int(row["dropped_count"]),
        dropped_event_types_json=str(row["dropped_event_types_json"]),
        quarantine=bool(row["quarantine"]),
        identity_error=row["identity_error"],
        agent_type=row["agent_type"],
        provider=row["provider"],
        model=row["model"],
    )
