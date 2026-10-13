from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class EventRow:
    ingest_seq: int
    event_id: str
    run_id: str
    agent_id: str | None
    event_type: str
    ts: str
    source_instance_id: str
    source_seq: int
    payload_json: str

    @classmethod
    def from_sqlite(cls, row: sqlite3.Row) -> "EventRow":
        return cls(
            ingest_seq=int(row["ingest_seq"]),
            event_id=str(row["event_id"]),
            run_id=str(row["run_id"]),
            agent_id=row["agent_id"],
            event_type=str(row["event_type"]),
            ts=str(row["ts"]),
            source_instance_id=str(row["source_instance_id"]),
            source_seq=int(row["source_seq"]),
            payload_json=str(row["payload_json"]),
        )
