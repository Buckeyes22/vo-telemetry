from __future__ import annotations

from .reducer import MetricsSnapshot


def prometheus_text(snapshot: MetricsSnapshot) -> str:
    lines = [
        "# HELP subagent_lanes_running Current lanes projected as running.",
        "# TYPE subagent_lanes_running gauge",
    ]
    for (provider, orchestrator), value in sorted(snapshot.lanes_running.items()):
        lines.append(_sample("subagent_lanes_running", value, provider=provider, orchestrator=orchestrator))

    lines.extend(
        [
            "# HELP subagent_lanes_stale Current lanes projected as stale or stuck.",
            "# TYPE subagent_lanes_stale gauge",
        ]
    )
    for (provider, orchestrator), value in sorted(snapshot.lanes_stale.items()):
        lines.append(_sample("subagent_lanes_stale", value, provider=provider, orchestrator=orchestrator))

    lines.extend(
        [
            "# HELP subagent_lanes_stalled Lanes heartbeating but emitting no real progress past the "
            "stall window (hung/zombie children the silence-based stale/stuck detector misses).",
            "# TYPE subagent_lanes_stalled gauge",
        ]
    )
    for (provider, orchestrator), value in sorted(snapshot.lanes_stalled.items()):
        lines.append(_sample("subagent_lanes_stalled", value, provider=provider, orchestrator=orchestrator))

    lines.extend(
        [
            "# HELP subagent_lanes_failed_total Total lanes that reached failed terminal state.",
            "# TYPE subagent_lanes_failed_total counter",
        ]
    )
    for (provider, orchestrator), value in sorted(snapshot.lanes_failed_total.items()):
        lines.append(_sample("subagent_lanes_failed_total", value, provider=provider, orchestrator=orchestrator))

    lines.extend(
        [
            "# HELP subagent_lanes_abandoned_total Lanes reaped to a terminal abandoned/crashed state "
            "(orphans: lane_started with no lane_stopped; parent run closed, abandoned, or crashed).",
            "# TYPE subagent_lanes_abandoned_total counter",
        ]
    )
    for (provider, orchestrator), value in sorted(snapshot.lanes_abandoned_total.items()):
        lines.append(_sample("subagent_lanes_abandoned_total", value, provider=provider, orchestrator=orchestrator))

    lines.extend(
        [
            "# HELP subagent_tokens_total Token deltas processed by direction.",
            "# TYPE subagent_tokens_total counter",
        ]
    )
    for (provider, orchestrator, direction), token_value in sorted(snapshot.tokens.items()):
        lines.append(
            _sample(
                "subagent_tokens_total",
                token_value,
                provider=provider,
                orchestrator=orchestrator,
                direction=direction,
            )
        )

    lines.extend(
        [
            "# HELP subagent_cost_usd_total Total cost reported by liveness events.",
            "# TYPE subagent_cost_usd_total counter",
        ]
    )
    for (provider, orchestrator), cost_value in sorted(snapshot.cost.items()):
        lines.append(_sample("subagent_cost_usd_total", cost_value, provider=provider, orchestrator=orchestrator))

    # --- Control-plane health for the liveness plane. All reducer-derived. ---
    lines.extend(
        [
            "# HELP liveness_ingest_requests_total Liveness ingest requests by result.",
            "# TYPE liveness_ingest_requests_total counter",
        ]
    )
    for result, value in sorted(snapshot.ingest_requests_total.items()):
        lines.append(_sample("liveness_ingest_requests_total", value, result=result))

    lines.extend(
        [
            "# HELP liveness_reducer_last_ingest_seq_applied Highest ingest_seq the reducer has applied.",
            "# TYPE liveness_reducer_last_ingest_seq_applied gauge",
            f"liveness_reducer_last_ingest_seq_applied {snapshot.last_ingest_seq_applied}",
            "# HELP liveness_wal_max_ingest_seq Highest ingest_seq durably written to the WAL.",
            "# TYPE liveness_wal_max_ingest_seq gauge",
            f"liveness_wal_max_ingest_seq {snapshot.max_ingest_seq}",
            "# HELP liveness_reducer_lag_events Reducer lag: max_ingest_seq - last_ingest_seq_applied.",
            "# TYPE liveness_reducer_lag_events gauge",
            f"liveness_reducer_lag_events {snapshot.reducer_lag_events}",
            "# HELP liveness_wal_size_bytes On-disk size of the SQLite WAL files (db + -wal + -shm).",
            "# TYPE liveness_wal_size_bytes gauge",
            f"liveness_wal_size_bytes {snapshot.wal_size_bytes}",
        ]
    )

    lines.extend(
        [
            "# HELP liveness_source_backlog_events Reducer-visible per-source gap-buffer depth (producer backlog proxy).",
            "# TYPE liveness_source_backlog_events gauge",
        ]
    )
    for source_kind, value in sorted(snapshot.source_backlog_events.items()):
        lines.append(_sample("liveness_source_backlog_events", value, source_kind=source_kind))

    # Producer-reported loss. Loss must be visible, never silent, so emit a zero
    # baseline keyed (provider=all, orchestrator=all) when no drops have been reported — the
    # panel/PromQL resolves to 0 rather than "No data". Reducer-derived rollup, low-cardinality.
    lines.extend(
        [
            "# HELP liveness_producer_dropped_events_total Producer-reported dropped non-terminal events .",
            "# TYPE liveness_producer_dropped_events_total counter",
        ]
    )
    if snapshot.producer_dropped_events_total:
        for (provider, orchestrator), value in sorted(snapshot.producer_dropped_events_total.items()):
            lines.append(
                _sample(
                    "liveness_producer_dropped_events_total",
                    value,
                    provider=provider,
                    orchestrator=orchestrator,
                )
            )
    else:
        lines.append(_sample("liveness_producer_dropped_events_total", 0, provider="all", orchestrator="all"))

    lines.append("")
    return "\n".join(lines)


def _sample(name: str, value: float | int, **labels: str) -> str:
    rendered_labels = ",".join(f'{key}="{_escape(label)}"' for key, label in sorted(labels.items()))
    return f"{name}{{{rendered_labels}}} {value}"


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
