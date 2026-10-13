# Architecture and invariants

The observability plane transports OTel logs, metrics and traces. The liveness
plane transports facts about runs, lanes and tools. `TRACEPARENT` and trace IDs
describe distributed trace hierarchy; run, agent and tool IDs describe lane
attribution. Neither replaces the other.

![Code-derived two-plane component architecture.](images/architecture.svg)

## Liveness flow

```text
hooks / supervised CLI shims / transcript tailer
    -> durable SQLite outbox
    -> FIFO forwarder with retry and priority shedding
    -> token-authenticated HTTP ingest
    -> canonical SQLite WAL and reducer checkpoint
    -> in-memory run / lane / tool snapshots
    -> dashboard and read-only governed watcher
```

Producers emit facts; the reducer derives stale, stuck and terminal state.
Silence projects stale/stuck; lack of progress despite heartbeats can project stalled. A final message alone
does not prove success. `lane_stopped` carries a declared outcome, and the
reducer combines it with child tool outcomes.

`event_id` provides deduplication, `source_seq` orders events within a source,
and server-assigned `ingest_seq` gives canonical replay order. Terminal states
must not regress under replay. Missing attribution goes to a deterministic
quarantine lane. Run-level events may legitimately omit `agent_id`.

Lifecycle and terminal events are never deliberately shed by the outbox.
Heartbeat, activity, then token delta events are the shedding candidates;
loss is reported as `producer_dropped_events`. The reducer gap buffer is a
secondary protection, while the forwarder owns per-source FIFO.

Ingest bounds body size, timestamps, identifiers, known fields and content.
Full final messages are disabled by default; previews and parsed summaries
remain sensitive. SQLite WAL records support replay and checkpoint recovery.

## Observability example

[`examples/collector.yaml`](../examples/collector.yaml) supplies edge and gateway OTLP receivers. They have
different ports and different persistent queue directories. Identity uses
stable host, service, namespace and deployment attributes. Ephemeral run IDs
belong in event/log/trace fields, not metric labels. The example drops prompt,
response and database statement attributes and exports to local debug output.
Real collector routing, storage backends, sampling and authentication require
operator configuration and version-specific validation.

## Packages

| Package | Purpose |
| --- | --- |
| `src/liveness` | HTTP ingest, strict schema, SQLite store and reducer |
| `src/cc-host/cc_host` | Hooks, outbox, forwarder, transcript tailer and shims |
| `src/dashboards` | Passive Textual dashboard and polling loop |
| `src/watcher` | Incident analysis, governed actions, audit and notification adapter |

The watcher defaults to dry-run and an empty allowed-action set. Telegram
notification is optional and requires explicit owner configuration. Tests use
mocked notification transport and synthetic snapshots.
