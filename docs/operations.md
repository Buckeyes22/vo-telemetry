# Operations and troubleshooting

[Setup](setup.md) · [Configuration](configuration.md) · [Engineering limits](engineering-decisions.md)

This guide covers a local single-node process.

## Check the control plane first

```bash
curl --fail http://127.0.0.1:8723/healthz
curl --fail http://127.0.0.1:8723/readyz
curl --fail http://127.0.0.1:8723/metrics
```

Health is process state. Readiness additionally checks WAL access, reducer
initialization, progress and pending source gaps. A healthy but unready service
should not be described as fully caught up. The dashboard gates initial rendering
on readiness and distinguishes an unavailable process from a data-endpoint error.

| Signal | What it can establish | Important limit |
| --- | --- | --- |
| `liveness_reducer_lag_events` | Gap between durable high-water and applied progress | Does not count unsent producer rows |
| `liveness_source_backlog_events` | Reducer source gap-buffer depth | A server-side proxy, not direct producer outbox size |
| `liveness_wal_size_bytes` | Database/WAL/shared-memory file size | Not a disk-capacity alert by itself |
| `liveness_producer_dropped_events_total` | Producer-reported shedding | Report arrives after forwarder recovery; loss is not instantly visible |
| `subagent_lanes_stale` / `subagent_lanes_stalled` | Silence versus no-progress projections in live runs | Healthy heartbeat does not prove useful work |
| `liveness_ingest_requests_total` | Accepted, duplicate and error request counts | In-memory counters reset on process restart |

Metrics omit run/agent IDs from labels. Provider/orchestrator values still come
from producers; control those values rather than assuming cardinality is bounded
simply because IDs are absent.

## Diagnose common failures

| Symptom | Check | Next step |
| --- | --- | --- |
| Demo cannot bind | Another process uses the port | Choose an unused `--port`; leave the existing process alone |
| Ingest `401` | Missing, empty or mismatched token | Export the same local token in backend and producer environments; restart backend after configuration changes |
| Ingest `400` | Error field, clock skew, source prefix, unknown fields | Use the generated sender; compare with the schema; put extensions under `payload.ext` |
| Ingest `413` | Request body versus configured limit | Bound payload content at producer; do not raise limits blindly |
| Readiness `503` | WAL reachability, progress and pending gaps | Check filesystem access, disk space, source sequences and backend error response |
| Queue is not draining | URL/token, failed older row, backoff timing | Verify connectivity and oldest unsent source row; `--once` sends at most one eligible event |
| Dashboard remains empty | API base, readiness, selected run and filters | Query `/runs/current`; clear filters with `f`; the served demo run stays open with a completed lane; closed runs require their specific snapshot URL |
| No cost shown | Event provenance and pricing model key | Preserve unavailable values; do not infer a rate from a similar model name |
| Watcher sends nothing | Dry-run and empty action allowlist | Expected default; inspect local audit decisions before configuring live notification |
| Audit logging falls back to stderr | Unwritable parent directory | Supply a writable local audit path |

The forwarder retries non-success HTTP/network responses with exponential backoff
and jitter. Per-source FIFO means a failed older row blocks newer rows from that
source. It does not stop other eligible sources. Do not edit queued source
sequences to make a backlog disappear.

## Storage and recovery

The disposable demo removes its temporary database on normal shutdown. A
persistent backend keeps the SQLite main database and live WAL state at your
configured path. Do not delete `-wal`/`-shm` sidecars from a live database as a
cleanup procedure. Use a consistent SQLite backup or stop the owning process
cleanly before making a file-level copy.

Test restoration against a separate temporary path and loopback port. Restart
replay from retained events is covered by the local smoke check. Off-host backup,
archive retention, corruption repair and disaster recovery are not implemented
as an operational workflow.

Retention and pruning need deliberate engineering: the configured retention
fields do not run a scheduler, and a checkpoint alone is not a full materialized
state backup. Pruning history can change reconstructed counters/state after
restart. The producer also retains sent rows until pruning is explicitly invoked.

## Security and sensitive outputs

Keep the server on loopback or place it behind authenticated private access.
Snapshot/event GETs are unauthenticated; the service supplies no TLS or multi-user
authorization. Review token distribution, file permissions and local-user access
before collecting real data. No encrypted storage is provided by the application.

Full final messages are off by default, but previews, summaries, errors, project
paths, nested extension values, spool output and audit logs may be sensitive.
The watcher redacts configured fields before notifications; that is not a
universal scrubber for all producer storage. Avoid attaching raw run databases
or transcript/spool exports to public issues.

The Linux installer writes hooks and enables user services. CLI launchers execute
real tools; `CC_HOST_ALLOW_UNRESTRICTED=1` is an explicit bypass. Review those
side effects before adoption. Cost alarms are advisory display features, not
payment authorization or budget enforcement.
