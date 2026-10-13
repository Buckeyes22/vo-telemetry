# Engineering decisions and limitations

[Architecture](architecture.md) · [Validation](testing.md) · [Operations](operations.md)

This document separates demonstrated behavior from design intent. Decisions are
inferred from the implementation and tests, rather than presented as historical
ADRs or evidence of employer impact.

## Why two planes

OTel trace IDs describe causal execution. Liveness run/agent/tool IDs identify
long-lived actors and lifecycle facts. Keeping them separate avoids equating a
quiet trace stream with a completed agent. Shims carry trace context while
emitting their own liveness records. The repository includes those adapters and a local trace-only collector
template; a full metrics/logs/traces backend is not part of this project.

## Why a durable producer outbox

A hook can exit before the backend recovers. SQLite moves retry ownership to
a long-lived forwarder and lets the producer append without waiting for remote
acknowledgement. FIFO protects each source's semantics. Event IDs handle repeat
delivery. The tradeoff is more local state and an operational need to observe
queue/disk growth. A backend outage is decoupled from the child CLI, but a local
disk failure still matters.

The queue's event target is soft: lifecycle/terminal events remain protected even
when the target is exceeded. Heartbeats, activity and token deltas are eligible
for shedding, with explicit loss reports. A bounded spool is separate from the
outbox and does not imply a hard bound on all telemetry disk usage.

## Why facts and a reducer

One reducer centralizes late-event handling and state transitions. Sequence IDs
support deterministic replay, while timestamps support age projections. Terminal
protection avoids completed tools appearing active after an old heartbeat.
Quarantine preserves malformed attribution for diagnosis instead of silently
merging it into an unrelated lane.

SQLite WAL provides a small inspectable single-node store with standard-library
dependencies. It does not provide replication, consensus or high availability.
Startup replays retained events; the checkpoint is a watermark, not a durable
full-state snapshot. This simplicity makes local reproduction practical but
requires care with retention and increasing event volume.

## Why explicit cost provenance

Provider-reported cost, pricing-table estimates and unavailable cost are distinct.
Exact model identifiers matter. Unknown rates remain unavailable instead of being
guessed. The dashboard evaluates visual ceilings from event provenance and lane
totals. It does not stop jobs, enforce a payment limit or prove invoice accuracy.

## Why governed dry-run notification

Live incident feeds can amplify errors through repeated actions. The watcher
defaults to dry-run and no allowed actions; symbolic action names pass through
idempotency, cooldown and intervention limits before dispatch. Telegram is the
implemented external action. Autonomous shell remediation is not implemented.
Governance state is in memory, so restarts reset budgets and incident keys.
Persistent governance would be needed before relying on cross-restart limits.

## Known implementation limits

| Limit | Consequence and possible remedy |
| --- | --- |
| Minimal HTTP server; GETs have no auth; no built-in TLS | Keep local/private; hostile-network deployment needs protocol/access hardening |
| Full message disabled but preview/summary/extension/spool data retained | Review collection policy; redaction is not comprehensive encryption or scrubbing |
| Counter and cost high-water state rebuilt from retained WAL | Prune plus restart can alter counter reconstruction or double-count re-reported cost; durable reducer state is a future improvement |
| Orphan reaping occurs during snapshot/metrics reads | Transition timing can depend on observation cadence; tick-based reaping is a future design option |
| Closed run can reap a lane before a late failure stop | Terminal protection preserves abandonment instead of reclassifying it as failed; ordering needs deliberate reconciliation policy |
| Lane deltas use event change versions | Time-only projections can age without a new delta; clients must reconcile periodically |
| No automatic retention/archive scheduler | Defined config fields do not imply scheduled data expiry |
| Soft outbox target and unpruned sent rows | Monitor real disk usage; no global hard storage bound is claimed |
| Producer-provided metric label values | Omitting IDs reduces risk but does not enforce fixed provider/orchestrator cardinality |
| Lane table uses raw accumulated cost; rollup carries availability provenance | A per-lane zero without provenance is not proof of free execution |
| UI and provider adapters have version-sensitive interfaces | Unit/headless checks do not replace live CLI/terminal compatibility verification |

The SQLite append/read error path returns retryable `503` in covered tests.
Do not generalize that to every later checkpoint/reduction error or every HTTP
request shape. The runtime is not described here as a complete production ingress.

## Maturity

**Implemented and locally tested:** backend/reducer, producer unit behavior,
watcher governance, dashboard pure functions, HTTP smoke and actual synthetic
dashboard rendering.

**Implemented but not live-validated here:** provider CLI integrations, Linux
hook/systemd installation, live Telegram notification and interactive terminal
use across environments.

**Illustrative:** loopback Compose setup and trace collector template. Compose
configuration parses; no container or collector deployment result is claimed.

**Future improvements, not completed work:** durable reducer/governance snapshots,
scheduled retention/backup, stronger ingress hardening, scale characterization
and broader compatibility testing. These are not implemented.
