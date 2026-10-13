# Configuration reference

[Setup](setup.md) · [Operations](operations.md)

Settings are environment variables. The backend's frozen dataclass defaults
are evaluated at module import; export variables **before** starting Python.
The empty ingest-token default rejects all ingest. Values below are source
defaults, not production capacity recommendations.

## Backend

| Variable | Default | Purpose |
| --- | --- | --- |
| `LIVENESS_BIND_HOST` | `127.0.0.1` | HTTP listen address |
| `LIVENESS_BIND_PORT` | `8723` | HTTP port |
| `LIVENESS_DB_PATH` | `./data/liveness/events.db` | Persistent SQLite database |
| `LIVENESS_INGEST_TOKEN` | empty | Required bearer token for POST ingest |
| `LIVENESS_MAX_BODY_BYTES` | `262144` | Request body bound |
| `LIVENESS_TS_PAST_WINDOW_SEC` / `LIVENESS_TS_FUTURE_WINDOW_SEC` | `86400` each | Timestamp sanity window |
| `LIVENESS_STALE_THRESHOLD_SEC` | `60` | Silence threshold for stale projection |
| `LIVENESS_STUCK_THRESHOLD_SEC` | `300` | Silence threshold for stuck projection |
| `LIVENESS_STALL_THRESHOLD_SEC` | `600` | No-progress threshold despite heartbeats |
| `LIVENESS_GAP_TIMEOUT_SEC` | `30` | Reducer source-sequence gap timeout |
| `LIVENESS_RUN_ABANDON_THRESHOLD_SEC` | `3600` | Run inactivity abandonment threshold |
| `LIVENESS_MAX_EVENTS_PAGE_SIZE` | `1000` | Maximum event page size |
| `LIVENESS_STORE_FULL_FINAL_MESSAGE` | `false` | Opt-in full final-message storage |
| `LIVENESS_FINAL_MESSAGE_PREVIEW_CHARS` | `256` | Preview bound; preview is still sensitive |

Identifier bounds are 256 characters for run IDs, 128 for agent IDs and 64 for
event IDs. Text bounds include summary/activity 512, worktree 256, model 160 and
error 1024. See the complete names and defaults in
[`LivenessConfig`](../src/liveness/config.py) and the actual transformations in
[`schema.py`](../src/liveness/schema.py).

`LIVENESS_RETENTION_DAYS` (14) and `LIVENESS_ARCHIVE_DIR` are defined configuration
fields, but this server entry point does not schedule retention or archive jobs.
Do not infer automatic data expiry from setting them.

## Producer, forwarder and shim

| Variable | Default | Purpose |
| --- | --- | --- |
| `CLAUDE_TELEMETRY_HOME` | `~/.claude/telemetry` | Producer state, pricing, spool and logs |
| `CC_HOST_OUTBOX_DIR` / `CC_HOST_OUTBOX_DB` | telemetry home `outbox/`, then `events.db` | Outbox location |
| `LIVENESS_INGEST_URL` | `http://127.0.0.1:8723/ingest` | Forwarder endpoint |
| `CC_HOST_LIVENESS_ENV_FILE` | telemetry home `liveness.env` | Forwarder environment file |
| `CC_HOST_OUTBOX_MAX_EVENTS` | `10000` | Soft unsent-event shedding target |
| `CC_HOST_FORWARDER_POLL_SEC` | `0.5` | Idle queue polling interval |
| `CC_HOST_FORWARDER_INITIAL_BACKOFF_SEC` / `CC_HOST_FORWARDER_MAX_BACKOFF_SEC` | `1` / `60` | Retry range before jitter |
| `CC_HOST_FORWARDER_REQUEST_TIMEOUT_SEC` | `5` | Forwarder request timeout |
| `CC_HOST_ALLOW_UNRESTRICTED` | unset/false; launchers `0` | Explicit CLI sandbox/permission bypass opt-in |
| `CC_HOST_HEARTBEAT_SEC` | `15` | Shim heartbeat cadence |
| `CC_HOST_SPOOL_MAX_BYTES` | `10485760` | Shim spool size bound |
| `CC_HOST_CHILD_TIMEOUT_SEC` | unset; background launchers `3600` | Optional child timeout |
| `CLAUDE_PROJECTS_DIR` | `~/.claude/projects` | Transcript-discovery root |

Terminal/lifecycle records are protected from deliberate shedding. The soft
event target can be exceeded when only protected records remain. Delivered
rows are marked sent; the forwarder does not schedule pruning of sent rows.

Qwen needs `QWEN_OPENAI_BASE_URL`, `QWEN_OPENAI_API_KEY` and a valid `QWEN_MODEL`.
The template model name is a placeholder. There is no bundled key or provider
account. `CC_HOST_PRICING_FILE` can override the empty pricing template; use
exact emitted model strings and verified rates. Unknown cost remains unavailable.

## Watcher and dashboard

| Variable | Default | Purpose |
| --- | --- | --- |
| `WATCHER_API_BASE` | `http://127.0.0.1:8723` | Snapshot API |
| `WATCHER_POLL_INTERVAL` / `WATCHER_API_TIMEOUT` | `15` / `10` | Poll and request seconds |
| `WATCHER_DRY_RUN` | `true` | Prevent external actions |
| `WATCHER_ALLOWED_ACTIONS` | empty | Explicit action allowlist |
| `WATCHER_MAX_INTERVENTIONS_PER_RUN` / `WATCHER_MAX_RETRIES_PER_AGENT` | `3` / `2` | In-memory intervention budgets |
| `WATCHER_COOLDOWN_SECONDS` | `300` | Repeat-action cooldown |
| `WATCHER_LANE_STALL_THRESHOLD` | `120` | Watcher idle threshold |
| `WATCHER_CORRELATED_STALL_QUORUM` | `0.5` | Fraction of active lanes for correlated classification |
| `WATCHER_TRANSIENT_STALL_MAX` | `300` | Transient-versus-escalate threshold |
| `WATCHER_AUDIT_LOG` | `./data/watcher-audit.jsonl` | Local append-only decision log |
| `WATCHER_TELEGRAM_TOKEN` / `WATCHER_TELEGRAM_CHAT_ID` | empty | Optional live notification credentials/destination |

The watcher threshold and reducer threshold are distinct: one supports incident
classification, the other projects backend state. Watcher budgets and incident
keys reset on process restart. An audit file is evidence of prior decisions,
not persistent governance state.

The dashboard uses `--api-base` for the endpoint. `VO_COST_CEILING_RUN` and
`VO_COST_CEILING_<PROVIDER>` override visual cost thresholds. The indicators do
not stop child execution. Full defaults and provider-key handling are in
[`subagent_dashboard.py`](../src/dashboards/subagent_dashboard.py).

## Secrets and collection policy

The [example environment](../examples/config.env.example) intentionally leaves
credential values empty. Never commit live environment files, databases,
outboxes, transcript exports or spool files. Blank full-message storage does not
remove previews, summaries, activity, project paths or nested extension data.
Use synthetic data until you have reviewed collection, access and retention.

The Qwen adapter passes the scoped key through the child's `OPENAI_API_KEY`
environment, never adding it to command arguments. The CLI's [current official
configuration](https://qwenlm.github.io/qwen-code-docs/en/users/configuration/auth/)
documents this mechanism. Environment exposure to privileged local processes
remains a limitation. `CC_HOST_CODEX_CONFIG_FILE` overrides the optional Codex
model-config lookup; no config file is created or modified.
