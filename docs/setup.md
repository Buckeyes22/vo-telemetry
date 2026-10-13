# Setup and integration

[README](../README.md) · [Configuration](configuration.md) · [Troubleshooting](operations.md)

## Requirements

Python 3.11+ is required (`datetime.UTC`, modern type syntax). CI runs on
3.11, 3.12 and 3.13. The backend and watcher use only the standard library.
PyYAML is needed for producer pricing; Textual is optional for the dashboard.
All shell commands below run from the repository root on macOS or Linux.
The hook/systemd installer is Linux-specific. Windows shell setup is untested.

## Disposable demonstration

```bash
PYTHONPATH=src python3 examples/local_demo.py
```

The demo generates an ingest token, binds only to loopback, sends six initial
synthetic lifecycle events over HTTP and keeps the run open with one completed
lane, so `/runs/current` can populate the dashboard selector. It prints the
run ID and direct snapshot URL and refreshes run activity every 30 seconds.
It reads no model credentials or transcripts. Data lives in a temporary
directory; Ctrl-C sends the closing event, shuts down the server and removes
the data. Use `--port 8724` if the default port is occupied.

From another terminal:

```bash
curl --fail http://127.0.0.1:8723/healthz
curl --fail http://127.0.0.1:8723/readyz
curl --fail http://127.0.0.1:8723/runs/current
curl --fail http://127.0.0.1:8723/metrics
```

The completed lane is visible inside an open run, so the active-lane gauge
need not be nonzero. A terminal lane is not an active workload.
Use [API documentation](api-and-data.md) to inspect the run and its event page.

## Persistent local backend

Use this path when you want a database that survives shutdown:

```bash
python3 -m venv .venv
. .venv/bin/activate
mkdir -p data/liveness
export LIVENESS_DB_PATH="$PWD/data/liveness/events.db"
export LIVENESS_BIND_HOST=127.0.0.1
export LIVENESS_BIND_PORT=8723
export LIVENESS_INGEST_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
PYTHONPATH=src python -m liveness.server
```

Do not run this alongside the disposable demo on the same port. Store the token
through your own local secret mechanism if producers need to reconnect after
restart. Regenerating it changes which producers may ingest. The process does
not automatically read `.env`; [the configuration template](../examples/config.env.example)
is an inventory of environment settings, not an automatically loaded config file.

To send the synthetic run from another shell, securely supply the same
`LIVENESS_INGEST_TOKEN` there, then run:

```bash
. .venv/bin/activate
python examples/send_synthetic_run.py
```

The sender defaults to loopback port 8723; `DEMO_API_BASE` overrides it.

## Optional dashboard

```bash
. .venv/bin/activate
python -m pip install -r requirements-dashboard.txt
PYTHONPATH=src python -m dashboards.subagent_dashboard --api-base http://127.0.0.1:8723
```

Use `q` to quit, `r` to refresh, `o` to cycle orchestrator filters, `s` to filter
run state, `f` to clear filters, and `j`/`k` to select a run. `t` displays a
locally available bounded spool tail; it does not fetch spool content from the
backend. No spool is supplied by the synthetic demo. Cost-ceiling indicators
are visual alerts; they do not enforce spending limits or stop jobs.

## Optional watcher

```bash
export WATCHER_API_BASE=http://127.0.0.1:8723
export WATCHER_DRY_RUN=true
export WATCHER_ALLOWED_ACTIONS=
export WATCHER_AUDIT_LOG="$PWD/data/watcher-audit.jsonl"
PYTHONPATH=src python3 -m watcher
```

The watcher polls every 15 seconds by default and analyzes open runs. The completed demo lane will not produce an active-stall alert. Dry-run mode
performs local analysis/audit but sends no notifications. Telegram is the
implemented external action; tests use mocked transport.

## Producer integration

Review the [producer guide](../src/cc-host/README.md),
[shims](../src/cc-host/shims/README.md), and [data-handling limits](engineering-decisions.md)
before collecting real agent runs. Configure `CLAUDE_TELEMETRY_HOME`,
`CC_HOST_OUTBOX_DIR`, `LIVENESS_INGEST_URL` and `LIVENESS_INGEST_TOKEN`.
The forwarder also reads its configured `CC_HOST_LIVENESS_ENV_FILE`; environment
values already present take precedence over that file.

```bash
. .venv/bin/activate
python -m pip install -r src/cc-host/requirements.txt
PYTHONPATH=src/cc-host python -m cc_host.forwarder --once
```

`--once` attempts at most one eligible event, not a complete queue drain. Hook
installation is optional: `src/cc-host/install.sh` modifies Claude settings,
writes forwarder/tailer systemd user units and enables services. Even
`--no-systemctl` still writes hooks and unit files. Inspect the installer before
running it on a real user account; it is not part of the quick start or tests.

## Container and collector templates

```bash
# With a freshly generated LIVENESS_INGEST_TOKEN already exported:
docker compose -f examples/compose.yaml config --quiet
# Optional: builds/starts a container and creates a persistent named volume.
docker compose -f examples/compose.yaml up --build
```

The host port binds to loopback; the container listens on its internal interfaces.
Stopping with `down` preserves the named volume unless you request
volume deletion. Image pulls and builds require network access.

`examples/collector.yaml` is an illustrative `otelcol-contrib` trace pipeline.
It requires writable queue directories and validation against your installed
collector version. No collector binary or production backend is bundled.
