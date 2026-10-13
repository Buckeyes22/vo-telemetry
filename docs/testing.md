# Testing and evidence

[README](../README.md) · [Setup](setup.md) · [Engineering limits](engineering-decisions.md)

## Reproduce the checks

From the repository root:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.txt -r requirements-dashboard.txt
PYTHONPATH=src:src/cc-host python -m pytest src/liveness/tests src/cc-host/tests src/watcher/tests src/dashboards/tests examples/tests -q
PYTHONPATH=src python examples/local_smoke.py
```

The same commands run in CI on Python 3.11, 3.12 and 3.13 (see
`.github/workflows/ci.yml`), together with `ruff check`, `ruff format --check`
and `mypy`.

| Area | What the checks cover | Source |
| --- | --- | --- |
| Ingest | Authentication, schema/time bounds, dedupe, final-message handling, SQLite corruption responses and actual loopback HTTP | [Backend tests](../src/liveness/tests/test_ingest_api.py) |
| Reduction | Ordering, late events, gap handling, terminal states, replay watermark, orphan handling, progress and metrics | [Reducer tests](../src/liveness/tests/test_reducer.py) |
| Producers | Outbox ordering/shedding, hook events, wrapper guard, CLI parsing/identity and synthetic transcript reconciliation | [Producer tests](../src/cc-host/tests) |
| Watcher | Incident fixtures, governance limits, redaction, dry-run and mocked notification dispatch | [Watcher tests](../src/watcher/tests/test_governed_reaction.py) |
| Dashboard | Pure cost/provenance, event pagination and ceiling evaluation functions | [Dashboard tests](../src/dashboards/tests/test_cost_panels.py) |

The smoke check uses a temporary database and an ephemeral loopback HTTP port.
It verifies invalid-token rejection, seven generated lifecycle events, duplicate
acknowledgement, terminal snapshots, readiness, health, metrics and restart
replay. It shuts down its own server before exiting and needs no model provider.

## Reproduce the dashboard image

With dashboard dependencies installed:

```bash
PYTHONPATH=src python examples/capture_dashboard.py --output docs/images/dashboard-synthetic.svg
```

The script starts the real backend on an ephemeral loopback port, ingests only
synthetic events, starts the actual `SubagentDashboard` in Textual's headless
application harness, waits for all three lanes and exports the rendered SVG.
It does not paint a product mockup or read real user transcripts. Provider names
are grouping examples; all model labels, IDs, counts and outcomes are synthetic.
The provider rollup marks cost unavailable; a lane table zero is an accumulator
default, not a measured billing result. Capture appearance/timestamps may change on rerun.

The image demonstrates rendering and local API connectivity. It is not a full
interactive UI test suite. Existing dashboard tests primarily exercise pure
functions; operator interaction and terminal compatibility remain separate checks.

## Configuration checks

Shell scripts pass `bash -n`; fixture JSON and template YAML are parsed by the
tests that load them. The Compose file can be checked without starting a
container:

```bash
LIVENESS_INGEST_TOKEN=dummy docker compose -f examples/compose.yaml config --quiet
```

## Not covered by automated tests

- Compatibility across every Python version, OS or terminal.
- A container build/start or live OTel collector/backend deployment.
- Real provider CLI behavior, model performance, billing accuracy or live Telegram delivery.
- Load limits, availability targets, encrypted storage, multi-tenant access or durable watcher budgets.
- Automated backup/retention or off-host disaster recovery.

Passing tests supports the covered behaviors. It does not establish production
readiness or performance at an unspecified scale.

The architecture and delivery diagrams are generated from explicitly labeled
implementation boundaries with standard-library SVG code:

```bash
python examples/build_diagrams.py
```

`src/watcher/tests/test_summary_privacy.py` checks that summaries are excluded
from dry-run logs and from the payload handed to the notification transport.
`src/cc-host/tests/test_launcher_privacy.py` checks background prompt/log
permissions under a 022 umask, the missing-Codex-config fallback and that Qwen
keys never appear in argv.

`examples/tests/test_local_demo.py` launches the documented demo CLI on an
ephemeral port, checks its printed run ID and snapshot URL, then polls the same
server through the real Textual application and asserts a selected run with a
completed lane. The HTTP/replay smoke check (`examples/local_smoke.py`) covers a
closed historical run instead.
