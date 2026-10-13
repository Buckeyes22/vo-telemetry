# Host-side liveness producers

Hooks and supervised CLI shims append facts to a durable SQLite outbox.
The forwarder preserves per-source FIFO, retries delivery, and reports drops.

For an isolated trial, set `CLAUDE_TELEMETRY_HOME` and `CC_HOST_OUTBOX_DIR`
to disposable directories. Set `LIVENESS_INGEST_URL` and a fresh
`LIVENESS_INGEST_TOKEN`; see `examples/config.env.example`.

`install.sh` is an optional Linux integration: it modifies Claude hook settings,
writes systemd user units, and enables the forwarder and transcript tailer.
Inspect it before using it on a real account. The root quick start needs no
hook installation. The installer derives source and interpreter paths from
its own location. The static systemd unit is only a template; update its
source path and interpreter for your checkout.

Shim launchers resolve the package relative to the script. Their default keeps
CLI sandbox/permission controls enabled; `CC_HOST_ALLOW_UNRESTRICTED=1` is an
explicit opt-in. The supervisor preserves child exit codes and attributes
liveness separately from OTel trace correlation. See `shims/README.md`.

Pricing keys must match exact emitted model strings; unknown rates remain
unavailable. `pricing/models.yaml` contains no actual model rates.
