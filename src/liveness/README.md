# Liveness backend

Python standard-library HTTP service with SQLite WAL, strict event validation,
token-authenticated ingest, deterministic reduction and replay.

Follow the root README for a loopback-only local run and synthetic demo.
The service exposes `/ingest`, `/runs/current`, `/lanes/current`,
`/runs/{run_id}`, `/runs/{run_id}/lanes`, `/runs/{run_id}/events`,
`/healthz`, `/readyz` and `/metrics`.

Set `LIVENESS_INGEST_TOKEN` to a freshly generated local value. Defaults use
`127.0.0.1:8723` and `./data/liveness/events.db`. Read APIs are unauthenticated;
keep this service on loopback or behind an authenticated private proxy.
Full final messages are disabled by default, but previews and summaries can
still contain sensitive text.
