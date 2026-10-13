# Subagent Textual Dashboard

Passive, read-only TUI that polls the liveness snapshot API and renders
per-provider swim lanes with status badges, cost rollup, and throughput pulse.

## Quick start

```bash
# from the repo root
cd src
pip install -r dashboards/requirements.txt
python -m dashboards.subagent_dashboard

# or with a custom API base URL
python -m dashboards.subagent_dashboard --api-base http://127.0.0.1:8723
```

## Key bindings

| Key | Action |
|-----|--------|
| `q` | Quit |
| `r` | Force refresh now |
| `o` | Cycle orchestrator filter |
| `f` | Clear filter |
| `j` | Next run |
| `k` | Previous run |

## Layout

```
┌─────────────────────────────────────────────────────────────┐
│ Header (clock)                                              │
├─────────────────────────────────────────────────────────────┤
│ Phase ribbon: run_id · project · orchestrator · state · age │
│ Throughput pulse: snap/s · server_time                      │
├───────────────┬─────────────────────────────────────────────┤
│ Run selector  │ Filter bar                                  │
│ (state icons) │ Lane swim-lane table (per-provider sorted)  │
│               │   Provider · AgentID · Type · Status        │
│               │   Tok↑ · Tok↓ · Cost · Failed · Last Act   │
│               │   Branch · Worktree · Flags                 │
│               ├─────────────────────────────────────────────┤
│               │ Provider rollup (tokens + cost per provider)│
├───────────────┴─────────────────────────────────────────────┤
│ Status bar: API connectivity · snapshot_version             │
│ Footer (key hints)                                          │
└─────────────────────────────────────────────────────────────┘
```

## Features

- **Per-provider swim lanes** — sorted by provider, then agent_id; color-coded
- **Phase ribbon** — run-level state, project, orchestrator, last-event age
- **Status badges** — `running` / `stale` / `stuck` / `running_with_failed_child` / `done` / `done_with_warnings` / `failed`
- **Telemetry degraded indicator** — shown as `TELEM-DEGRADED` flag when `dropped_events_total > 0` or `telemetry_degraded = true`
- **Provider token+cost rollup** — aggregated tokens in/out and cost per provider with provenance note
- **Throughput pulse** — rolling snap/s calculated over last 30 s
- **Failure surfacing** — `failed_child_count` shown in red; `QUARANTINE` and `ID-ERR` flags
- **Worktree/branch column** — shows worktree path and branch for each lane
- **Run selector** — lists open runs from `/runs/current` with open/closed/abandoned/crashed state icons
- **Orchestrator filter** — press `o` to cycle, `f` to clear
- **Delta polling** — uses `since_snapshot_version` on `/lanes` endpoint to avoid flicker; merges incremental lane updates
- **No auth** — open API, no token needed
