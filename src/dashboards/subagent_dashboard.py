"""
Subagent Textual Dashboard — vo-telemetry

Passive TUI polling the liveness snapshot API at http://127.0.0.1:8723.
Endpoints used:
  GET /runs/current
  GET /runs/{run_id}
  GET /runs/{run_id}/lanes?since_snapshot_version=N

Run with:
  python -m dashboards.subagent_dashboard
  python src/dashboards/subagent_dashboard.py
"""

from __future__ import annotations

import argparse
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, ClassVar

from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding, BindingType
from textual.containers import Container, ScrollableContainer, Vertical
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    Label,
    ListItem,
    ListView,
    Log,
    Static,
)

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

DEFAULT_API_BASE = "http://127.0.0.1:8723"
POLL_INTERVAL_RUNS = 5.0  # seconds between /runs/current refreshes
POLL_INTERVAL_LANES = 2.0  # seconds between /lanes polls

# Startup /readyz gate: block first render until the backend reports
# WAL reachable, reducer caught up, and snapshot state initialized.
READYZ_POLL_INTERVAL = 1.0  # seconds between /readyz probes while gating
READYZ_BACKOFF_MAX = 10.0  # cap the readyz backoff so we keep probing

# Exponential backoff on API failure. Each consecutive failure doubles the
# sleep up to BACKOFF_MAX; the first success resets to the base interval.
BACKOFF_BASE = 1.0
BACKOFF_MAX = 30.0
BACKOFF_FACTOR = 2.0

STATUS_COLORS: dict[str, str] = {
    "running": "green",
    "running_with_failed_child": "yellow",
    "stale": "dark_orange",
    "stuck": "red",
    "done": "cyan",
    "done_with_warnings": "bright_yellow",
    "failed": "bright_red",
    "unknown": "grey50",
}

STATUS_ICONS: dict[str, str] = {
    "running": "[bold green]▶[/]",
    "running_with_failed_child": "[bold yellow]▶![/]",
    "stale": "[bold dark_orange]⏸[/]",
    "stuck": "[bold red]✖[/]",
    "done": "[bold cyan]✔[/]",
    "done_with_warnings": "[bold bright_yellow]✔⚠[/]",
    "failed": "[bold bright_red]✗[/]",
    # tailer_pending: shim heartbeats present but no transcript yet.
    # Transient projection — distinct from running because no lane activity has
    # been observed; the tailer is still discovering agent-<id>.jsonl.
    "tailer_pending": "[bold grey62]◌[/]",
    "unknown": "[grey50]?[/]",
}

# Derived-state labels that are read-time projections, NOT stored terminal
# states. The dashboard renders them dim/bracketed so they read as
# "computed from heartbeat age / tailer discovery", not as a stored outcome.
TRANSIENT_STATES: frozenset[str] = frozenset({"stale", "stuck", "tailer_pending"})

# Quarantine agent_ids are deterministically keyed "quarantine:<short_hash>"
# . Detection is on this prefix — the authoritative signal — not a
# side-channel boolean field.
QUARANTINE_PREFIX = "quarantine:"

# Derived run closure states that the run selector can filter on.
RUN_STATES: tuple[str, ...] = ("open", "closed", "abandoned", "crashed")

RUN_STATE_ICONS: dict[str, str] = {
    "open": "🟢",
    "closed": "⚪",
    "abandoned": "🟡",
    "crashed": "🔴",
}

PROVIDER_COLORS: dict[str, str] = {
    "anthropic": "#5fafff",
    "openai": "#5fd787",
    "glm": "#d787ff",
    "gemini": "cyan",
    "ollama": "yellow",
    "unknown": "grey50",
}

# Cost provenance. cost_source comes off the *raw events* (the lane
# snapshot carries cost_usd_total but NOT the provenance fields — the reducer does
# not persist them — so provenance is read from /runs/{id}/events). We render each
# distinctly: pricing_table costs are honest-but-estimated, provider_reported are
# authoritative, and `unavailable` MUST be visually marked as "no trustworthy cost"
# rather than shown as a real number. No guessed cost value is ever displayed.
COST_SOURCE_LABELS: dict[str, str] = {
    "provider_reported": "[green]provider-reported[/]",
    "pricing_table": "[cyan]priced[/]",
    "unavailable": "[reverse yellow] COST UNAVAILABLE [/]",
}
COST_SOURCE_ORDER = ("provider_reported", "pricing_table", "unavailable")

# Click-to-tail: the shim spools each child's stdout/stderr to a
# bounded file keyed by tool_run_id at <telemetry_home>/spool/<tool_run_id>.log.
# CC_HOST honors CLAUDE_TELEMETRY_HOME; default ~/.claude/telemetry.
SPOOL_DIR = Path(os.getenv("CLAUDE_TELEMETRY_HOME", "~/.claude/telemetry")).expanduser() / "spool"
SPOOL_TAIL_BYTES = 64 * 1024  # bounded read window from the (already bounded) spool

# Cost-ceiling guardrail. Per-provider and per-run thresholds in USD.
# Configurable via env (VO_COST_CEILING_<PROVIDER>=<usd>, VO_COST_CEILING_RUN=<usd>).
# A warn band fires at WARN_FRACTION of the ceiling; >= ceiling is a breach.
DEFAULT_PROVIDER_CEILINGS: dict[str, float] = {
    "anthropic": 5.0,
    "openai": 5.0,
    "glm": 2.0,
    "gemini": 5.0,
}
DEFAULT_RUN_CEILING = 10.0
CEILING_WARN_FRACTION = 0.8

# /events pagination. The backend bounds each page; we follow
# next_page_token, advancing since_ingest_seq past each page boundary.
EVENTS_PAGE_SIZE = 500
EVENTS_MAX_PAGES = 40  # hard stop so a runaway run can't pin the poll thread


def _load_provider_ceilings() -> dict[str, float]:
    """Per-provider cost ceilings, overridable via VO_COST_CEILING_<PROVIDER> env."""
    ceilings = dict(DEFAULT_PROVIDER_CEILINGS)
    for key, value in os.environ.items():
        if not key.startswith("VO_COST_CEILING_"):
            continue
        name = key[len("VO_COST_CEILING_") :].lower()
        if name == "run":
            continue
        try:
            ceilings[name] = float(value)
        except ValueError:
            continue
    return ceilings


def _load_run_ceiling() -> float:
    raw = os.getenv("VO_COST_CEILING_RUN")
    if raw is None:
        return DEFAULT_RUN_CEILING
    try:
        return float(raw)
    except ValueError:
        return DEFAULT_RUN_CEILING


# ──────────────────────────────────────────────────────────────────────────────
# HTTP helpers (sync, run in thread)
# ──────────────────────────────────────────────────────────────────────────────


def _get_json(url: str, timeout: float = 10.0) -> Any:
    """GET JSON, returning the parsed body or None on any failure.

    Treats any non-2xx status as a failure (None). For health endpoints that
    deliberately use HTTP status as signal (/readyz 503), use _probe() instead.
    """
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        # Scheme validated at config load.
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec B310
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, OSError):
        return None


def _probe(url: str, timeout: float = 5.0) -> tuple[int | None, dict | None]:
    """Probe an endpoint, returning (status_code, parsed_body).

    Unlike _get_json this surfaces the HTTP status even for 4xx/5xx, because
    /readyz uses 503 as a meaningful "not ready yet" signal, and we must
    distinguish "not ready" (503 with a body) from "unreachable" (None status).
    """
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        # Scheme validated at config load.
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec B310
            body = resp.read().decode()
            try:
                parsed = json.loads(body)
            except json.JSONDecodeError:
                parsed = None
            return resp.status, parsed
    except urllib.error.HTTPError as e:
        # 503/4xx still carry a JSON body we want (readyz reports its reasons).
        try:
            parsed = json.loads(e.read().decode())
        except (json.JSONDecodeError, OSError, ValueError):
            parsed = None
        return e.code, parsed
    except (urllib.error.URLError, OSError):
        return None, None


def fetch_runs_current(api_base: str) -> dict | None:
    return _get_json(f"{api_base}/runs/current")


def fetch_run_detail(api_base: str, run_id: str) -> dict | None:
    return _get_json(f"{api_base}/runs/{run_id}")


def fetch_lanes(api_base: str, run_id: str, since_version: int) -> dict | None:
    url = f"{api_base}/runs/{run_id}/lanes?since_snapshot_version={since_version}"
    return _get_json(url)


def fetch_readyz(api_base: str) -> tuple[int | None, dict | None]:
    """Probe /readyz. 200 => backend ready; 503 => alive but not caught up."""
    return _probe(f"{api_base}/readyz")


def fetch_healthz(api_base: str) -> tuple[int | None, dict | None]:
    """Probe /healthz. 200 => process alive; anything else/None => unhealthy."""
    return _probe(f"{api_base}/healthz")


def fetch_events_page(api_base: str, run_id: str, since_ingest_seq: int, limit: int = EVENTS_PAGE_SIZE) -> dict | None:
    """Fetch one bounded page of raw events for a run.

    Returns the parsed page ({events, next_page_token, ...}) or None on failure.
    run_id is URL-encoded because real run_ids contain a colon (session:epoch).
    """
    rid = urllib.parse.quote(run_id, safe="")
    url = f"{api_base}/runs/{rid}/events?since_ingest_seq={since_ingest_seq}&limit={limit}"
    return _get_json(url)


def fetch_all_events(api_base: str, run_id: str, since_ingest_seq: int = 0) -> tuple[list[dict], int]:
    """Page the whole event stream for a run, advancing since_ingest_seq.

    Follows ``next_page_token`` (the last ingest_seq of the page) so we walk past
    every page boundary. Returns (events, highest_ingest_seq_seen) so the caller
    can resume from a watermark rather than re-reading the run from zero.

    A bounded EVENTS_MAX_PAGES guard prevents a pathological run from pinning the
    poll thread; FIFO/ingest_seq ordering from the backend is preserved.
    """
    events: list[dict] = []
    cursor = since_ingest_seq
    highest = since_ingest_seq
    for _ in range(EVENTS_MAX_PAGES):
        page = fetch_events_page(api_base, run_id, cursor)
        if page is None:
            break
        batch = page.get("events") or []
        events.extend(batch)
        for ev in batch:
            seq = ev.get("ingest_seq")
            if isinstance(seq, int) and seq > highest:
                highest = seq
        token = page.get("next_page_token")
        if not token:
            break
        try:
            # next_page_token IS the since_ingest_seq for the following page —
            # advancing the cursor past the boundary the page just covered.
            next_cursor = int(token)
        except (TypeError, ValueError):
            break
        if next_cursor <= cursor:
            break  # defensive: never loop on a non-advancing token
        cursor = next_cursor
    return events, highest


def read_spool_tail(tool_run_id: str, max_bytes: int = SPOOL_TAIL_BYTES) -> tuple[str, str | None]:
    """Read the tail of a tool_run_id's bounded spool file.

    Returns (text, error). The spool is already size-bounded by the shim, so we
    additionally cap the read window. Resolves the path strictly inside SPOOL_DIR
    and refuses anything that escapes it (the spool dir is a 0700 trust boundary).
    """
    if not tool_run_id:
        return "", "no tool_run_id for this lane"
    candidate = (SPOOL_DIR / f"{tool_run_id}.log").resolve()
    try:
        candidate.relative_to(SPOOL_DIR.resolve())
    except ValueError:
        return "", "spool path escapes spool dir (refused)"
    if not candidate.exists():
        return "", f"no spool file at {candidate}"
    try:
        size = candidate.stat().st_size
        with candidate.open("rb") as fh:
            if size > max_bytes:
                fh.seek(size - max_bytes)
            data = fh.read()
        return data.decode("utf-8", errors="replace"), None
    except OSError as exc:
        return "", f"spool read error: {exc}"


# ──────────────────────────────────────────────────────────────────────────────
# Data model helpers
# ──────────────────────────────────────────────────────────────────────────────


def _ts_age(ts_str: str | None) -> str:
    """Return human-readable age from an ISO-8601 timestamp string."""
    if not ts_str:
        return "—"
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        secs = int((datetime.now(timezone.utc) - dt).total_seconds())
        if secs < 0:
            secs = 0
        if secs < 60:
            return f"{secs}s"
        if secs < 3600:
            return f"{secs // 60}m{secs % 60:02d}s"
        h = secs // 3600
        m = (secs % 3600) // 60
        return f"{h}h{m:02d}m"
    except (ValueError, TypeError):
        return "?"


def _ts_short(ts_str: str | None) -> str:
    """Short absolute wall-clock 'HH:MM:SSZ' for run-selector timestamps."""
    if not ts_str:
        return "—"
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(timezone.utc)
        return dt.strftime("%H:%M:%SZ")
    except (ValueError, TypeError):
        return "?"


def _fmt_cost(usd: float | None) -> str:
    if usd is None:
        return "—"
    if usd == 0.0:
        return "$0.00"
    if usd < 0.001:
        return f"${usd:.5f}"
    if usd < 1.0:
        return f"${usd:.4f}"
    return f"${usd:.2f}"


def _lane_cost_cell(cost: float) -> str:
    """Lane-table cost: a dash when no cost was reported, so 0 never reads as $0.00 billed."""
    if cost == 0.0:
        return "[grey50]—[/]"
    return _fmt_cost(cost)


def _fmt_tokens(n: float | None) -> str:
    if n is None:
        return "—"
    if n == 0:
        return "0"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(int(n))


def _safe_widget_id(run_id: str) -> str:
    """Map an arbitrary run_id to a valid Textual widget id.

    Real run_ids look like '4ac8f99d-...:1780280207' — the colon (and any other
    char outside [A-Za-z0-9_-]) is an illegal Textual widget id and makes
    ListItem(id=...) raise BadIdentifier, crashing the dashboard the instant a
    real run arrives. Replace every illegal char with '_' and prefix 'run_'.
    RunSelectorPane keeps the reverse mapping so selection recovers the true id.
    """
    safe = "".join(c if (c.isalnum() or c in "_-") else "_" for c in run_id)
    return f"run_{safe}"


def _is_quarantine_lane(lane: dict) -> bool:
    """True iff this lane is the deterministic quarantine lane.

    The authoritative signal is the ``quarantine:<short_hash>`` prefix on the
    ``agent_id`` — never a null agent_id, never an out-of-band field. We also
    honor the reducer's ``quarantine`` boolean as a belt-and-suspenders, but the
    prefix is the contract, and it is what we key the *display* on.
    """
    agent_id = lane.get("agent_id") or ""
    return agent_id.startswith(QUARANTINE_PREFIX) or bool(lane.get("quarantine"))


def _is_tailer_pending(lane: dict) -> bool:
    """True iff the lane has a shim heartbeat but no transcript/tool stream yet.

    Grounded in the REAL reducer behavior (reducer.py): ``lane_started`` itself
    stamps ``last_activity_ts``, so a freshly-started lane is NOT distinguishable
    by a null activity timestamp. The honest "shim heartbeats but no transcript
    yet" signal in the snapshot is therefore: the lane is active (started, not
    terminal) and has produced **no tool/transcript stream yet** — i.e. ``tools``
    is empty. Once the shim's tool stream or the transcript tailer lands the first
    ``tool_*``/activity event, ``tools`` is non-empty and the lane reads as
    ``running``. This is the case "the reducer must render a lane
    that has shim heartbeats but no transcript yet."
    """
    base = lane.get("base_state") or lane.get("state")
    terminal = {"done", "done_with_warnings", "failed", "closed"}
    if base in terminal:
        return False
    if not lane.get("started_ts"):
        return False
    # No tool/transcript stream has produced any child progress yet.
    return not (lane.get("tools") or {})


def _lane_degraded(lane: dict) -> bool:
    """True iff the lane received a ``producer_dropped_events`` event.

    The reducer sets ``telemetry_degraded`` exactly when it applies a
    ``producer_dropped_events`` fact (reducer.py: event_type ==
    'producer_dropped_events' → lane.telemetry_degraded = True). We key the
    badge on that flag — NOT on ``dropped_events_total > 0``, which is a derived
    counter, not the event-received signal.
    """
    return bool(lane.get("telemetry_degraded"))


def _lane_status(lane: dict) -> str:
    """Derive the display status badge from lane fields.

    Layers the dashboard-side ``tailer_pending`` projection on top of the
    reducer's projected lane state. The reducer already projects
    running/stale/stuck/terminal; we only override to ``tailer_pending`` when the
    shim has started a lane that has produced no transcript activity yet, which
    the reducer reports as a bare non-terminal base_state.
    """
    state = lane.get("state") or lane.get("base_state") or "unknown"
    # Only reinterpret genuinely-active, no-activity lanes; never override a
    # stale/stuck/terminal projection the reducer already computed.
    if (
        state not in TRANSIENT_STATES
        and state
        not in {
            "done",
            "done_with_warnings",
            "failed",
        }
        and _is_tailer_pending(lane)
    ):
        return "tailer_pending"
    return state


def _lane_provider_color(provider: str | None) -> str:
    if not provider:
        return PROVIDER_COLORS["unknown"]
    key = provider.lower()
    return PROVIDER_COLORS.get(key, "grey50")


def _tools_summary(tools: dict) -> tuple[int, int, float]:
    """Returns (tokens_in, tokens_out, cost_usd) across all tool entries."""
    tin = tout = 0
    cost = 0.0
    for t in tools.values():
        tin += t.get("tokens_in") or 0
        tout += t.get("tokens_out") or 0
        cost += t.get("cost_usd_total") or 0.0
    return tin, tout, cost


def build_cost_provenance(events: list[dict]) -> dict[str, dict[str, str]]:
    """Map tool_run_id -> {cost_source, pricing_version} from raw events.

    The lane snapshot does NOT carry provenance (the reducer persists only
    cost_usd_total). The raw /events stream does — every tool_completed/tool_failed
    carries cost_source and pricing_version. We index the *last* provenance seen
    per tool_run_id (events are ingest_seq-ordered, so last wins).
    """
    prov: dict[str, dict[str, str]] = {}
    for ev in events:
        trid = ev.get("tool_run_id")
        if not trid:
            continue
        cs = ev.get("cost_source")
        pv = ev.get("pricing_version")
        if cs is None and pv is None:
            continue
        entry = prov.setdefault(trid, {})
        if cs is not None:
            entry["cost_source"] = cs
        if pv is not None:
            entry["pricing_version"] = pv
    return prov


def _provider_rollup(lanes: list[dict], provenance: dict[str, dict[str, str]] | None = None) -> dict[str, dict]:
    """Aggregate tokens and cost per provider, with cost_source provenance.

    ``provenance`` maps tool_run_id -> {cost_source, pricing_version} (from
    build_cost_provenance over /events). Per provider we track tokens, cost,
    the set of cost_source values, the set of pricing_version values, and the
    cost attributed to ``unavailable`` sources separately so the panel can render
    'unavailable' distinctly and never present a guessed number as authoritative.
    """
    provenance = provenance or {}
    rollup: dict[str, dict] = {}
    for lane in lanes:
        prov = (lane.get("provider") or "unknown").lower()
        if prov not in rollup:
            rollup[prov] = {
                "tokens_in": 0,
                "tokens_out": 0,
                "cost_usd": 0.0,
                "unavailable_cost": 0.0,
                "agents": 0,
                "sources": set(),
                "pricing_versions": set(),
            }
        rollup[prov]["agents"] += 1
        for trid, t in (lane.get("tools") or {}).items():
            rollup[prov]["tokens_in"] += t.get("tokens_in") or 0
            rollup[prov]["tokens_out"] += t.get("tokens_out") or 0
            cost = t.get("cost_usd_total") or 0.0
            meta = provenance.get(trid, {})
            source = meta.get("cost_source")
            pv = meta.get("pricing_version")
            if source:
                rollup[prov]["sources"].add(source)
            if pv:
                rollup[prov]["pricing_versions"].add(pv)
            if source == "unavailable":
                # cost is not trustworthy — keep it OUT of the headline cost total
                rollup[prov]["unavailable_cost"] += cost
            else:
                rollup[prov]["cost_usd"] += cost
    return rollup


def _provider_cost_totals(lanes: list[dict], provenance: dict[str, dict[str, str]] | None = None) -> dict[str, float]:
    """Trustworthy (non-'unavailable') cumulative cost USD per provider."""
    rollup = _provider_rollup(lanes, provenance)
    return {prov: data["cost_usd"] for prov, data in rollup.items()}


def evaluate_ceilings(
    lanes: list[dict],
    provider_ceilings: dict[str, float],
    run_ceiling: float,
    provenance: dict[str, dict[str, str]] | None = None,
) -> list[tuple[str, str, float, float]]:
    """Cost-ceiling guardrail.

    Returns a list of (severity, scope_label, cost, ceiling) where severity is
    'breach' (cost >= ceiling) or 'warn' (cost >= WARN_FRACTION*ceiling). Scope is
    per-provider plus an aggregate per-run total. 'unavailable' cost is excluded —
    we never raise a ceiling alarm on a number we don't trust.
    """
    alarms: list[tuple[str, str, float, float]] = []
    totals = _provider_cost_totals(lanes, provenance)
    run_total = 0.0
    for prov, cost in sorted(totals.items()):
        run_total += cost
        ceiling = provider_ceilings.get(prov)
        if ceiling is None or ceiling <= 0:
            continue
        if cost >= ceiling:
            alarms.append(("breach", f"provider:{prov}", cost, ceiling))
        elif cost >= ceiling * CEILING_WARN_FRACTION:
            alarms.append(("warn", f"provider:{prov}", cost, ceiling))
    if run_ceiling and run_ceiling > 0:
        if run_total >= run_ceiling:
            alarms.append(("breach", "run-total", run_total, run_ceiling))
        elif run_total >= run_ceiling * CEILING_WARN_FRACTION:
            alarms.append(("warn", "run-total", run_total, run_ceiling))
    return alarms


def _status_badge(status: str) -> str:
    """Render a status badge with its icon + label.

    Transient projections (stale/stuck/tailer_pending) are wrapped
    in brackets and dimmed so they are visually distinct from stored terminal
    states — they are read-time projections, not outcomes.
    """
    icon = STATUS_ICONS.get(status, STATUS_ICONS["unknown"])
    if status in TRANSIENT_STATES:
        # bracketed = "this is a projection, not a stored state"
        return f"{icon} [italic dim]\\[{status}][/]"
    return f"{icon} {status}"


def _lane_badges(lane: dict) -> str:
    """Build the trailing badge string for a lane (degraded/quarantine/id-err)."""
    parts: list[str] = []
    # : degraded keys off a *received* producer_dropped_events event, which
    # the reducer surfaces as telemetry_degraded — never the dropped counter.
    if _lane_degraded(lane):
        n = lane.get("dropped_events_total") or 0
        parts.append(f"[reverse red] TELEMETRY DEGRADED [/][red] (dropped={n})[/]")
    if _is_quarantine_lane(lane):
        parts.append("[reverse bright_red] QUARANTINE [/]")
    if lane.get("identity_error"):
        parts.append("[orange3]ID-ERR[/]")
    return " ".join(parts)


# ──────────────────────────────────────────────────────────────────────────────
# Widgets
# ──────────────────────────────────────────────────────────────────────────────


class PhaseRibbon(Static):
    """Shows the run-level phase/state and last-event age."""

    DEFAULT_CSS = """
    PhaseRibbon {
        height: 1;
        background: $panel;
        padding: 0 1;
    }
    """

    def update_run(self, run: dict | None) -> None:
        if run is None:
            self.update("[grey50]No run selected[/]")
            return
        state = run.get("state", "unknown")
        icon = RUN_STATE_ICONS.get(state, "⚪")
        run_id = run.get("run_id", "?")
        project = run.get("project") or "?"
        orch = run.get("orchestrator") or "?"
        age = _ts_age(run.get("last_event_ts"))
        snap_v = run.get("snapshot_version", 0)
        self.update(
            f"{icon} [bold]{run_id}[/]  project=[cyan]{project}[/]  "
            f"orch=[yellow]{orch}[/]  state=[bold]{state}[/]  "
            f"last_event=[green]{age} ago[/]  snapshot_v=[grey]{snap_v}[/]"
        )


class ThroughputPulse(Static):
    """Shows a throughput pulse: events/s estimate and staleness."""

    DEFAULT_CSS = """
    ThroughputPulse {
        height: 1;
        background: $panel-darken-1;
        padding: 0 1;
    }
    """

    _history: list[tuple[float, int]] = []

    def update_pulse(self, snapshot_version: int, server_time: str | None) -> None:
        now = time.monotonic()
        self._history.append((now, snapshot_version))
        # keep last 30 seconds
        cutoff = now - 30.0
        self._history = [(t, v) for t, v in self._history if t >= cutoff]
        if len(self._history) >= 2:
            dt = self._history[-1][0] - self._history[0][0]
            dv = self._history[-1][1] - self._history[0][1]
            rate = dv / dt if dt > 0 else 0.0
            pulse_str = f"[green]{rate:.1f} snap/s[/]"
        else:
            pulse_str = "[grey50]—[/]"
        st = _ts_age(server_time) if server_time else "?"
        self.update(f"Throughput: {pulse_str}  server_time=[grey]{server_time or '?'}[/]  age={st}")


QUARANTINE_PROVIDER_KEY = "__quarantine__"


def _lane_row(lane: dict) -> tuple[str, ...]:
    """Build the DataTable row cells for a single lane within a swim section."""
    agent_id = lane.get("agent_id") or "?"
    agent_type = lane.get("agent_type") or "?"
    status = _lane_status(lane)

    tin, tout, cost = _tools_summary(lane.get("tools") or {})
    failed = lane.get("failed_child_count") or 0
    last_act = _ts_age(lane.get("last_activity_ts"))
    branch = lane.get("branch") or "—"
    worktree_path = lane.get("worktree") or "—"
    if worktree_path != "—" and len(worktree_path) > 30:
        worktree_path = "…" + worktree_path[-28:]

    outcome = lane.get("declared_outcome")
    outcome_cell = "—"
    if outcome and outcome != "unknown":
        oc = {"success": "green", "failure": "bright_red", "partial": "yellow"}.get(outcome, "grey50")
        outcome_cell = f"[{oc}]{outcome}[/]"

    failed_cell = f"[bold red]{failed}[/]" if failed > 0 else "0"

    return (
        agent_id,
        agent_type,
        _status_badge(status),
        outcome_cell,
        _fmt_tokens(tin),
        _fmt_tokens(tout),
        _lane_cost_cell(cost),
        failed_cell,
        f"[grey50]{last_act}[/]",
        branch,
        worktree_path,
        _lane_badges(lane),
    )


class LaneSwimSection(Vertical):
    """One per-provider swim lane: header + phase-ribbon strip + its own table.

    Each provider (opencode-glm, codex, ...) gets its *own* horizontal lane
    section, not a single flat table. The quarantine lane
    renders as a distinct section with a bright-red header so it never reads as a
    named lane.
    """

    DEFAULT_CSS = """
    LaneSwimSection {
        height: auto;
        margin: 0 0 1 0;
        border: round $primary-darken-2;
    }
    LaneSwimSection.quarantine {
        border: round red;
    }
    LaneSwimSection .lane_header {
        height: 1;
        padding: 0 1;
        background: $panel;
    }
    LaneSwimSection .lane_phase {
        height: 1;
        padding: 0 1;
        background: $panel-darken-1;
        color: $text-muted;
    }
    LaneSwimSection DataTable {
        height: auto;
        max-height: 12;
    }
    """

    def __init__(self, provider_key: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._provider_key = provider_key
        self._is_quarantine = provider_key == QUARANTINE_PROVIDER_KEY
        self._pending_lanes: list[dict] | None = None
        self._ready = False
        if self._is_quarantine:
            self.add_class("quarantine")

    @property
    def _safe(self) -> str:
        return self._provider_key.replace(":", "_").replace("-", "_").strip("_") or "x"

    def compose(self) -> ComposeResult:
        yield Static(id=f"hdr_{self._safe}", classes="lane_header")
        yield Static(id=f"phase_{self._safe}", classes="lane_phase")
        yield DataTable(id=f"dt_{self._safe}", zebra_stripes=True, cursor_type="row")

    LANE_COLUMNS: tuple[str, ...] = (
        "Agent ID",
        "Type",
        "Status",
        "Outcome",
        "Tok↑",
        "Tok↓",
        "Cost",
        "Failed",
        "Last Act",
        "Branch",
        "Worktree",
        "Badges",
    )

    def _ensure_columns(self, dt: DataTable) -> None:
        """Add the lane columns exactly once.

        A section can render before the DataTable's own ``on_mount`` has run, so
        columns may not exist yet — adding
        a 12-cell row to a 0-column table raises 'More values provided than there
        are columns' and kills the poll worker. Guard idempotently.
        """
        if not dt.columns:
            dt.add_columns(*self.LANE_COLUMNS)

    def on_mount(self) -> None:
        dt: DataTable = self.query_one(f"#dt_{self._safe}", DataTable)
        self._ensure_columns(dt)
        self._ready = True
        if self._pending_lanes is not None:
            self._render_section(self._pending_lanes)

    def update_section(self, lanes: list[dict]) -> None:
        """Render now if on_mount has run; otherwise keep the latest lanes for on_mount.

        A poll can deliver lanes for a section that was mounted on the previous
        poll but whose children are not composed yet; querying them would raise
        NoMatches and kill the poll worker.
        """
        self._pending_lanes = lanes
        if self._ready:
            self._render_section(lanes)

    def _render_section(self, lanes: list[dict]) -> None:
        hdr: Static = self.query_one(f"#hdr_{self._safe}", Static)
        if self._is_quarantine:
            hdr.update(
                f"[reverse bright_red] QUARANTINE LANE [/] "
                f"[bright_red]unattributed events [/]  "
                f"agents=[bold]{len(lanes)}[/]"
            )
        else:
            pcolor = _lane_provider_color(self._provider_key)
            hdr.update(f"[{pcolor} bold]▌ {self._provider_key}[/]  lanes=[bold]{len(lanes)}[/]")

        # Phase ribbon strip for THIS provider's lanes: a compact strip
        # of per-lane status glyphs alongside the swim lane, so the whole lane's
        # phase reads at a glance.
        phase: Static = self.query_one(f"#phase_{self._safe}", Static)
        phase.update(self._phase_strip(lanes))

        dt: DataTable = self.query_one(f"#dt_{self._safe}", DataTable)
        self._ensure_columns(dt)  # the table's own on_mount may not have run yet
        dt.clear()
        for lane in sorted(lanes, key=lambda la: la.get("agent_id") or ""):
            dt.add_row(*_lane_row(lane))

    @staticmethod
    def _phase_strip(lanes: list[dict]) -> str:
        """One glyph per lane summarising its phase, plus a per-state tally."""
        if not lanes:
            return "[grey50]phase: —[/]"
        glyphs: list[str] = []
        tally: dict[str, int] = {}
        for lane in sorted(lanes, key=lambda la: la.get("agent_id") or ""):
            status = _lane_status(lane)
            tally[status] = tally.get(status, 0) + 1
            glyphs.append(STATUS_ICONS.get(status, STATUS_ICONS["unknown"]))
        tally_str = "  ".join(f"{STATUS_ICONS.get(s, '?')}×{n}" for s, n in sorted(tally.items()))
        return f"[grey62]phase[/] {' '.join(glyphs)}   {tally_str}"


class SwimLanes(ScrollableContainer):
    """Stack of per-provider swim-lane sections.

    Replaces the old single flat DataTable. Builds/destroys provider sections to
    match the live provider set; the quarantine lane (detected via the
    ``quarantine:<hash>`` agent_id prefix) is split into its own section.
    """

    DEFAULT_CSS = """
    SwimLanes {
        height: 1fr;
    }
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._sections: dict[str, LaneSwimSection] = {}

    def _section_key(self, lane: dict) -> str:
        if _is_quarantine_lane(lane):
            return QUARANTINE_PROVIDER_KEY
        return (lane.get("provider") or "unknown").lower()

    def update_lanes(self, lanes: list[dict], orchestrator_filter: str | None = None) -> None:
        filtered = lanes
        if orchestrator_filter:
            filtered = [la for la in lanes if (la.get("orchestrator") or "").lower() == orchestrator_filter.lower()]

        buckets: dict[str, list[dict]] = {}
        for lane in filtered:
            buckets.setdefault(self._section_key(lane), []).append(lane)

        # named providers alphabetically; quarantine always last
        ordered_keys = sorted(k for k in buckets if k != QUARANTINE_PROVIDER_KEY)
        if QUARANTINE_PROVIDER_KEY in buckets:
            ordered_keys.append(QUARANTINE_PROVIDER_KEY)

        # drop sections whose provider vanished
        for key in list(self._sections):
            if key not in buckets:
                self._sections.pop(key).remove()

        for key in ordered_keys:
            section = self._sections.get(key)
            if section is None:
                section = LaneSwimSection(key)
                self._sections[key] = section
                self.mount(section)
            # A section that is not mounted yet keeps the lanes and renders on mount.
            section.update_section(buckets[key])


class ProviderRollupPanel(Static):
    """Per-provider token+cost rollup with real cost_source/pricing_version provenance."""

    DEFAULT_CSS = """
    ProviderRollupPanel {
        height: auto;
        max-height: 10;
        background: $panel;
        padding: 0 1;
        border-top: solid $primary;
    }
    """

    def update_rollup(self, lanes: list[dict], provenance: dict[str, dict[str, str]] | None = None) -> None:
        rollup = _provider_rollup(lanes, provenance)
        if not rollup:
            self.update("[grey50]No provider data[/]")
            return
        lines: list[str] = []
        for prov, data in sorted(rollup.items()):
            pcolor = _lane_provider_color(prov)
            agents = data["agents"]
            tin = _fmt_tokens(data["tokens_in"])
            tout = _fmt_tokens(data["tokens_out"])
            sources = data.get("sources") or set()

            # Cost cell. If EVERY cost-bearing tool for this provider was
            # 'unavailable', we do NOT show a dollar figure at all — we mark it
            # COST UNAVAILABLE so no guessed value is presented.
            non_unavail = {s for s in sources if s and s != "unavailable"}
            if data["cost_usd"] == 0.0 and "unavailable" in sources and not non_unavail:
                cost_cell = COST_SOURCE_LABELS["unavailable"]
            else:
                cost_cell = _fmt_cost(data["cost_usd"])
                if "unavailable" in sources:
                    # some tools priced, some unavailable — flag the partial gap
                    cost_cell += " " + COST_SOURCE_LABELS["unavailable"]

            # Provenance suffix: which cost_source(s) backed this number, plus the
            # pricing_version(s) when the priced/pricing_table path was used.
            src_badges = (
                " ".join(COST_SOURCE_LABELS.get(s, f"[grey50]{s}[/]") for s in COST_SOURCE_ORDER if s in sources)
                or "[grey50]no cost events[/]"
            )
            pvs = sorted(v for v in (data.get("pricing_versions") or set()) if v)
            pv_suffix = f" pv={'/'.join(pvs)}" if pvs else ""

            lines.append(
                f"[{pcolor}]{prov}[/] agents={agents} "
                f"tok↑={tin} tok↓={tout} cost={cost_cell} "
                f"src={src_badges}{pv_suffix}"
            )
        # Provenance footer: cost_source/pricing_version are read from the raw
        # /events stream, not guessed.
        lines.append(
            "[grey50]cost_source/pricing_version come from /runs/{id}/events; "
            "'unavailable' is shown as such, never as a guessed value[/]"
        )
        self.update("\n".join(lines))


class CostCeilingBar(Static):
    """Cost-ceiling guardrail banner.

    Renders nothing when no provider/run is near its ceiling, a yellow warn band
    at >=80% of a ceiling, and a red breach band at/over a ceiling. Ceilings are
    configurable per-provider and per-run (env-overridable).
    """

    DEFAULT_CSS = """
    CostCeilingBar {
        height: auto;
        max-height: 3;
        padding: 0 1;
    }
    """

    def update_ceilings(self, alarms: list[tuple[str, str, float, float]]) -> None:
        if not alarms:
            self.update("[grey50]cost-ceiling guardrail armed (no provider/run over 80% of its ceiling)[/]")
            self.styles.background = None
            return
        breaches = [a for a in alarms if a[0] == "breach"]
        parts: list[str] = []
        for severity, scope, cost, ceiling in alarms:
            pct = (cost / ceiling * 100.0) if ceiling else 0.0
            if severity == "breach":
                parts.append(
                    f"[reverse bright_red] CEILING BREACH [/] {scope} "
                    f"{_fmt_cost(cost)} ≥ {_fmt_cost(ceiling)} ({pct:.0f}%)"
                )
            else:
                parts.append(
                    f"[reverse yellow] APPROACHING [/] {scope} {_fmt_cost(cost)} / {_fmt_cost(ceiling)} ({pct:.0f}%)"
                )
        self.update("   ".join(parts))
        self.styles.background = "red" if breaches else "dark_orange"


class RunSelectorPane(Widget):
    """Left-panel run selector: lists all known runs with state badges."""

    DEFAULT_CSS = """
    RunSelectorPane {
        width: 32;
        border-right: solid $primary-darken-2;
        background: $panel-darken-1;
    }
    RunSelectorPane Label {
        padding: 0 1;
        width: 100%;
    }
    RunSelectorPane ListView {
        height: 1fr;
    }
    """

    BINDINGS: ClassVar[list[BindingType]] = []

    # sanitized widget id -> real run_id; populated by update_runs()
    _id_to_run_id: dict[str, str] = {}

    def compose(self) -> ComposeResult:
        yield Label("[bold]Runs[/]", id="runs_header")
        yield ListView(id="runs_list")

    def update_runs(self, runs: list[dict], run_state_filter: str | None = None) -> None:
        lv: ListView = self.query_one("#runs_list", ListView)
        # reverse map: sanitized widget id -> real run_id (recovers the colon)
        self._id_to_run_id = {}

        # Build the desired (id -> label) set, deduped: the snapshot API can list
        # the same run_id more than once and Textual ids must be unique.
        # Filter by derived run-state when a filter is active.
        desired: dict[str, str] = {}
        for run in runs:
            state = run.get("state", "unknown")
            if run_state_filter is not None and state != run_state_filter:
                continue
            icon = RUN_STATE_ICONS.get(state, "⚪")
            run_id = run.get("run_id", "?")
            project = run.get("project") or "?"
            # The run row shows run_id, status (state), AND timestamp.
            # Prefer the run's started_ts; fall back to last_event_ts. We show a
            # short HH:MM:SSZ wall-clock plus a relative age so both an absolute
            # anchor and recency are visible at a glance.
            ts = run.get("started_ts") or run.get("last_event_ts")
            ts_short = _ts_short(ts)
            age = _ts_age(run.get("last_event_ts") or run.get("started_ts"))
            widget_id = _safe_widget_id(run_id)
            if widget_id in desired:
                continue
            desired[widget_id] = f"{icon} {run_id[:20]}\n  [{project}] {state}\n  {ts_short} ({age})"
            self._id_to_run_id[widget_id] = run_id

        # Reconcile incrementally rather than clear()+append(). ListView.clear()
        # is async (returns AwaitRemove); doing clear() then a synchronous
        # append() in the same tick leaves the old children mounted and raises
        # DuplicateIds. So we remove only the items that vanished and append only
        # the genuinely new ones — no full redraw, no flicker, no id collision.
        existing: dict[str, ListItem] = {}
        for item in lv.query(ListItem):
            if item.id:
                existing[item.id] = item

        for wid, item in existing.items():
            if wid not in desired:
                item.remove()

        for wid, label_text in desired.items():
            if wid not in existing:
                lv.append(ListItem(Label(label_text), id=wid))


class FilterBar(Static):
    """Shows current active filters and allows toggling."""

    DEFAULT_CSS = """
    FilterBar {
        height: 1;
        background: $panel-darken-2;
        padding: 0 1;
    }
    """

    def update_filters(self, orchestrator: str | None, run_state: str | None = None) -> None:
        parts: list[str] = []
        if orchestrator:
            parts.append(f"orchestrator=[yellow]{orchestrator}[/]")
        if run_state:
            ricon = RUN_STATE_ICONS.get(run_state, "")
            parts.append(f"run-state={ricon} [magenta]{run_state}[/]")
        if parts:
            self.update("Filter: " + "  ".join(parts) + "  [grey50](f=clear)[/]")
        else:
            self.update("[grey50]No filters active  (o=orchestrator, s=run-state)[/]")


class StatusBar(Static):
    """Bottom status bar showing API connectivity."""

    DEFAULT_CSS = """
    StatusBar {
        height: 1;
        background: $panel-darken-3;
        padding: 0 1;
        dock: bottom;
    }
    """

    def set_ok(self, api_base: str, snap_v: int) -> None:
        self.update(f"[green]●[/] API [cyan]{api_base}[/]  snap_v={snap_v}")

    def set_error(self, api_base: str, msg: str) -> None:
        self.update(f"[red]●[/] API [cyan]{api_base}[/] ERROR: {msg}")

    def set_connecting(self, api_base: str) -> None:
        self.update(f"[yellow]●[/] Connecting to [cyan]{api_base}[/]…")

    def set_waiting_ready(self, api_base: str, reasons: str) -> None:
        """Shown during the /readyz startup gate (backend alive but not ready)."""
        self.update(f"[yellow]◐[/] Waiting for [cyan]{api_base}[/] /readyz — {reasons}")

    def set_unhealthy(self, api_base: str, msg: str) -> None:
        """Shown when /healthz fails: the backend process is down/unreachable."""
        self.update(f"[bright_red]✖[/] API [cyan]{api_base}[/] UNHEALTHY: {msg} [grey50](retrying with backoff)[/]")


class SpoolTailPane(Widget):
    """Click-to-tail sub-pane for a lane's bounded shim spool file.

    Hidden until the user invokes click-to-tail (key 't' or selecting a lane). It
    reads the *tail* of <spool>/<tool_run_id>.log — already size-bounded by the
    shim — and renders it read-only. Closing returns focus to the lane table.
    """

    DEFAULT_CSS = """
    SpoolTailPane {
        display: none;
        height: 14;
        border-top: solid $secondary;
        background: $panel-darken-1;
    }
    SpoolTailPane.-visible {
        display: block;
    }
    SpoolTailPane #spool_header {
        height: 1;
        background: $secondary;
        color: $text;
        padding: 0 1;
    }
    SpoolTailPane Log {
        height: 1fr;
    }
    """

    def compose(self) -> ComposeResult:
        yield Label("", id="spool_header")
        yield Log(id="spool_log", highlight=False)

    def show_tail(self, agent_id: str, tool_run_id: str | None, text: str, error: str | None) -> None:
        header: Label = self.query_one("#spool_header", Label)
        log: Log = self.query_one("#spool_log", Log)
        log.clear()
        trid = tool_run_id or "—"
        header.update(
            f"[bold]spool tail[/] lane=[cyan]{agent_id}[/] tool_run_id=[grey62]{trid}[/]  [grey50](t/esc to close)[/]"
        )
        if error:
            log.write_line(f"[no spool] {error}")
        elif not text.strip():
            log.write_line("[spool is empty]")
        else:
            for line in text.splitlines():
                log.write_line(line)
        self.add_class("-visible")

    def hide(self) -> None:
        self.remove_class("-visible")


# ──────────────────────────────────────────────────────────────────────────────
# Main App
# ──────────────────────────────────────────────────────────────────────────────


class SubagentDashboard(App):
    """
    Subagent liveness dashboard.
    Polls /runs/current and /runs/{run_id}/lanes with since_snapshot_version.
    """

    TITLE = "vo-telemetry — Subagent Dashboard"
    SUB_TITLE = "Passive read-only view"

    CSS = """
    Screen {
        layout: vertical;
    }

    #main_area {
        layout: horizontal;
        height: 1fr;
    }

    #right_panel {
        layout: vertical;
        width: 1fr;
    }

    #lanes_scroll {
        height: 1fr;
    }

    PhaseRibbon {
        dock: top;
    }

    #filter_bar {
        height: 1;
    }

    ProviderRollupPanel {
        height: auto;
        max-height: 6;
    }

    ThroughputPulse {
        height: 1;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh_now", "Refresh"),
        Binding("o", "filter_orchestrator", "Filter orch"),
        Binding("s", "filter_run_state", "Filter run-state"),
        Binding("f", "clear_filter", "Clear filter"),
        Binding("j", "next_run", "Next run"),
        Binding("k", "prev_run", "Prev run"),
        Binding("t", "tail_spool", "Tail spool"),
        Binding("escape", "close_spool", "Close tail", show=False),
    ]

    # reactive state
    _selected_run_id: reactive[str | None] = reactive(None)
    _orchestrator_filter: reactive[str | None] = reactive(None)
    # Run selector filter by derived run-state: None=all, or one of
    # open/closed/abandoned/crashed. Cycled with the 's' key.
    _run_state_filter: reactive[str | None] = reactive(None)
    _all_runs: list[dict] = []
    _lanes_snapshot_version: int = 0
    _current_lanes: list[dict] = []
    _api_base: str = DEFAULT_API_BASE

    def __init__(self, api_base: str = DEFAULT_API_BASE, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._api_base = api_base
        self._all_runs = []
        self._lanes_snapshot_version = 0
        self._current_lanes = []
        self._run_state_filter = None
        # Cost provenance: tool_run_id -> {cost_source, pricing_version},
        # rebuilt each events-poll cycle from /runs/{id}/events. The lane snapshot
        # does NOT carry provenance, so this is the authoritative source for it.
        self._cost_provenance: dict[str, dict[str, str]] = {}
        # /events pagination watermark: the highest ingest_seq we've paged
        # for the selected run. Resets to 0 whenever the selected run changes.
        self._events_watermark: int = 0
        # Cost-ceiling guardrail config, env-overridable.
        self._provider_ceilings: dict[str, float] = _load_provider_ceilings()
        self._run_ceiling: float = _load_run_ceiling()
        # agent_id -> tool_run_id, for click-to-tail of the bounded spool.
        self._lane_tool_run_id: dict[str, str] = {}
        # Cooperative stop flag for the polling threads. Set on quit / Ctrl-C /
        # SIGTERM so the worker `while` loops exit promptly instead of spinning
        # forever. Threads sleep on this Event so shutdown is immediate.
        self._stop_event = threading.Event()
        # Set True once /readyz returns 200 — gates the data-poll workers so we
        # never render against a backend that hasn't initialized its snapshot.
        # NB: named _backend_ready, NOT _ready — App._ready is a Textual
        # internal coroutine and must not be shadowed by an attribute.
        self._backend_ready = False
        self._healthy = True

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield PhaseRibbon(id="phase_ribbon")
        yield ThroughputPulse(id="throughput_pulse")
        with Container(id="main_area"):
            yield RunSelectorPane(id="run_selector")
            with Vertical(id="right_panel"):
                yield FilterBar(id="filter_bar")
                yield SwimLanes(id="swim_lanes")
                yield CostCeilingBar(id="cost_ceiling_bar")
                yield ProviderRollupPanel(id="provider_rollup")
                yield SpoolTailPane(id="spool_tail")
        yield StatusBar(id="status_bar")
        yield Footer()

    def on_mount(self) -> None:
        status_bar: StatusBar = self.query_one("#status_bar", StatusBar)
        status_bar.set_connecting(self._api_base)
        filter_bar: FilterBar = self.query_one("#filter_bar", FilterBar)
        filter_bar.update_filters(None)
        # Install a process-signal handler so SIGTERM (and SIGINT, if Textual's
        # own Ctrl-C handling is bypassed) cleanly stops the poll threads.
        try:
            import signal

            signal.signal(signal.SIGTERM, self._on_signal)
        except (ValueError, OSError):
            # signal.signal only works on the main thread; if we're not there
            # (rare under some launchers) Textual's action_quit still drives
            # _stop_event via on_unmount, so degradation is graceful.
            pass
        # Do NOT start the data polls yet. First pass the /readyz startup gate
        # (rendering is blocked until /readyz returns 200). The gate worker
        # starts the data-poll workers once the backend reports ready.
        self._start_readyz_gate()

    def _on_signal(self, *_args: Any) -> None:
        """Signal handler — request cooperative shutdown of poll threads."""
        self._stop_event.set()
        # Schedule the Textual exit on the app's own loop.
        self.call_from_thread(self.exit)

    def on_unmount(self) -> None:
        """Textual teardown hook — ensure poll threads are told to stop."""
        self._stop_event.set()

    # ── readyz startup gate  ───────────────────────────────────────────

    @work(exclusive=True, thread=True)
    def _start_readyz_gate(self) -> None:
        """Block first data render until /readyz returns 200.

        /healthz says the process is alive; /readyz additionally confirms WAL
        reachable, reducer caught up, and snapshot initialized. We probe with a
        capped backoff and surface *why* it isn't ready in the status bar, then
        hand off to the data-poll workers exactly once.
        """
        sleep_for = READYZ_POLL_INTERVAL
        while not self._stop_event.is_set():
            status, body = fetch_readyz(self._api_base)
            if status == 200:
                self._backend_ready = True
                self.call_from_thread(self._on_ready)
                return
            # Build a human reason from the readyz body (or connectivity state).
            if status is None:
                reason = "unreachable"
                sleep_for = min(sleep_for * BACKOFF_FACTOR, READYZ_BACKOFF_MAX)
            else:
                reasons = []
                if body:
                    if not body.get("wal_reachable", True):
                        reasons.append("WAL unreachable")
                    if not body.get("reducer_initialized", True):
                        reasons.append("reducer init")
                    if not body.get("reducer_caught_up", True):
                        applied = body.get("last_ingest_seq_applied")
                        mx = body.get("max_ingest_seq")
                        reasons.append(f"reducer catching up ({applied}/{mx})")
                    if body.get("fatal_error"):
                        reasons.append(f"fatal: {body['fatal_error']}")
                reason = ", ".join(reasons) if reasons else f"HTTP {status}"
                sleep_for = READYZ_POLL_INTERVAL
            self.call_from_thread(self._show_waiting_ready, reason)
            self._stop_event.wait(sleep_for)

    def _on_ready(self) -> None:
        """UI thread: readyz passed — start the data-poll workers once."""
        self._start_poll_runs()
        self._start_poll_lanes()
        self._start_poll_events()

    def _show_waiting_ready(self, reason: str) -> None:
        status_bar: StatusBar = self.query_one("#status_bar", StatusBar)
        status_bar.set_waiting_ready(self._api_base, reason)

    # ── poll workers (with backoff + healthz-driven error state) ───────────────

    @work(exclusive=False, thread=True)
    def _start_poll_runs(self) -> None:
        """Background thread: poll /runs/current with exponential backoff.

        On failure we probe /healthz to distinguish a transient blip from a
        dead backend, surface a health-driven error state, and back off
        exponentially. The first success resets the backoff to the base poll.
        """
        backoff = BACKOFF_BASE
        while not self._stop_event.is_set():
            data = fetch_runs_current(self._api_base)
            if data is None:
                self.call_from_thread(self._handle_api_failure, "/runs/current")
                self._stop_event.wait(backoff)
                backoff = min(backoff * BACKOFF_FACTOR, BACKOFF_MAX)
                continue
            # success — reset backoff and clear any prior unhealthy state
            backoff = BACKOFF_BASE
            self.call_from_thread(self._handle_runs_data, data)
            self._stop_event.wait(POLL_INTERVAL_RUNS)

    @work(exclusive=False, thread=True)
    def _start_poll_lanes(self) -> None:
        """Background thread: poll /runs/{id}/lanes with delta versioning + backoff."""
        backoff = BACKOFF_BASE
        while not self._stop_event.is_set():
            run_id = self._selected_run_id
            if not run_id:
                self._stop_event.wait(POLL_INTERVAL_LANES)
                continue
            data = fetch_lanes(self._api_base, run_id, self._lanes_snapshot_version)
            if data is None:
                # lanes failure is non-fatal for the whole UI; the runs poll
                # owns the healthz/error surfacing. Just back off and retry.
                self._stop_event.wait(backoff)
                backoff = min(backoff * BACKOFF_FACTOR, BACKOFF_MAX)
                continue
            backoff = BACKOFF_BASE
            self.call_from_thread(self._handle_lanes_data, data)
            self._stop_event.wait(POLL_INTERVAL_LANES)

    @work(exclusive=False, thread=True)
    def _start_poll_events(self) -> None:
        """Background thread: page /runs/{id}/events to harvest cost provenance.

        Pages the raw event stream for the selected run via since_ingest_seq,
        following next_page_token across page boundaries, and rebuilds the
        tool_run_id -> {cost_source, pricing_version} map. This is the ONLY source
        of cost_source/pricing_version (the lane snapshot does not carry them).
        Polled at the lanes cadence; the per-run watermark resets on run change.
        """
        backoff = BACKOFF_BASE
        while not self._stop_event.is_set():
            run_id = self._selected_run_id
            if not run_id:
                self._stop_event.wait(POLL_INTERVAL_LANES)
                continue
            # Page from 0 each cycle so a re-priced tool (cost_source flipping from
            # unavailable -> pricing_table on a later tool_completed) is reflected;
            # the run event volume is bounded and EVENTS_MAX_PAGES caps the walk.
            events, _highest = fetch_all_events(self._api_base, run_id, 0)
            if not events:
                self._stop_event.wait(backoff)
                backoff = min(backoff * BACKOFF_FACTOR, BACKOFF_MAX)
                continue
            backoff = BACKOFF_BASE
            provenance = build_cost_provenance(events)
            highest = max(
                (e["ingest_seq"] for e in events if isinstance(e.get("ingest_seq"), int)),
                default=0,
            )
            self.call_from_thread(self._handle_events_data, run_id, provenance, highest)
            self._stop_event.wait(POLL_INTERVAL_LANES)

    def _handle_events_data(self, run_id: str, provenance: dict[str, dict[str, str]], watermark: int) -> None:
        """UI thread: apply harvested cost provenance + re-render rollup/ceilings."""
        # Ignore a late page for a run the user already navigated away from.
        if run_id != self._selected_run_id:
            return
        self._cost_provenance = provenance
        self._events_watermark = watermark
        self._render_cost_panels()

    def _render_cost_panels(self) -> None:
        """Re-render the provider rollup (with provenance) and the ceiling bar."""
        rollup: ProviderRollupPanel = self.query_one("#provider_rollup", ProviderRollupPanel)
        rollup.update_rollup(self._current_lanes, self._cost_provenance)
        ceiling_bar: CostCeilingBar = self.query_one("#cost_ceiling_bar", CostCeilingBar)
        alarms = evaluate_ceilings(
            self._current_lanes,
            self._provider_ceilings,
            self._run_ceiling,
            self._cost_provenance,
        )
        ceiling_bar.update_ceilings(alarms)

    def _handle_api_failure(self, endpoint: str) -> None:
        """UI thread: an API call failed — probe /healthz to drive error state."""
        status, _body = fetch_healthz(self._api_base)
        status_bar: StatusBar = self.query_one("#status_bar", StatusBar)
        if status == 200:
            # process alive but the data endpoint blipped — soft error, no crash
            self._healthy = True
            status_bar.set_error(self._api_base, f"{endpoint} blip (healthz OK)")
        else:
            self._healthy = False
            msg = "no /healthz response" if status is None else f"/healthz HTTP {status}"
            status_bar.set_unhealthy(self._api_base, msg)

    # ── data handlers (called on UI thread) ──────────────────────────────────

    def _handle_runs_data(self, data: dict | None) -> None:
        status_bar: StatusBar = self.query_one("#status_bar", StatusBar)
        if data is None:
            status_bar.set_error(self._api_base, "no response from /runs/current")
            return

        runs = data.get("runs") or []
        snap_v = data.get("snapshot_version") or 0
        server_time = data.get("server_time")

        status_bar.set_ok(self._api_base, snap_v)

        # update throughput pulse
        pulse: ThroughputPulse = self.query_one("#throughput_pulse", ThroughputPulse)
        pulse.update_pulse(snap_v, server_time)

        self._all_runs = runs

        # update run selector (honoring any active run-state filter)
        selector: RunSelectorPane = self.query_one("#run_selector", RunSelectorPane)
        selector.update_runs(runs, self._run_state_filter)

        # auto-select first open run if nothing selected
        if self._selected_run_id is None and runs:
            open_runs = [r for r in runs if r.get("state") == "open"]
            target = open_runs[0] if open_runs else runs[0]
            self._selected_run_id = target.get("run_id")

        # update phase ribbon for selected run
        selected_run = next((r for r in runs if r.get("run_id") == self._selected_run_id), None)
        if selected_run is None and runs:
            selected_run = runs[0]

        ribbon: PhaseRibbon = self.query_one("#phase_ribbon", PhaseRibbon)
        ribbon.update_run(selected_run)

    def _handle_lanes_data(self, data: dict | None) -> None:
        if data is None:
            return

        new_version = data.get("snapshot_version") or 0
        if new_version > self._lanes_snapshot_version:
            self._lanes_snapshot_version = new_version

        lanes = data.get("lanes") or []
        if lanes:
            # merge incoming lanes into current_lanes by agent_id
            lane_map = {la["agent_id"]: la for la in self._current_lanes if la.get("agent_id")}
            for la in lanes:
                aid = la.get("agent_id")
                if aid:
                    lane_map[aid] = la
            self._current_lanes = list(lane_map.values())

        # Rebuild agent_id -> tool_run_id for click-to-tail. A lane may
        # have several tools; we tail the most-recently-active one so 't' shows
        # the live child's spool.
        self._rebuild_lane_tool_map()

        # refresh the per-provider swim lanes
        swim: SwimLanes = self.query_one("#swim_lanes", SwimLanes)
        swim.update_lanes(self._current_lanes, self._orchestrator_filter)

        # Re-render cost panels with the latest provenance + ceiling evaluation.
        self._render_cost_panels()

    def _rebuild_lane_tool_map(self) -> None:
        """agent_id -> tool_run_id (most-recently-active tool) for click-to-tail."""
        mapping: dict[str, str] = {}
        for lane in self._current_lanes:
            aid = lane.get("agent_id")
            tools = lane.get("tools") or {}
            if not aid or not tools:
                continue

            def _key(item: tuple[str, dict]) -> str:
                t = item[1]
                return t.get("last_activity_ts") or t.get("completed_ts") or t.get("started_ts") or ""

            trid = max(tools.items(), key=_key)[0]
            mapping[aid] = trid
        self._lane_tool_run_id = mapping

    # ── run selection from ListView ───────────────────────────────────────────

    @on(ListView.Selected)
    def on_run_selected(self, event: ListView.Selected) -> None:
        item = event.item
        if item.id and item.id.startswith("run_"):
            # Recover the true run_id (with its colon) from the selector's
            # reverse map; fall back to the slice for ids that needed no mangling.
            selector: RunSelectorPane = self.query_one("#run_selector", RunSelectorPane)
            run_id = selector._id_to_run_id.get(item.id, item.id[4:])
            if run_id != self._selected_run_id:
                self._select_run(run_id)

    def _select_run(self, run_id: str | None) -> None:
        """Switch the selected run, resetting all per-run watermarks."""
        if run_id == self._selected_run_id:
            return
        self._selected_run_id = run_id
        self._lanes_snapshot_version = 0
        self._current_lanes = []
        # reset cost provenance + the /events pagination watermark for the new run
        self._cost_provenance = {}
        self._events_watermark = 0
        self._lane_tool_run_id = {}
        # close any open spool tail (it belonged to the previous run)
        try:
            self.query_one("#spool_tail", SpoolTailPane).hide()
        except Exception:
            pass
        selected_run = next((r for r in self._all_runs if r.get("run_id") == run_id), None)
        ribbon: PhaseRibbon = self.query_one("#phase_ribbon", PhaseRibbon)
        ribbon.update_run(selected_run)
        self._render_cost_panels()

    # ── key actions ───────────────────────────────────────────────────────────

    def action_refresh_now(self) -> None:
        """Force an immediate one-shot refresh without spawning new pollers."""
        self._refresh_once()

    @work(exclusive=False, thread=True)
    def _refresh_once(self) -> None:
        """One-shot fetch off the UI thread (bound to 'r'). Honors stop flag."""
        if self._stop_event.is_set():
            return
        data = fetch_runs_current(self._api_base)
        if data is None:
            self.call_from_thread(self._handle_api_failure, "/runs/current")
            return
        self.call_from_thread(self._handle_runs_data, data)
        run_id = self._selected_run_id
        if run_id:
            ldata = fetch_lanes(self._api_base, run_id, self._lanes_snapshot_version)
            if ldata is not None:
                self.call_from_thread(self._handle_lanes_data, ldata)
            # also refresh cost provenance from /events on a manual refresh
            events, _highest = fetch_all_events(self._api_base, run_id, 0)
            if events:
                provenance = build_cost_provenance(events)
                highest = max(
                    (e["ingest_seq"] for e in events if isinstance(e.get("ingest_seq"), int)),
                    default=0,
                )
                self.call_from_thread(self._handle_events_data, run_id, provenance, highest)

    def action_filter_orchestrator(self) -> None:
        """Cycle through known orchestrators."""
        orchestrators = sorted(set(la.get("orchestrator") or "unknown" for la in self._current_lanes))
        if not orchestrators:
            return
        current = self._orchestrator_filter
        if current is None or current not in orchestrators:
            new_filter = orchestrators[0]
        else:
            idx = orchestrators.index(current)
            new_filter = orchestrators[(idx + 1) % len(orchestrators)]
        self._orchestrator_filter = new_filter
        self._refresh_filter_bar()
        swim: SwimLanes = self.query_one("#swim_lanes", SwimLanes)
        swim.update_lanes(self._current_lanes, self._orchestrator_filter)

    def action_clear_filter(self) -> None:
        """Clear all filters (orchestrator + run-state)."""
        self._orchestrator_filter = None
        self._run_state_filter = None
        self._refresh_filter_bar()
        swim: SwimLanes = self.query_one("#swim_lanes", SwimLanes)
        swim.update_lanes(self._current_lanes, None)
        # rebuild the run list so the run-state filter is reflected immediately
        self._rebuild_run_selector()

    def _refresh_filter_bar(self) -> None:
        filter_bar: FilterBar = self.query_one("#filter_bar", FilterBar)
        filter_bar.update_filters(self._orchestrator_filter, self._run_state_filter)

    def _rebuild_run_selector(self) -> None:
        selector: RunSelectorPane = self.query_one("#run_selector", RunSelectorPane)
        selector.update_runs(self._all_runs, self._run_state_filter)

    def action_filter_run_state(self) -> None:
        """Cycle the run-selector filter through the derived run states.

        Order: None (all) -> open -> closed -> abandoned -> crashed -> None.
        """
        cycle: list[str | None] = [None, *RUN_STATES]
        cur = self._run_state_filter
        idx = cycle.index(cur) if cur in cycle else 0
        self._run_state_filter = cycle[(idx + 1) % len(cycle)]
        self._refresh_filter_bar()
        self._rebuild_run_selector()

    def action_next_run(self) -> None:
        """Select next run in list (run-state filter aware)."""
        ids = self._visible_run_ids()
        if not ids:
            return
        cur = self._selected_run_id
        new_id = ids[0] if cur not in ids else ids[(ids.index(cur) + 1) % len(ids)]
        self._select_run(new_id)

    def action_prev_run(self) -> None:
        """Select previous run in list (run-state filter aware)."""
        ids = self._visible_run_ids()
        if not ids:
            return
        cur = self._selected_run_id
        new_id = ids[-1] if cur not in ids else ids[(ids.index(cur) - 1) % len(ids)]
        self._select_run(new_id)

    def _visible_run_ids(self) -> list[str]:
        """run_ids honoring the active run-state filter (matches the selector)."""
        runs = self._all_runs
        if self._run_state_filter is not None:
            runs = [r for r in runs if r.get("state") == self._run_state_filter]
        return [str(r["run_id"]) for r in runs if r.get("run_id")]

    # ── click-to-tail of the bounded shim spool  ────────────────

    @on(DataTable.RowSelected)
    def on_lane_row_selected(self, event: DataTable.RowSelected) -> None:
        """Selecting a lane row (mouse click or enter) tails its spool file."""
        try:
            row = event.data_table.get_row_at(event.cursor_row)
        except Exception:
            return
        if not row:
            return
        agent_id = str(row[0])  # column 0 is the raw agent_id (no markup)
        self._tail_lane(agent_id)

    def action_tail_spool(self) -> None:
        """Key 't': tail the spool of the lane under the focused table cursor."""
        agent_id = self._focused_agent_id()
        if agent_id is None:
            self._spool_message("no lane selected — focus a lane row, then press t")
            return
        self._tail_lane(agent_id)

    def action_close_spool(self) -> None:
        try:
            self.query_one("#spool_tail", SpoolTailPane).hide()
        except Exception:
            pass

    def _focused_agent_id(self) -> str | None:
        """agent_id of the row under the cursor of the focused lane DataTable."""
        focused = self.focused
        if isinstance(focused, DataTable):
            try:
                row = focused.get_row_at(focused.cursor_row)
            except Exception:
                return None
            if row:
                return str(row[0])
        # fall back to any lane table that has a cursor row
        for dt in self.query(DataTable):
            try:
                row = dt.get_row_at(dt.cursor_row)
            except Exception:
                continue
            if row:
                return str(row[0])
        return None

    def _tail_lane(self, agent_id: str) -> None:
        trid = self._lane_tool_run_id.get(agent_id)
        text, error = read_spool_tail(trid) if trid else ("", "no tool_run_id for this lane")
        pane: SpoolTailPane = self.query_one("#spool_tail", SpoolTailPane)
        pane.show_tail(agent_id, trid, text, error)

    def _spool_message(self, msg: str) -> None:
        pane: SpoolTailPane = self.query_one("#spool_tail", SpoolTailPane)
        pane.show_tail("—", None, "", msg)


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────


def _http_url(value: str) -> str:
    """argparse type for --api-base: only http(s) URLs; trailing slash removed."""
    parts = urllib.parse.urlsplit(value)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise argparse.ArgumentTypeError(f"expected an http(s) URL, got {value!r}")
    return value.rstrip("/")


def main() -> None:
    parser = argparse.ArgumentParser(description="vo-telemetry subagent liveness dashboard")
    parser.add_argument(
        "--api-base",
        type=_http_url,
        default=DEFAULT_API_BASE,
        help=f"Base URL for the liveness snapshot API (default: {DEFAULT_API_BASE})",
    )
    args = parser.parse_args()
    app = SubagentDashboard(api_base=args.api_base)
    app.run()


if __name__ == "__main__":
    main()
