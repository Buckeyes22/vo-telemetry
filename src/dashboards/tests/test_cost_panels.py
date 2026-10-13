"""Unit tests for cost provenance, ceilings, /events pagination and spool tail.

These exercise the pure functions the dashboard relies on, against the SAME
field shapes represented by synthetic liveness backend fixtures.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dashboards import subagent_dashboard as d  # noqa: E402


# ── cost provenance from raw /events ──────────────────────────────────────────


def test_build_cost_provenance_maps_tool_run_id_to_source_and_version():
    events = [
        {"event_type": "tool_started", "tool_run_id": "t1"},
        {
            "event_type": "tool_completed",
            "tool_run_id": "t1",
            "cost_usd_total": 1.85,
            "cost_source": "pricing_table",
            "pricing_version": "2026-05-31",
        },
        {
            "event_type": "tool_completed",
            "tool_run_id": "t2",
            "cost_usd_total": 0.0,
            "cost_source": "unavailable",
            "pricing_version": "2026-05-31",
        },
    ]
    prov = d.build_cost_provenance(events)
    assert prov["t1"] == {"cost_source": "pricing_table", "pricing_version": "2026-05-31"}
    assert prov["t2"]["cost_source"] == "unavailable"


def test_provenance_last_wins_when_resource_repriced():
    # tool first reported unavailable, then re-completed as priced — last wins.
    events = [
        {"tool_run_id": "t1", "cost_source": "unavailable", "pricing_version": "2026-05-31"},
        {"tool_run_id": "t1", "cost_source": "pricing_table", "pricing_version": "2026-05-31"},
    ]
    assert d.build_cost_provenance(events)["t1"]["cost_source"] == "pricing_table"


# ── provider rollup with provenance ───────────────────────────────────────────


def _lane(provider, trid, cost, tin=0, tout=0):
    return {
        "agent_id": f"a_{provider}",
        "provider": provider,
        "tools": {trid: {"tokens_in": tin, "tokens_out": tout, "cost_usd_total": cost}},
    }


def test_rollup_segregates_unavailable_cost_from_trustworthy_cost():
    lanes = [
        _lane("anthropic", "t1", 1.85, tin=120000, tout=8000),
        _lane("glm", "t2", 0.0, tin=40000, tout=3000),
    ]
    prov = {
        "t1": {"cost_source": "pricing_table", "pricing_version": "2026-05-31"},
        "t2": {"cost_source": "unavailable", "pricing_version": "2026-05-31"},
    }
    rollup = d._provider_rollup(lanes, prov)
    assert rollup["anthropic"]["cost_usd"] == pytest.approx(1.85)
    assert "pricing_table" in rollup["anthropic"]["sources"]
    # glm cost is 'unavailable' → it must NOT land in the trustworthy total
    assert rollup["glm"]["cost_usd"] == 0.0
    assert rollup["glm"]["unavailable_cost"] == 0.0  # the unavailable cost was 0
    assert rollup["glm"]["sources"] == {"unavailable"}


def test_rollup_unavailable_with_nonzero_number_is_excluded_from_total():
    # a provider that reported a number but tagged it unavailable must NOT have it
    # counted as a trustworthy cost (no guessed values).
    lanes = [_lane("glm", "t2", 9.99)]
    prov = {"t2": {"cost_source": "unavailable", "pricing_version": "x"}}
    rollup = d._provider_rollup(lanes, prov)
    assert rollup["glm"]["cost_usd"] == 0.0
    assert rollup["glm"]["unavailable_cost"] == pytest.approx(9.99)


# ── cost-ceiling guardrail ────────────────────────────────────────────────────


def test_ceiling_breach_and_warn_bands():
    lanes = [_lane("anthropic", "t1", 4.6)]  # ceiling 5.0 → warn at 4.0, breach at 5.0
    prov = {"t1": {"cost_source": "pricing_table"}}
    alarms = d.evaluate_ceilings(lanes, {"anthropic": 5.0}, run_ceiling=100.0, provenance=prov)
    sev = {scope: s for (s, scope, _c, _ceil) in alarms}
    assert sev["provider:anthropic"] == "warn"

    lanes2 = [_lane("anthropic", "t1", 5.5)]
    alarms2 = d.evaluate_ceilings(lanes2, {"anthropic": 5.0}, run_ceiling=100.0, provenance=prov)
    sev2 = {scope: s for (s, scope, _c, _ceil) in alarms2}
    assert sev2["provider:anthropic"] == "breach"


def test_ceiling_ignores_unavailable_cost():
    # a big 'unavailable' number must never trip a ceiling alarm.
    lanes = [_lane("glm", "t2", 999.0)]
    prov = {"t2": {"cost_source": "unavailable"}}
    alarms = d.evaluate_ceilings(lanes, {"glm": 2.0}, run_ceiling=2.0, provenance=prov)
    assert alarms == []


def test_run_total_ceiling_aggregates_providers():
    lanes = [_lane("anthropic", "t1", 3.0), _lane("openai", "t2", 4.0)]
    prov = {"t1": {"cost_source": "pricing_table"}, "t2": {"cost_source": "provider_reported"}}
    # run ceiling 5.0 → run-total 7.0 breaches
    alarms = d.evaluate_ceilings(lanes, {}, run_ceiling=5.0, provenance=prov)
    run = [a for a in alarms if a[1] == "run-total"]
    assert run and run[0][0] == "breach"
    assert run[0][2] == pytest.approx(7.0)


# ── /events pagination via since_ingest_seq ───────────────────────────────────


def test_fetch_all_events_pages_past_boundary(monkeypatch):
    # simulate a backend that returns 2 pages then stops.
    pages = {
        0: {
            "events": [{"ingest_seq": 1, "tool_run_id": "t1"}, {"ingest_seq": 2, "tool_run_id": "t1"}],
            "next_page_token": "2",
        },
        2: {"events": [{"ingest_seq": 3, "tool_run_id": "t2"}], "next_page_token": None},
    }
    calls: list[int] = []

    def fake_page(api_base, run_id, since, limit=d.EVENTS_PAGE_SIZE):
        calls.append(since)
        return pages.get(since)

    monkeypatch.setattr(d, "fetch_events_page", fake_page)
    events, highest = d.fetch_all_events("http://x", "run:1", 0)
    assert [e["ingest_seq"] for e in events] == [1, 2, 3]
    assert highest == 3
    # cursor advanced 0 -> 2 (past the first page boundary), then stopped
    assert calls == [0, 2]


def test_fetch_all_events_stops_on_nonadvancing_token(monkeypatch):
    # a malformed backend returning the same token must not infinite-loop.
    def fake_page(api_base, run_id, since, limit=d.EVENTS_PAGE_SIZE):
        return {"events": [{"ingest_seq": 1, "tool_run_id": "t"}], "next_page_token": "0"}

    monkeypatch.setattr(d, "fetch_events_page", fake_page)
    events, _ = d.fetch_all_events("http://x", "run:1", 0)
    assert len(events) == 1  # one page, then bailed on non-advancing token


# ── spool tail (bounded, path-confined) ───────────────────────────────────────


def test_read_spool_tail_reads_bounded_file(tmp_path, monkeypatch):
    monkeypatch.setattr(d, "SPOOL_DIR", tmp_path)
    trid = "abc-123"
    (tmp_path / f"{trid}.log").write_text("stdout: hello\nstdout: world\n")
    text, err = d.read_spool_tail(trid)
    assert err is None
    assert "hello" in text and "world" in text


def test_read_spool_tail_missing_file_reports_error(tmp_path, monkeypatch):
    monkeypatch.setattr(d, "SPOOL_DIR", tmp_path)
    text, err = d.read_spool_tail("does-not-exist")
    assert text == ""
    assert err and "no spool file" in err


def test_read_spool_tail_refuses_path_escape(tmp_path, monkeypatch):
    monkeypatch.setattr(d, "SPOOL_DIR", tmp_path)
    text, err = d.read_spool_tail("../../etc/passwd")
    assert text == ""
    assert err and "escapes" in err


# ── lane cost cell and provider colours ───────────────────────────────────────


def test_lane_cost_cell_shows_dash_when_no_cost_reported():
    assert d._lane_cost_cell(0.0) == "[grey50]—[/]"


def test_lane_cost_cell_keeps_small_positive_cost():
    assert d._lane_cost_cell(0.0004) == "$0.00040"
    assert d._lane_cost_cell(2.5) == "$2.50"


def test_lane_row_uses_dash_for_zero_cost():
    lane = {
        "agent_id": "a1",
        "agent_type": "synthetic",
        "status": "done",
        "tools": {"t1": {"tokens_in": 10, "tokens_out": 2, "cost_usd_total": 0.0}},
    }
    assert d._lane_row(lane)[6] == "[grey50]—[/]"  # column 6 = Cost


def test_provider_colours_are_readable_on_dark_background():
    dark_ansi = {"blue", "green", "magenta", "red", "black"}
    assert not dark_ansi.intersection(d.PROVIDER_COLORS.values())


def test_api_base_type_accepts_http_and_rejects_file():
    import argparse

    assert d._http_url("http://127.0.0.1:8723/") == "http://127.0.0.1:8723"
    with pytest.raises(argparse.ArgumentTypeError):
        d._http_url("file:///etc/passwd")


# ── swim-lane mount race ──────────────────────────────────────────────────────


def test_second_poll_before_section_mounts_does_not_crash():
    import asyncio

    from textual.app import App
    from textual.widgets import DataTable

    class Host(App):
        def compose(self):
            yield d.SwimLanes(id="stack")

    async def scenario():
        app = Host()
        async with app.run_test() as pilot:
            stack = app.query_one("#stack", d.SwimLanes)
            lanes = [{"agent_id": "a1", "provider": "demo", "tools": {}}]
            stack.update_lanes(lanes)
            stack.update_lanes(lanes)  # next poll lands before the new section's children mount
            await pilot.pause()
            await pilot.pause()
            assert stack._sections["demo"].query_one(DataTable).row_count == 1

    asyncio.run(scenario())


def test_fmt_tokens_renders_whole_numbers_without_decimal():
    # The backend stores token counts as floats; the table must still read as integers.
    assert d._fmt_tokens(240.0) == "240"
    assert d._fmt_tokens(0.0) == "0"
    assert d._fmt_tokens(1200.0) == "1.2k"
