from __future__ import annotations

import json

from cc_host.pricing import PricingTable
from cc_host.stream_parser import StreamParser


def test_codex_json_events_emit_activity_and_token_delta() -> None:
    parser = StreamParser("codex", PricingTable.empty())

    emissions = parser.consume_line(
        json.dumps(
            {
                "type": "agent_message_delta",
                "delta": "hello",
                "model": "gpt-5.4-mini",
                "usage": {"input_tokens": 10, "output_tokens": 4},
            }
        )
    )

    assert [item.event_type for item in emissions] == ["tool_activity", "token_delta"]
    assert emissions[0].fields["activity"] == "agent_message_delta"
    assert emissions[1].fields["tokens_delta"] == {"in": 10, "out": 4}
    assert parser.state.model == "gpt-5.4-mini"
    assert parser.state.tokens_total == {"in": 10, "out": 4}


def test_opencode_step_finish_emits_token_delta_and_cost_from_nested_part() -> None:
    """opencode nests usage under part.tokens / part.cost on `step_finish` events (the REAL shape on
    the live stream). Regression for the bug where extraction only looked at the top level and dropped
    ALL opencode token/cost telemetry (every opencode lane showed 0 tokens / None cost)."""
    parser = StreamParser("opencode", PricingTable.empty())

    emissions = parser.consume_line(
        json.dumps(
            {
                "type": "step_finish",
                "sessionID": "ses_x",
                "part": {
                    "type": "step-finish",
                    "tokens": {
                        "total": 20728,
                        "input": 18623,
                        "output": 199,
                        "reasoning": 0,
                        "cache": {"write": 0, "read": 1906},
                    },
                    "cost": 0.01188012,
                },
                "model": "minimax/MiniMax-M3",
            }
        )
    )

    assert "token_delta" in [item.event_type for item in emissions]
    td = next(item for item in emissions if item.event_type == "token_delta")
    assert td.fields["tokens_delta"] == {"in": 18623, "out": 199}
    assert parser.state.model == "minimax/MiniMax-M3"
    assert parser.state.cost_usd_total == 0.01188012


def test_opencode_tokens_are_summed_per_step_not_cumulative_delta() -> None:
    """opencode's per-step counts are non-monotonic (e.g. 18623, 2348, 522). They must be SUMMED;
    the old cumulative-delta (max(0, total-prev)) discarded every step after a smaller one — which is
    why opencode lanes reported almost no tokens."""
    parser = StreamParser("opencode", PricingTable.empty())
    steps = [(18623, 199, 0.0119), (2348, 131, 0.0042), (522, 380, 0.0040)]
    total_in = total_out = 0
    deltas_in = []
    for tin, tout, cost in steps:
        em = parser.consume_line(
            json.dumps(
                {
                    "type": "step_finish",
                    "part": {"type": "step-finish", "tokens": {"input": tin, "output": tout}, "cost": cost},
                }
            )
        )
        td = next(i for i in em if i.event_type == "token_delta")
        deltas_in.append(td.fields["tokens_delta"]["in"])
        total_in += tin
        total_out += tout

    # every per-step delta is captured (NOT zeroed out after a smaller step)
    assert deltas_in == [18623, 2348, 522]
    assert parser.state.tokens_total == {"in": total_in, "out": total_out}  # {"in": 21493, "out": 710}
    assert parser.state.cost_usd_total is not None
    assert abs(parser.state.cost_usd_total - (0.0119 + 0.0042 + 0.0040)) < 1e-9


def test_pricing_table_cost_is_used_when_provider_cost_is_absent() -> None:
    pricing = PricingTable(
        effective_date="2026-05-31",
        currency="USD",
        rates={"exact-model": {"input_per_1m": 2.0, "output_per_1m": 10.0}},
    )
    parser = StreamParser("codex", pricing)
    parser.consume_line(
        json.dumps(
            {
                "type": "turn_end",
                "model": "exact-model",
                "usage": {"input_tokens": 1_000_000, "output_tokens": 500_000},
            }
        )
    )

    assert parser.terminal_cost_fields() == {
        "cost_usd_total": 7.0,
        "cost_source": "pricing_table",
        "pricing_version": "2026-05-31",
    }
