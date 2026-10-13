from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from .pricing import PricingTable


@dataclass(frozen=True)
class ParsedEmission:
    event_type: str
    fields: dict[str, Any]


@dataclass
class ParserState:
    model: str | None = None
    tokens_total: dict[str, float] = field(default_factory=lambda: {"in": 0, "out": 0})
    cost_usd_total: float | None = None
    cost_source: str = "unavailable"
    pricing_version: str = "unavailable"
    # Last terminal agent message seen in the child's JSONL stream. The shim uses it for
    # lane_stopped's final_message preview/sha (content off by default) and to honor an inline
    # TELEMETRY_OUTCOME marker if the agent emitted one. Never the full transcript.
    final_message: str = ""


class StreamParser:
    def __init__(self, cli: str, pricing: PricingTable):
        self.cli = cli
        self.pricing = pricing
        self.state = ParserState(pricing_version=pricing.effective_date)

    def consume_line(self, line: str) -> list[ParsedEmission]:
        text = line.strip()
        if not text:
            return []
        try:
            item = json.loads(text)
        except json.JSONDecodeError:
            return [ParsedEmission("tool_activity", {"activity": text[:512], "ext": {"stream": self.cli}})]
        if not isinstance(item, dict):
            return []

        emissions: list[ParsedEmission] = []
        model = _find_first_str(item, ("model", "model_id", "modelID"))
        if model:
            self.state.model = model

        final = _final_agent_message(item)
        if final:
            self.state.final_message = final

        activity = _activity_label(item)
        if activity:
            emissions.append(ParsedEmission("tool_activity", {"activity": activity, "ext": {"stream": self.cli}}))

        # Token/cost accounting is CLI-shaped:
        #  - opencode emits PER-STEP usage under part.tokens / part.cost on `step_finish` events
        #    (the per-step counts are non-monotonic, e.g. 18623, 2348, 522), so each step is an
        #    increment to SUM — a cumulative-delta would discard every step after a smaller one.
        #  - codex (and the default) report a CUMULATIVE running total, so we delta it.
        if self.cli == "opencode":
            step = _opencode_step_usage(item)
            if step is not None:
                if step["cost"] is not None:
                    self.state.cost_usd_total = (self.state.cost_usd_total or 0.0) + step["cost"]
                    self.state.cost_source = "provider_reported"
                    self.state.pricing_version = "provider_reported"
                delta = {"in": step["in"], "out": step["out"]}
                self.state.tokens_total = {
                    "in": self.state.tokens_total.get("in", 0) + step["in"],
                    "out": self.state.tokens_total.get("out", 0) + step["out"],
                }
                if delta["in"] or delta["out"]:
                    emissions.append(
                        ParsedEmission(
                            "token_delta", {"tokens_delta": delta, "tokens_total": dict(self.state.tokens_total)}
                        )
                    )
            return emissions

        provider_cost = _extract_cost(item)
        if provider_cost is not None:
            self.state.cost_usd_total = provider_cost
            self.state.cost_source = "provider_reported"
            self.state.pricing_version = "provider_reported"

        total = _extract_tokens_total(item)
        if total is not None:
            delta = {
                "in": max(0, total["in"] - self.state.tokens_total.get("in", 0)),
                "out": max(0, total["out"] - self.state.tokens_total.get("out", 0)),
            }
            self.state.tokens_total = total
            if delta["in"] or delta["out"]:
                emissions.append(ParsedEmission("token_delta", {"tokens_delta": delta, "tokens_total": total}))
        return emissions

    def terminal_cost_fields(self) -> dict[str, Any]:
        if self.state.cost_source == "provider_reported" and self.state.cost_usd_total is not None:
            return {
                "cost_usd_total": self.state.cost_usd_total,
                "cost_source": "provider_reported",
                "pricing_version": "provider_reported",
            }
        priced = self.pricing.cost_for(self.state.model, self.state.tokens_total)
        if priced["cost_source"] == "pricing_table":
            return priced
        return {
            "cost_source": "unavailable",
            "pricing_version": priced["pricing_version"],
        }


def _final_agent_message(item: dict[str, Any]) -> str | None:
    """Extract the CLI's terminal agent message text (NOT the full transcript), tolerant of the
    codex and opencode JSONL shapes."""
    # codex `exec --json`: {"type":"item.completed","item":{"type":"agent_message","text":"..."}}
    if item.get("type") == "item.completed":
        inner = item.get("item")
        if isinstance(inner, dict) and inner.get("type") == "agent_message":
            text = inner.get("text")
            if isinstance(text, str) and text:
                return text
    # codex flat variant: {"type":"agent_message","text"/"message":"..."}
    if item.get("type") == "agent_message":
        for key in ("text", "message"):
            value = item.get(key)
            if isinstance(value, str) and value:
                return value
    # opencode: final assistant text on session.idle / message.completed / message.updated
    if item.get("type") in ("session.idle", "message.completed", "message.updated"):
        message = _find_first_str(item, ("text",))
        if message:
            return message
    return None


def _activity_label(item: dict[str, Any]) -> str | None:
    value = _find_first_str(item, ("type", "event", "kind", "name"))
    if value:
        return value[:512]
    message = _find_first_str(item, ("message", "delta", "text"))
    return message[:512] if message else None


def _find_first_str(item: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = item.get(key)
        if isinstance(value, str) and value:
            return value
    for value in item.values():
        if isinstance(value, dict):
            found = _find_first_str(value, keys)
            if found:
                return found
    return None


def _extract_cost(item: dict[str, Any]) -> float | None:
    for key in ("cost", "cost_usd", "costUSD", "cost_usd_total"):
        value = item.get(key)
        if isinstance(value, (int, float)) and value >= 0:
            return float(value)
    usage = item.get("usage")
    if isinstance(usage, dict):
        return _extract_cost(usage)
    return None


def _extract_tokens_total(item: dict[str, Any]) -> dict[str, float] | None:
    for container_key in ("usage", "tokens", "token_usage", "tokenUsage"):
        value = item.get(container_key)
        if isinstance(value, dict):
            tokens = _tokens_from_mapping(value)
            if tokens is not None:
                return tokens
    return _tokens_from_mapping(item)


def _tokens_from_mapping(value: dict[str, Any]) -> dict[str, float] | None:
    in_tokens = _number(value, "input_tokens", "input", "in", "prompt_tokens", "promptTokens")
    out_tokens = _number(value, "output_tokens", "output", "out", "completion_tokens", "completionTokens")
    if in_tokens is None and out_tokens is None:
        return None
    return {"in": float(in_tokens or 0), "out": float(out_tokens or 0)}


def _opencode_step_usage(item: dict[str, Any]) -> dict[str, Any] | None:
    """opencode reports PER-STEP token/cost usage nested under `part.tokens` / `part.cost` on
    `step_finish` events (verified on the live stream: only step_finish carries part.tokens, and the
    per-step counts are non-monotonic — i.e. per-step, NOT a cumulative counter). Returns one step's
    {"in","out","cost"} to be SUMMED, or None if this event carries no step usage. Gating on
    type=="step_finish" avoids double-counting from other event shapes."""
    if item.get("type") != "step_finish":
        return None
    part = item.get("part")
    if not isinstance(part, dict):
        return None
    token_block = part.get("tokens")
    tokens = _tokens_from_mapping(token_block) if isinstance(token_block, dict) else None
    cost = part.get("cost")
    cost_val = float(cost) if isinstance(cost, (int, float)) and not isinstance(cost, bool) and cost >= 0 else None
    if tokens is None and cost_val is None:
        return None
    return {"in": (tokens or {}).get("in", 0.0), "out": (tokens or {}).get("out", 0.0), "cost": cost_val}


def _number(value: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        amount = value.get(key)
        if isinstance(amount, (int, float)) and amount >= 0:
            return float(amount)
    return None
