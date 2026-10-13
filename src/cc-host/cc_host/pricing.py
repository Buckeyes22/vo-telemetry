from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .paths import pricing_file


@dataclass(frozen=True)
class PricingTable:
    effective_date: str
    currency: str
    rates: dict[str, dict[str, float]]

    @classmethod
    def empty(cls) -> "PricingTable":
        return cls(effective_date="unavailable", currency="USD", rates={})

    @classmethod
    def load(cls, path: str | Path | None = None) -> "PricingTable":
        file_path = Path(path) if path is not None else pricing_file()
        if not file_path.exists():
            return cls.empty()
        data = yaml.safe_load(file_path.read_text(encoding="utf-8")) or {}
        pricing = data.get("pricing", {})
        models = pricing.get("models", {}) or {}
        rates: dict[str, dict[str, float]] = {}
        for model, entry in models.items():
            if not isinstance(entry, dict):
                continue
            try:
                rates[str(model)] = {
                    "input_per_1m": float(entry["input_per_1m"]),
                    "output_per_1m": float(entry["output_per_1m"]),
                }
            except (KeyError, TypeError, ValueError):
                continue
        return cls(
            effective_date=str(pricing.get("effective_date", "unavailable")),
            currency=str(pricing.get("currency", "USD")),
            rates=rates,
        )

    def cost_for(self, model: str | None, tokens_total: dict[str, float]) -> dict[str, Any]:
        if not model or model not in self.rates:
            return {
                "cost_usd_total": None,
                "cost_source": "unavailable",
                "pricing_version": self.effective_date,
            }
        rate = self.rates[model]
        total = (tokens_total.get("in", 0) / 1_000_000) * rate["input_per_1m"]
        total += (tokens_total.get("out", 0) / 1_000_000) * rate["output_per_1m"]
        return {
            "cost_usd_total": round(total, 8),
            "cost_source": "pricing_table",
            "pricing_version": self.effective_date,
        }
