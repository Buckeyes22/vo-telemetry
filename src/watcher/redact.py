"""Redaction helpers — must be applied before any outbound send.

Rules:
- Any field whose name appears in the config's ``redact_fields`` set has its
  value replaced with the literal string ``[REDACTED]``.
- Applies recursively to nested dicts.
- Does NOT modify the original object; always returns a new dict.
- No exceptions are swallowed — if the input is not a dict, a TypeError is
  raised so callers cannot accidentally pass unredacted text.

Structured message builders must apply ``redact_dict`` before formatting.
``redact_text`` only caps length; it is not secret detection. Fields outside the
configured set and arbitrary free text can still contain sensitive material.
"""

from __future__ import annotations

from typing import Any, Dict, FrozenSet


_DEFAULT_REDACT: FrozenSet[str] = frozenset(
    {
        "final_message_preview",
        "summary",
        "prompt",
        "response",
        "content",
        "db.statement",
        "http.request.body",
        "http.response.body",
    }
)


def redact_dict(
    data: Dict[str, Any],
    redact_fields: FrozenSet[str] = _DEFAULT_REDACT,
) -> Dict[str, Any]:
    """Return a copy of *data* with sensitive fields replaced by ``[REDACTED]``.

    Recursively descends into nested dicts.  Lists are traversed but their
    non-dict elements are left untouched.
    """
    if not isinstance(data, dict):
        raise TypeError(f"redact_dict expects a dict, got {type(data)!r}")
    out: Dict[str, Any] = {}
    for key, value in data.items():
        if key in redact_fields:
            out[key] = "[REDACTED]"
        elif isinstance(value, dict):
            out[key] = redact_dict(value, redact_fields)
        elif isinstance(value, list):
            out[key] = _redact_list(value, redact_fields)
        else:
            out[key] = value
    return out


def _redact_list(lst: list, redact_fields: FrozenSet[str]) -> list:
    result: list[Any] = []
    for item in lst:
        if isinstance(item, dict):
            result.append(redact_dict(item, redact_fields))
        elif isinstance(item, list):
            result.append(_redact_list(item, redact_fields))
        else:
            result.append(item)
    return result


def redact_text(text: str) -> str:
    """Cap free-text length. This function does not remove secrets or fields."""
    limit = 480
    if len(text) > limit:
        return text[:limit] + " … [truncated by redact_text]"
    return text
