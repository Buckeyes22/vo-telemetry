"""Telegram notification stub.

The send function is a DRY-RUN STUB by default.  When ``dry_run=True`` (the
default) it logs exactly what it would send and returns without making any
network call.

When ``dry_run=False`` AND a valid bot token + chat ID are configured, it
sends via the Telegram Bot API using only the standard library (``urllib``).

Security rules applied unconditionally (dry-run AND live):
1. Structured field redaction happens in message builders before formatting.
   ``redact_text`` only caps length; it does not detect arbitrary secrets.
2. Length cap: messages are hard-truncated to ``config.telegram_max_length``
   characters.
3. No shell execution: the bot token and chat ID come from config only; no
   part of the message text is used to construct a URL segment or system call.
4. The function NEVER raises — it logs errors and returns, so a Telegram
   failure cannot crash the watcher loop.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

from .config import WatcherConfig
from .redact import redact_dict, redact_text

logger = logging.getLogger(__name__)


def send_telegram(
    message: str,
    *,
    config: WatcherConfig,
    parse_mode: str = "MarkdownV2",
) -> None:
    """Send (or log) *message* via Telegram.

    In dry-run mode this function logs what it would send and returns.
    In live mode it POSTs to the Telegram Bot API.

    Parameters
    ----------
    message:
        The pre-redacted message body. Only length limiting is applied here.
    config:
        Watcher configuration (dry_run, tokens, chat ID, length cap).
    parse_mode:
        Telegram parse mode.  ``"MarkdownV2"`` is the default.  The caller is
        responsible for escaping special chars when using MarkdownV2.
    """
    # --- Redact and cap length unconditionally ---
    safe_message = redact_text(message)
    if len(safe_message) > config.telegram_max_length:
        safe_message = safe_message[: config.telegram_max_length] + "\n…[truncated]"

    if config.dry_run:
        logger.info(
            "[DRY-RUN] Would send Telegram message to chat_id=%r:\n%s",
            config.telegram_chat_id or "(not configured)",
            safe_message,
        )
        return

    # --- Live send ---
    token = config.telegram_bot_token.strip()
    chat_id = config.telegram_chat_id.strip()

    if not token or not chat_id:
        logger.warning("Telegram send skipped: bot token or chat ID not configured.")
        return

    _send_live(safe_message, token=token, chat_id=chat_id, parse_mode=parse_mode)


# ------------------------------------------------------------------ #
# Internal helpers
# ------------------------------------------------------------------ #


def _send_live(
    message: str,
    *,
    token: str,
    chat_id: str,
    parse_mode: str,
) -> None:
    """POST *message* to the Telegram Bot API.

    Uses only the standard library.  The token is used only to construct the
    URL path component; it is never logged.  The chat_id is treated as an
    opaque string and URL-encoded.
    """
    # Validate that token and chat_id contain no URL-unsafe structural chars
    # that could alter the request meaning.  A real token is "<digits>:<alnum>".
    if "/" in token or "\n" in token or "\r" in token:
        logger.error("Telegram token contains unsafe characters; send aborted.")
        return
    if "\n" in chat_id or "\r" in chat_id:
        logger.error("Telegram chat_id contains unsafe characters; send aborted.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps(
        {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": parse_mode,
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        # Fixed https://api.telegram.org URL.
        with urllib.request.urlopen(req, timeout=15) as resp:  # nosec B310
            if resp.status != 200:
                logger.warning("Telegram API returned HTTP %d for sendMessage.", resp.status)
            else:
                logger.debug("Telegram message delivered.")
    except urllib.error.HTTPError as exc:
        logger.error("Telegram HTTPError %d: %s", exc.code, exc.reason)
    except urllib.error.URLError as exc:
        logger.error("Telegram URLError: %s", exc.reason)
    except Exception as exc:  # noqa: BLE001
        logger.error("Telegram send failed unexpectedly: %s", exc)


def escape_markdown_v2(text: str) -> str:
    """Escape special characters for Telegram MarkdownV2.

    Telegram MarkdownV2 requires escaping: _ * [ ] ( ) ~ ` > # + - = | { } . !
    """
    special = r"\_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{c}" if c in special else c for c in text)


def format_run_summary(
    *,
    run_id: str,
    project: Optional[str],
    state: str,
    lane_summaries: list,
    total_cost_usd: float,
    dry_run: bool,
    config: WatcherConfig | None = None,
) -> str:
    """Produce a human-readable end-of-run Telegram summary.

    ``lane_summaries`` is a list of dicts with keys:
        agent_id, declared_outcome, summary (may be None), failed_children
    Configured sensitive fields are replaced before formatting. Length limiting
    is separate and is not a general secret scrubber.
    """
    fields = redact_dict(
        {"run_id": run_id, "project": project, "state": state, "lane_summaries": lane_summaries},
        (config or WatcherConfig()).redact_fields,
    )
    run_id = fields["run_id"]
    project = fields["project"]
    state = fields["state"]
    lane_summaries = fields["lane_summaries"]
    prefix = "[DRY-RUN] " if dry_run else ""
    lines = [
        f"{prefix}*Run closed*",
        f"run\\_id: `{escape_markdown_v2(run_id)}`",
        f"project: {escape_markdown_v2(project or 'unknown')}",
        f"state: {escape_markdown_v2(state)}",
        "",
        "*Lanes:*",
    ]
    for ls in lane_summaries:
        agent = escape_markdown_v2(ls.get("agent_id", "?"))
        outcome = escape_markdown_v2(ls.get("declared_outcome") or "unknown")
        raw_summary = ls.get("summary") or ""
        safe_summary = escape_markdown_v2(redact_text(raw_summary)) if raw_summary else ""
        failed = ls.get("failed_children", 0)
        line = f"  • `{agent}` → {outcome}"
        if failed:
            line += f" \\({failed} failed tool\\)"
        if safe_summary:
            line += f"\n    _{safe_summary}_"
        lines.append(line)

    lines += [
        "",
        f"cost: ${total_cost_usd:.4f}",
    ]
    return "\n".join(lines)
