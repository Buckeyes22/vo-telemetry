"""Snapshot API client — read-only HTTP polling.

Uses only the Python standard library (urllib).  No authentication is
required against the current open endpoint; this layer is designed so that
an auth header can be added in one place when the endpoint is locked down.

Exposes:
  - ``fetch_current()`` → ``CurrentSnapshot``
  - ``fetch_lanes(run_id)`` → ``List[Lane]``

Both methods raise ``ApiError`` on network/parse failures so callers can
distinguish between "endpoint down" (acceptable; watcher is blind) and
unexpected errors.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from typing import List, Optional

from .config import WatcherConfig
from .models import CurrentSnapshot, Lane

logger = logging.getLogger(__name__)


class ApiError(Exception):
    """Raised when the snapshot API call fails."""

    def __init__(self, message: str, status_code: Optional[int] = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class SnapshotApiClient:
    """Read-only HTTP client for the liveness snapshot API."""

    def __init__(self, config: WatcherConfig) -> None:
        self._base = config.api_base.rstrip("/")
        self._timeout = config.api_timeout_seconds

    # ------------------------------------------------------------------ #
    # Public read-only methods
    # ------------------------------------------------------------------ #

    def fetch_current(self) -> CurrentSnapshot:
        """GET /runs/current → CurrentSnapshot."""
        raw = self._get("/runs/current")
        try:
            return CurrentSnapshot.from_dict(raw)
        except (KeyError, TypeError, ValueError) as exc:
            raise ApiError(f"Failed to parse /runs/current response: {exc}") from exc

    def fetch_lanes(self, run_id: str) -> List[Lane]:
        """GET /runs/{run_id}/lanes → List[Lane]."""
        # run_id comes from the server-returned JSON, not user input, but
        # we still sanitise it to avoid any path-traversal risk.
        safe_run_id = _safe_path_segment(run_id)
        raw = self._get(f"/runs/{safe_run_id}/lanes")
        try:
            return [Lane.from_dict(lane) for lane in (raw.get("lanes") or [])]
        except (KeyError, TypeError, ValueError) as exc:
            raise ApiError(f"Failed to parse /runs/{run_id}/lanes response: {exc}") from exc

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _get(self, path: str) -> dict:
        url = self._base + path
        req = urllib.request.Request(url, method="GET")
        try:
            # Scheme validated at config load.
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:  # nosec B310
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            raise ApiError(f"HTTP {exc.code} from {url}", status_code=exc.code) from exc
        except urllib.error.URLError as exc:
            raise ApiError(f"URL error fetching {url}: {exc.reason}") from exc
        except OSError as exc:
            raise ApiError(f"Network error fetching {url}: {exc}") from exc

        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise ApiError(f"Invalid JSON from {url}: {exc}") from exc


def _safe_path_segment(value: str) -> str:
    """Strip characters that could manipulate a URL path.

    run_id values from the server are alphanumeric + hyphens/underscores.
    Anything else is stripped.
    """
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")
    sanitised = "".join(c for c in value if c in allowed)
    if sanitised != value:
        logger.warning(
            "run_id %r contained unsafe chars; sanitised to %r.",
            value,
            sanitised,
        )
    return sanitised
