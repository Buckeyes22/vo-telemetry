from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from .outbox import Outbox, OutboxRow
from .util import ensure_private_dir, utc_now


DEFAULT_INGEST_URL = "http://127.0.0.1:8723/ingest"


@dataclass(frozen=True)
class ForwarderConfig:
    ingest_url: str
    ingest_token: str
    poll_interval: float = 0.5
    initial_backoff: float = 1.0
    max_backoff: float = 60.0
    request_timeout: float = 5.0
    max_events: int = 10_000

    def __post_init__(self) -> None:
        parts = urlsplit(self.ingest_url)
        if parts.scheme not in ("http", "https") or not parts.netloc:
            raise ValueError(f"LIVENESS_INGEST_URL must be an http(s) URL, got {self.ingest_url!r}")

    @classmethod
    def from_env(cls) -> "ForwarderConfig":
        load_env_file(
            Path(
                os.getenv(
                    "CC_HOST_LIVENESS_ENV_FILE",
                    str(Path("~/.claude/telemetry/liveness.env").expanduser()),
                )
            )
        )
        return cls(
            ingest_url=os.getenv("LIVENESS_INGEST_URL", DEFAULT_INGEST_URL),
            ingest_token=os.getenv("LIVENESS_INGEST_TOKEN", ""),
            poll_interval=float(os.getenv("CC_HOST_FORWARDER_POLL_SEC", "0.5")),
            initial_backoff=float(os.getenv("CC_HOST_FORWARDER_INITIAL_BACKOFF_SEC", "1.0")),
            max_backoff=float(os.getenv("CC_HOST_FORWARDER_MAX_BACKOFF_SEC", "60.0")),
            request_timeout=float(os.getenv("CC_HOST_FORWARDER_REQUEST_TIMEOUT_SEC", "5.0")),
            max_events=int(os.getenv("CC_HOST_OUTBOX_MAX_EVENTS", "10000")),
        )


class Forwarder:
    def __init__(self, config: ForwarderConfig):
        if not config.ingest_token:
            raise RuntimeError("LIVENESS_INGEST_TOKEN is required")
        self.config = config
        self.outbox = Outbox(max_events=config.max_events)

    def run_once(self) -> bool:
        self.outbox.apply_priority_drop()
        row = self.outbox.next_sendable(now=utc_now())
        if row is None:
            return False
        ok, error = self._send(row)
        if ok:
            self.outbox.mark_sent(row.id)
            self.outbox.enqueue_drop_reports(now=utc_now())
        else:
            self.outbox.mark_failed(
                row.id,
                error or "send failed",
                initial_delay=self.config.initial_backoff,
                max_delay=self.config.max_backoff,
            )
        return True

    def run_forever(self) -> None:
        while True:
            did_work = self.run_once()
            if not did_work:
                time.sleep(self.config.poll_interval)

    def _send(self, row: OutboxRow) -> tuple[bool, str | None]:
        body = json.dumps(row.event, sort_keys=True).encode("utf-8")
        request = urllib.request.Request(
            self.config.ingest_url,
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {self.config.ingest_token}",
                "Content-Type": "application/json",
            },
        )
        try:
            # Scheme validated at config load.
            with urllib.request.urlopen(request, timeout=self.config.request_timeout) as response:  # nosec B310
                if response.status in {200, 202}:
                    return True, None
                return False, f"HTTP {response.status}"
        except urllib.error.HTTPError as exc:
            if exc.code in {200, 202}:
                return True, None
            return False, f"HTTP {exc.code}: {exc.read(1024).decode('utf-8', errors='replace')}"
        except Exception as exc:
            return False, str(exc)


def load_env_file(path: Path) -> None:
    path = path.expanduser()
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip().removeprefix("export ").strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="vo-telemetry CC-host liveness outbox forwarder")
    parser.add_argument("--once", action="store_true", help="drain at most one sendable event and exit")
    args = parser.parse_args(argv)
    ensure_private_dir(Path("~/.claude/telemetry/logs").expanduser())
    forwarder = Forwarder(ForwarderConfig.from_env())
    if args.once:
        forwarder.run_once()
        return 0
    forwarder.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
