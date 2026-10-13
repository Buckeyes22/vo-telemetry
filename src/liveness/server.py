from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from .app import LivenessBackend
from .config import config_from_env


def make_server(address: tuple[str, int], backend: LivenessBackend) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        server_version = "vo-telemetry-liveness/1.0"

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path != "/ingest":
                self._json(404, {"error": "not found"})
                return
            content_length = self.headers.get("Content-Length")
            if content_length is None:
                self._json(411, {"error": "Content-Length is required"})
                return
            try:
                length = int(content_length)
            except ValueError:
                self._json(400, {"error": "Content-Length must be an integer"})
                return
            if length > backend.config.max_body_bytes:
                self._json(413, {"error": f"request body exceeds {backend.config.max_body_bytes} bytes"})
                return
            body = self.rfile.read(length)
            status, payload = backend.ingest(body, self.headers.get("Authorization"))
            self._json(status, payload)

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)
            if path == "/healthz":
                self._json(*backend.healthz())
                return
            if path == "/readyz":
                self._json(*backend.readyz())
                return
            if path == "/metrics":
                self._text(200, backend.metrics_text(), "text/plain; version=0.0.4; charset=utf-8")
                return
            if path == "/runs/current":
                self._json(*backend.runs_current())
                return
            if path == "/lanes/current":
                self._json(*backend.lanes_current())
                return
            if path.startswith("/runs/"):
                self._handle_runs_path(path, query)
                return
            self._json(404, {"error": "not found"})

        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def _handle_runs_path(self, path: str, query: dict[str, list[str]]) -> None:
            parts = [unquote(part) for part in path.strip("/").split("/")]
            if len(parts) == 2:
                self._json(*backend.run_snapshot(parts[1]))
                return
            if len(parts) == 3 and parts[2] == "lanes":
                since = _int_query(query, "since_snapshot_version", 0)
                self._json(*backend.lanes(parts[1], since))
                return
            if len(parts) == 3 and parts[2] == "events":
                since = _int_query(query, "since_ingest_seq", 0)
                limit = _optional_int_query(query, "limit")
                agent_id = query.get("agent_id", [None])[0]
                self._json(*backend.events(parts[1], agent_id, since, limit))
                return
            self._json(404, {"error": "not found"})

        def _json(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _text(self, status: int, body_text: str, content_type: str) -> None:
            body = body_text.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return ThreadingHTTPServer(address, Handler)


def main() -> None:
    config = config_from_env()
    backend = LivenessBackend(config)
    server = make_server((config.bind_host, config.bind_port), backend)
    try:
        server.serve_forever()
    finally:
        server.server_close()


def _int_query(query: dict[str, list[str]], name: str, default: int) -> int:
    value = query.get(name, [str(default)])[0]
    try:
        return int(value)
    except ValueError:
        return default


def _optional_int_query(query: dict[str, list[str]], name: str) -> int | None:
    if name not in query:
        return None
    try:
        return int(query[name][0])
    except ValueError:
        return None


if __name__ == "__main__":
    main()
