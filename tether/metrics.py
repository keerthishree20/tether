"""Metrics, in Prometheus text format, served from the standard library.

Depth alone cannot tell a busy queue from a stuck one. A queue holding ten
thousand tasks and draining them in seconds is healthy; a queue holding four
that nobody has touched for an hour is not. `oldest_available_age_seconds` is
the number to alert on.
"""

from __future__ import annotations

import json
import pathlib
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import db
from .config import Settings, load
from .queue import Client

DASHBOARD_PATH = pathlib.Path(__file__).with_name("dashboard.html")

_GAUGES = [
    ("tether_tasks_available", "Tasks queued and eligible to run now", "available"),
    ("tether_tasks_scheduled", "Tasks queued but not yet eligible", "scheduled"),
    ("tether_tasks_in_flight", "Tasks currently held under a lease", "in_flight"),
    ("tether_tasks_done", "Tasks acknowledged", "done"),
    ("tether_tasks_dead", "Tasks parked in the dead-letter queue", "dead"),
    ("tether_oldest_available_age_seconds",
     "Age of the oldest task waiting to be leased", "oldest_available_age_s"),
]


def render(client: Client, *, queue: str | None = None) -> str:
    stats = client.stats(queue=queue)
    label = f'{{queue="{queue}"}}' if queue else ""
    lines: list[str] = []
    for name, help_text, key in _GAUGES:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} gauge")
        value = stats[key]
        lines.append(f"{name}{label} {value:g}" if isinstance(value, float)
                     else f"{name}{label} {value}")
    return "\n".join(lines) + "\n"


def snapshot(client: Client, *, queue: str | None = None, dead_limit: int = 20) -> dict:
    """Everything the dashboard polls, in one round trip's worth of queries."""
    return {
        "now": datetime.now(timezone.utc).strftime("%H:%M:%S"),
        # A steadily increasing clock the page can difference to get a rate,
        # unaffected by the wall clock being adjusted mid-session.
        "monotonic": time.monotonic(),
        "totals": client.stats(queue=queue),
        "queues": client.stats_by_queue(),
        "dead": [
            {"id": t.id, "queue": t.queue, "kind": t.kind, "attempts": t.attempts,
             "max_attempts": t.max_attempts,
             "last_error": (t.last_error or "").splitlines()[0][:200]}
            for t in client.dead_letters(queue=queue, limit=dead_limit)
        ],
    }


class _Handler(BaseHTTPRequestHandler):
    settings: Settings
    queue: str | None = None

    def do_GET(self) -> None:  # noqa: N802 - name fixed by the base class
        route = self.path.split("?", 1)[0].rstrip("/") or "/"
        if route == "/":
            self._send(DASHBOARD_PATH.read_bytes(), "text/html; charset=utf-8")
            return
        if route not in ("/metrics", "/api/stats"):
            self.send_error(404, "served here: / (dashboard), /metrics, /api/stats")
            return
        try:
            with db.connect(self.settings) as conn:
                client = Client(conn, self.settings)
                if route == "/metrics":
                    body = render(client, queue=self.queue).encode()
                    content_type = "text/plain; version=0.0.4; charset=utf-8"
                else:
                    body = json.dumps(snapshot(client, queue=self.queue)).encode()
                    content_type = "application/json; charset=utf-8"
        except Exception as exc:  # noqa: BLE001 - a scrape must not kill the server
            self.send_error(503, f"metrics unavailable: {exc}")
            return
        self._send(body, content_type)

    def _send(self, body: bytes, content_type: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args) -> None:
        pass  # a scrape every fifteen seconds is not worth a log line


def serve(port: int = 9464, *, settings: Settings | None = None, queue: str | None = None,
          stop: threading.Event | None = None) -> None:
    handler = type("BoundHandler", (_Handler,),
                   {"settings": settings or load(), "queue": queue})
    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    if stop is None:
        server.serve_forever()
        return
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    stop.wait()
    server.shutdown()
