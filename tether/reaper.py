"""The reaper.

Sweeps for leases that stopped being renewed and returns their tasks to the
queue, or parks them if the attempt budget is spent. Also keeps the starvation
boost current so low-priority work eventually runs.

Run it as its own process (`tether reap`) or inside a worker (`--with-reaper`).
Several may run at once: the sweep takes `FOR UPDATE SKIP LOCKED` on the rows it
claims, so two reapers divide the work instead of fighting over it.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import Counter
from typing import Callable

import psycopg

from . import db
from .config import Settings, load
from .queue import Client

log = logging.getLogger("tether.reaper")


class Reaper:
    def __init__(
        self,
        settings: Settings | None = None,
        *,
        stop: threading.Event | None = None,
        counters: Counter | None = None,
        batch: int = 500,
        on_event: Callable[[str, dict], None] | None = None,
    ):
        self.settings = settings or load()
        self.stop = stop or threading.Event()
        self.counters = counters if counters is not None else Counter()
        self.batch = batch
        self.on_event = on_event

    def sweep_once(self, client: Client) -> dict[str, int]:
        # One transaction per sweep: the rows claimed by FOR UPDATE stay locked
        # until the reclaim is written, so a second reaper cannot double-handle
        # them.
        with client.conn.transaction():
            result = client.reclaim_expired(batch=self.batch)
            result["boosted"] = client.apply_starvation_boost()
        self.counters["redelivered"] += result["redelivered"]
        self.counters["reaped_dead"] += result["dead"]
        if result["redelivered"] or result["dead"]:
            log.info("reclaimed %s, parked %s", result["redelivered"], result["dead"])
            if self.on_event:
                self.on_event("reaped", result)
        return result

    def run(self) -> Counter:
        conn = db.wait_until_ready(self.settings)
        client = Client(conn, self.settings)
        interval = self.settings.reap_interval_ms / 1000.0
        while not self.stop.is_set():
            try:
                self.sweep_once(client)
            except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
                log.warning("database unavailable, retrying: %s", str(exc).strip())
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass
                conn = db.wait_until_ready(self.settings, timeout_s=60.0)
                client = Client(conn, self.settings)
                continue
            self.stop.wait(interval)
        conn.close()
        return self.counters


def run_forever(settings: Settings | None = None) -> None:
    Reaper(settings).run()


def sweep(settings: Settings | None = None) -> dict[str, int]:
    """A single sweep, for tests and for `tether reap --once`."""
    settings = settings or load()
    with db.connect(settings) as conn:
        return Reaper(settings).sweep_once(Client(conn, settings))


def wait_for_redelivery(client: Client, task_id: int, *, timeout_s: float = 30.0,
                        settings: Settings | None = None) -> bool:
    """Sweep until the given task is back on the queue. Used by the chaos tests."""
    reaper = Reaper(settings or client.settings)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        reaper.sweep_once(client)
        task = client.get(task_id)
        if task and task.state in ("queued", "dead"):
            return True
        time.sleep(0.05)
    return False
