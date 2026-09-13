"""The worker pool.

One thread per slot, one connection per thread, plus a heartbeat thread that
renews the leases of everything currently in flight.

The heartbeat lives in its own thread on purpose. If the process is merely busy,
leases keep being renewed and long tasks are safe. If the process is wedged, that
thread stops too, the lease lapses, and the reaper takes the work back. A worker
being alive is not the same as a worker making progress, and only the second one
should hold a lease.
"""

from __future__ import annotations

import logging
import os
import signal
import socket
import threading
import time
import traceback
from collections import Counter
from typing import Callable

import psycopg

from . import db, handlers
from .config import Settings, load
from .queue import Client, Task

log = logging.getLogger("tether.worker")


class LeaseLost(RuntimeError):
    """The lease was reclaimed before this delivery could be acknowledged."""


def default_worker_id() -> str:
    return f"{socket.gethostname()}/{os.getpid()}"


class Worker:
    def __init__(
        self,
        *,
        queue: str = "default",
        concurrency: int = 4,
        settings: Settings | None = None,
        worker_id: str | None = None,
        batch: int = 1,
        concurrency_cap: int | None = None,
        run_reaper: bool = False,
        idle_exit_after: float | None = None,
        on_event: Callable[[str, dict], None] | None = None,
    ):
        self.settings = settings or load()
        self.queue = queue
        self.concurrency = concurrency
        self.worker_id = worker_id or default_worker_id()
        self.batch = batch
        self.concurrency_cap = concurrency_cap
        self.run_reaper = run_reaper
        #: Exit once the queue has been empty this many seconds. Benchmarks and
        #: tests use it; a long-running worker leaves it None.
        self.idle_exit_after = idle_exit_after
        self.on_event = on_event

        self.counters: Counter[str] = Counter()
        self._stop = threading.Event()
        self._inflight: dict[int, tuple[int, int]] = {}
        self._lock = threading.Lock()
        self._threads: list[threading.Thread] = []

    # ------------------------------------------------------------------ control

    def stop(self) -> None:
        """Ask every thread to finish its current task and exit."""
        self._stop.set()

    def install_signal_handlers(self) -> None:
        def handle(signum, _frame):
            log.info("received %s, draining", signal.Signals(signum).name)
            self.stop()
        signal.signal(signal.SIGTERM, handle)
        signal.signal(signal.SIGINT, handle)

    def run(self) -> Counter:
        self._threads = [
            threading.Thread(target=self._executor, args=(i,), name=f"exec-{i}", daemon=True)
            for i in range(self.concurrency)
        ]
        self._threads.append(threading.Thread(target=self._heartbeat, name="heartbeat", daemon=True))
        if self.run_reaper:
            self._threads.append(threading.Thread(target=self._reaper, name="reaper", daemon=True))

        for t in self._threads:
            t.start()
        for t in self._threads:
            t.join()
        return self.counters

    # ---------------------------------------------------------------- executors

    def _executor(self, slot: int) -> None:
        conn = db.wait_until_ready(self.settings)
        client = Client(conn, self.settings)
        idle_since: float | None = None

        while not self._stop.is_set():
            try:
                tasks = client.lease(
                    self.worker_id,
                    queue=self.queue,
                    batch=self.batch,
                    concurrency_cap=self.concurrency_cap,
                )
            except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
                self._emit("db_lost", {"slot": slot, "error": str(exc).strip()})
                self.counters["reconnects"] += 1
                conn, client = self._reconnect(conn)
                continue

            self.counters["lease_calls"] += 1
            if not tasks:
                # A lease that returns nothing while work is still waiting is a
                # race lost to another worker. The benchmark reports this ratio.
                self.counters["empty_leases"] += 1
                if self.idle_exit_after is not None:
                    idle_since = idle_since or time.monotonic()
                    if time.monotonic() - idle_since >= self.idle_exit_after:
                        self._stop.set()
                        break
                self._stop.wait(self.settings.poll_interval_ms / 1000.0)
                continue

            idle_since = None
            self.counters["leased"] += len(tasks)
            for task in tasks:
                conn, client = self._execute(conn, client, task)

        self._close(conn)

    def _execute(self, conn: psycopg.Connection, client: Client, task: Task):
        self._track(task)
        try:
            handler = handlers.get(task.kind)
            # One transaction covers the handler's writes, its idempotency claim
            # and the acknowledgement. A lost lease rolls the effect back with it.
            with conn.transaction():
                handler(handlers.Context(task=task, conn=conn, client=client,
                                         worker_id=self.worker_id))
                if not client.ack(task, self.worker_id):
                    raise LeaseLost(f"task {task.id} attempt {task.attempts}")
            self.counters["acked"] += 1
            self._emit("acked", {"task_id": task.id, "attempt": task.attempts})
        except LeaseLost:
            self.counters["lease_lost"] += 1
            self._emit("lease_lost", {"task_id": task.id, "attempt": task.attempts})
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            # The database went away mid-task. Do not try to record a failure on
            # a dead connection; the lease will simply expire and the reaper will
            # redeliver, which is exactly the guarantee we advertise.
            self.counters["reconnects"] += 1
            self._emit("db_lost", {"task_id": task.id, "error": str(exc).strip()})
            conn, client = self._reconnect(conn)
        except BaseException as exc:  # noqa: BLE001 - handler failures are data
            detail = "".join(traceback.format_exception_only(type(exc), exc)).strip()
            try:
                state = client.fail(task, self.worker_id, detail)
            except (psycopg.OperationalError, psycopg.InterfaceError):
                self.counters["reconnects"] += 1
                conn, client = self._reconnect(conn)
                state = None
            if state == "dead":
                self.counters["dead"] += 1
            elif state == "queued":
                self.counters["failed"] += 1
            else:
                self.counters["lease_lost"] += 1
            self._emit("failed", {"task_id": task.id, "attempt": task.attempts,
                                  "state": state, "error": detail})
        finally:
            self._untrack(task)
        return conn, client

    # ---------------------------------------------------------------- heartbeat

    def _heartbeat(self) -> None:
        conn = db.wait_until_ready(self.settings)
        client = Client(conn, self.settings)
        interval = self.settings.heartbeat_ms / 1000.0
        while not self._stop.wait(interval):
            with self._lock:
                fences = list(self._inflight.values())
            if not fences:
                continue
            try:
                renewed = client.heartbeat(self.worker_id, fences)
            except (psycopg.OperationalError, psycopg.InterfaceError):
                conn, client = self._reconnect(conn)
                continue
            self.counters["heartbeats"] += renewed
            if renewed < len(fences):
                # Some lease we thought we held is gone. The executor will find
                # out when its acknowledgement is rejected.
                self._emit("lease_expired_under_us", {"held": len(fences), "renewed": renewed})
        self._close(conn)

    # ------------------------------------------------------------------- reaper

    def _reaper(self) -> None:
        from .reaper import Reaper
        Reaper(self.settings, stop=self._stop, counters=self.counters, on_event=self.on_event).run()

    # ------------------------------------------------------------------ plumbing

    def _track(self, task: Task) -> None:
        with self._lock:
            self._inflight[task.id] = task.fence

    def _untrack(self, task: Task) -> None:
        with self._lock:
            self._inflight.pop(task.id, None)

    def _reconnect(self, old: psycopg.Connection):
        self._close(old)
        conn = db.wait_until_ready(self.settings, timeout_s=60.0)
        return conn, Client(conn, self.settings)

    @staticmethod
    def _close(conn: psycopg.Connection | None) -> None:
        try:
            if conn is not None:
                conn.close()
        except Exception:  # noqa: BLE001 - closing a dead socket is not news
            pass

    def _emit(self, event: str, fields: dict) -> None:
        if self.on_event:
            self.on_event(event, fields)
        log.debug("%s %s", event, fields)
