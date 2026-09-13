"""Milestone 7: the failures the queue exists to survive.

Every test here breaks something real. A green suite that never kills a process
proves only that the happy path works, and the happy path was never in doubt.
"""

from __future__ import annotations

import os
import pathlib
import re
import signal
import subprocess
import time

import pytest

from tether import db
from tether.queue import Client
from tether.reaper import Reaper
from tether.worker import Worker

from .conftest import requires_docker, wait_for

CONTAINER = os.environ.get("TETHER_PG_CONTAINER", "tether-pg")


def _reap_until(client: Client, predicate, *, timeout_s: float = 20.0):
    """Sweep on a loop until the predicate holds. The reaper is what makes
    every recovery in this file happen."""
    reaper = Reaper(client.settings)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        reaper.sweep_once(client)
        result = predicate()
        if result:
            return result
        time.sleep(0.05)
    pytest.fail(f"reaper did not reach the expected state within {timeout_s:g}s")


# ------------------------------------------------------------- worker killed


def test_a_killed_worker_has_its_task_redelivered(client, spawn_worker):
    """SIGKILL. No shutdown hook runs, nothing is released, no message is sent.
    The queue finds out only because a lease stopped being renewed."""
    task = client.enqueue("sleep", {"seconds": 30}, max_attempts=3)
    proc = spawn_worker("--concurrency", "1")

    wait_for(lambda: client.stats()["in_flight"] == 1, what="the worker to take the task")
    assert client.get(task.id).attempts == 1

    proc.kill()
    proc.wait(timeout=10)

    _reap_until(client, lambda: client.get(task.id).state == "queued")
    reclaimed = client.get(task.id)
    assert reclaimed.attempts == 1, "the kill must not consume more than the one delivery"
    assert reclaimed.leased_by is None
    assert "lease expired" in reclaimed.last_error

    # A second worker finishes the job.
    client.conn.execute(
        """UPDATE tasks SET payload = '{"seconds": 0.05}'::jsonb, available_at = now()
           WHERE id = %s""", (task.id,))
    Worker(settings=client.settings, concurrency=1, idle_exit_after=1.0,
           worker_id="survivor").run()

    finished = client.get(task.id)
    assert finished.state == "done"
    assert finished.attempts == 2
    assert client.check_invariants() == []


def test_a_frozen_worker_loses_its_lease_while_still_alive(client, spawn_worker):
    """SIGSTOP freezes every thread, heartbeats included. The process is up and
    the task is still gone, which is the correct outcome: holding a lease should
    require making progress, not merely existing."""
    task = client.enqueue("sleep", {"seconds": 30}, max_attempts=3)
    proc = spawn_worker("--concurrency", "1")
    wait_for(lambda: client.stats()["in_flight"] == 1, what="the worker to take the task")

    proc.send_signal(signal.SIGSTOP)
    try:
        _reap_until(client, lambda: client.get(task.id).state == "queued")
        assert proc.poll() is None, "the process should still be alive, merely frozen"
    finally:
        proc.send_signal(signal.SIGCONT)
        proc.kill()
        proc.wait(timeout=10)


def test_a_terminated_worker_finishes_what_it_started(client, spawn_worker):
    """SIGTERM is the opposite of SIGKILL: stop taking new work, finish the
    task in hand, exit. Nothing needs redelivering."""
    task = client.enqueue("sleep", {"seconds": 1.0}, max_attempts=3)
    proc = spawn_worker("--concurrency", "1", TETHER_LEASE_TTL_MS=30_000,
                        TETHER_HEARTBEAT_MS=500)
    wait_for(lambda: client.stats()["in_flight"] == 1, what="the worker to take the task")

    proc.terminate()
    proc.wait(timeout=20)

    finished = client.get(task.id)
    assert finished.state == "done"
    assert finished.attempts == 1


# ------------------------------------------------------------ database restart


@requires_docker
def test_work_survives_a_database_restart(base_settings, conn, settings, spawn_worker):
    """The connection every worker holds is torn out from under it mid-run."""
    client = Client(conn, settings)
    for _ in range(40):
        client.enqueue("sleep", {"seconds": 0.05})

    proc = spawn_worker("--concurrency", "4", "--with-reaper")
    wait_for(lambda: client.stats()["done"] >= 5, what="the worker to make progress")

    subprocess.run(["docker", "restart", CONTAINER], check=True, capture_output=True, timeout=120)

    # Our own connection died with the server; the workers reconnect on their own.
    conn.close()
    fresh = db.wait_until_ready(base_settings, timeout_s=90)
    checker = Client(fresh, settings)

    wait_for(lambda: checker.stats()["done"] == 40, timeout_s=90,
             what="every task to complete after the restart")
    assert proc.poll() is None, "the worker should have reconnected, not exited"
    assert checker.stats()["dead"] == 0
    assert checker.check_invariants() == []
    fresh.close()


# ------------------------------------------------------------------- the clock


def test_lease_expiry_is_decided_by_the_database_clock(client):
    """Workers disagree about the time. The row is the arbiter."""
    client.enqueue("sleep", lease_ttl_ms=1000)
    (task,) = client.lease("worker-a", lease_ttl_ms=1000)

    client.conn.execute(
        "UPDATE tasks SET leased_until = now() + interval '1 hour' WHERE id = %s", (task.id,))
    with client.conn.transaction():
        assert client.reclaim_expired()["redelivered"] == 0

    client.conn.execute(
        "UPDATE tasks SET leased_until = now() - interval '1 second' WHERE id = %s", (task.id,))
    with client.conn.transaction():
        assert client.reclaim_expired()["redelivered"] == 1


def test_no_lease_timestamp_is_ever_written_from_a_worker_clock():
    """A structural guard on the rule above.

    Every lease deadline in the queue is `now() + interval` evaluated by
    Postgres. If someone later reaches for the local clock to set one, this
    catches it before a skewed worker can steal a live lease.
    """
    source = pathlib.Path(__file__).resolve().parents[1] / "tether" / "queue.py"
    text = source.read_text()
    offenders = re.findall(r"(datetime\.now|datetime\.utcnow|time\.time)\s*\(", text)
    assert offenders == [], f"local clock used in the queue core: {offenders}"

    for column in ("available_at", "leased_until"):
        for assignment in re.findall(rf"{column}\s*=\s*([^,\n]+)", text):
            if "%" in assignment and "now()" not in assignment:
                pytest.fail(f"{column} assigned from a parameter rather than the database clock: "
                            f"{assignment.strip()}")


# --------------------------------------------------------------- backpressure


def test_a_producer_outrunning_workers_is_visible_before_users_notice(client):
    """Nothing here is broken. The point is that the gauges say so early."""
    for _ in range(200):
        client.enqueue("echo")
    client.conn.execute("SELECT pg_sleep(0.4)")

    backed_up = client.stats()
    assert backed_up["available"] == 200
    assert backed_up["oldest_available_age_s"] >= 0.4

    for _ in range(200):
        leased = client.lease("worker-a", batch=50)
        if not leased:
            break
        for task in leased:
            client.ack(task, "worker-a")

    drained = client.stats()
    assert drained["available"] == 0
    assert drained["done"] == 200
    assert drained["oldest_available_age_s"] == 0.0


def test_the_queue_never_leaves_a_task_in_two_states(client, spawn_worker):
    """The invariant checker, run against a queue that is being actively
    disrupted rather than a quiet one."""
    for _ in range(30):
        client.enqueue("flaky", {"failure_rate": 0.5}, max_attempts=4)

    proc = spawn_worker("--concurrency", "4", "--with-reaper")
    deadline = time.monotonic() + 6
    while time.monotonic() < deadline:
        assert client.check_invariants() == []
        time.sleep(0.1)

    proc.kill()
    proc.wait(timeout=10)
    _reap_until(client, lambda: client.stats()["in_flight"] == 0)
    assert client.check_invariants() == []

    stats = client.stats()
    assert stats["done"] + stats["dead"] + stats["available"] + stats["scheduled"] == 30
