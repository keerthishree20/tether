"""Milestone 5: delays, priority, starvation, and the concurrency cap."""

from __future__ import annotations

import threading
import time

from tether import db
from tether.queue import Client


def test_a_delayed_task_waits_for_its_time(client):
    task = client.enqueue("echo", delay_ms=400)
    assert client.lease("worker-a") == []
    client.conn.execute("SELECT pg_sleep(0.5)")
    leased = client.lease("worker-a")
    assert [t.id for t in leased] == [task.id]


def test_higher_priority_goes_first(client):
    low = client.enqueue("echo", priority=0)
    high = client.enqueue("echo", priority=5)
    middle = client.enqueue("echo", priority=2)

    order = [client.lease("worker-a")[0].id for _ in range(3)]
    assert order == [high.id, middle.id, low.id]


def test_equal_priority_is_served_oldest_first(client):
    first = client.enqueue("echo")
    client.conn.execute("SELECT pg_sleep(0.02)")
    second = client.enqueue("echo")
    order = [client.lease("worker-a")[0].id for _ in range(2)]
    assert order == [first.id, second.id]


def test_a_starved_task_is_boosted_until_it_runs(client, settings):
    """Priority alone starves low-priority work forever under sustained load.
    The boost is what bounds the wait."""
    low = client.enqueue("echo", priority=0)
    client.conn.execute("SELECT pg_sleep(%s)", (settings.boost_after_ms / 1000.0 + 0.1,))

    assert client.apply_starvation_boost() == 1
    boosted = client.get(low.id)
    assert boosted.boost >= 1

    # A newly arrived higher-priority task no longer automatically wins.
    client.enqueue("echo", priority=1)
    assert client.lease("worker-a")[0].id == low.id


def test_the_boost_is_capped(client, settings):
    client.enqueue("echo")
    client.conn.execute(
        "UPDATE tasks SET available_at = now() - interval '1 hour'")
    client.apply_starvation_boost()
    assert client.get(1).boost <= settings.boost_max


def test_the_boost_resets_once_the_task_is_delivered(client, settings):
    task = client.enqueue("echo")
    client.conn.execute("UPDATE tasks SET available_at = now() - interval '1 hour'")
    client.apply_starvation_boost()
    assert client.get(task.id).boost > 0
    (leased,) = client.lease("worker-a")
    assert leased.boost == 0


def test_the_concurrency_cap_is_enforced_inside_the_lease(client):
    for _ in range(10):
        client.enqueue("sleep", {"seconds": 5})

    first = client.lease("worker-a", batch=10, concurrency_cap=3)
    assert len(first) == 3
    assert client.lease("worker-b", batch=10, concurrency_cap=3) == []

    client.ack(first[0], "worker-a")
    assert len(client.lease("worker-b", batch=10, concurrency_cap=3)) == 1


def test_the_cap_is_per_queue(client):
    for queue in ("alpha", "beta"):
        for _ in range(4):
            client.enqueue("echo", queue=queue)

    assert len(client.lease("worker-a", queue="alpha", batch=4, concurrency_cap=2)) == 2
    assert len(client.lease("worker-a", queue="beta", batch=4, concurrency_cap=2)) == 2


def test_a_capped_queue_never_holds_a_lease_it_cannot_run(base_settings, conn, settings):
    """The deadlock this design avoids: leasing first and checking the cap
    afterwards would leave tasks pinned to a worker that refuses to start them."""
    seed = Client(conn, settings)
    for _ in range(20):
        seed.enqueue("echo")

    stop = threading.Event()
    peak = 0
    lock = threading.Lock()

    def contend(n: int) -> None:
        nonlocal peak
        own = db.connect(base_settings)
        try:
            client = Client(own, settings)
            while not stop.is_set():
                tasks = client.lease(f"w{n}", batch=5, concurrency_cap=4)
                if not tasks:
                    time.sleep(0.01)
                    continue
                with lock:
                    peak = max(peak, client.stats()["in_flight"])
                time.sleep(0.02)
                for task in tasks:
                    client.ack(task, f"w{n}")
        finally:
            own.close()

    threads = [threading.Thread(target=contend, args=(n,)) for n in range(6)]
    for t in threads:
        t.start()
    time.sleep(1.5)
    stop.set()
    for t in threads:
        t.join(timeout=15)

    assert peak <= 4, f"in-flight reached {peak}, above the cap of 4"
    assert seed.stats()["done"] == 20, "capped queue failed to drain"
