"""Milestone 1: a task reaches exactly one worker, and only one."""

from __future__ import annotations

import threading
from collections import Counter

from tether import db
from tether.queue import Client


def test_enqueue_starts_queued_and_available(client):
    task = client.enqueue("echo", {"n": 1})
    assert task.state == "queued"
    assert task.attempts == 0
    assert task.leased_by is None
    assert client.stats()["available"] == 1


def test_lease_marks_the_task_and_counts_the_delivery(client):
    client.enqueue("echo")
    (task,) = client.lease("worker-a")
    assert task.state == "leased"
    assert task.leased_by == "worker-a"
    assert task.attempts == 1
    assert task.leased_until is not None


def test_a_leased_task_is_invisible_to_everyone_else(client, second_client):
    client.enqueue("echo")
    assert len(client.lease("worker-a")) == 1
    assert second_client.lease("worker-b") == []


def test_lease_respects_the_batch_size(client):
    for _ in range(5):
        client.enqueue("echo")
    assert len(client.lease("worker-a", batch=3)) == 3
    assert len(client.lease("worker-a", batch=3)) == 2


def test_scheduled_tasks_are_not_leasable_yet(client):
    client.enqueue("echo", delay_ms=60_000)
    assert client.lease("worker-a") == []
    stats = client.stats()
    assert stats["scheduled"] == 1
    assert stats["available"] == 0


def test_ack_completes_the_task(client):
    client.enqueue("echo")
    (task,) = client.lease("worker-a")
    assert client.ack(task, "worker-a") is True
    done = client.get(task.id)
    assert done.state == "done"
    assert done.completed_at is not None
    assert done.leased_by is None


def test_ack_from_a_different_worker_is_refused(client):
    client.enqueue("echo")
    (task,) = client.lease("worker-a")
    assert client.ack(task, "worker-b") is False
    assert client.get(task.id).state == "leased"


def test_ack_with_a_stale_fence_is_refused(client, settings):
    """The scenario the fence exists for: a worker is reaped, the task is
    redelivered, and the original worker finally comes back to acknowledge."""
    client.enqueue("echo", lease_ttl_ms=1)
    (first,) = client.lease("worker-a", lease_ttl_ms=1)
    client.conn.execute("SELECT pg_sleep(0.05)")

    with client.conn.transaction():
        client.reclaim_expired()
    # Step past the retry backoff rather than sleeping through it.
    client.conn.execute("UPDATE tasks SET available_at = now() WHERE id = %s", (first.id,))
    (second,) = client.lease("worker-b")

    assert second.attempts == first.attempts + 1
    assert client.ack(first, "worker-a") is False, "a stale delivery acknowledged someone else's work"
    assert client.ack(second, "worker-b") is True


def test_twenty_workers_deliver_two_hundred_tasks_exactly_once(base_settings, conn, settings):
    """The `SKIP LOCKED` claim. No task is delivered twice and no worker blocks
    behind another's row lock."""
    seed = Client(conn, settings)
    total = 200
    for i in range(total):
        seed.enqueue("echo", {"i": i})

    delivered: Counter[int] = Counter()
    lock = threading.Lock()
    barrier = threading.Barrier(20)

    def drain(worker_no: int) -> None:
        own = db.connect(base_settings)
        try:
            client = Client(own, settings)
            barrier.wait(timeout=15)
            while True:
                tasks = client.lease(f"worker-{worker_no}", batch=3)
                if not tasks:
                    break
                with lock:
                    for task in tasks:
                        delivered[task.id] += 1
                for task in tasks:
                    client.ack(task, f"worker-{worker_no}")
        finally:
            own.close()

    threads = [threading.Thread(target=drain, args=(n,)) for n in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert sum(delivered.values()) == total
    assert set(delivered.values()) == {1}, "a task was handed to more than one worker"
    assert seed.stats()["done"] == total
    assert seed.check_invariants() == []
