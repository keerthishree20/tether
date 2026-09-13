"""Milestone 2: leases expire, work comes back, and it comes back once."""

from __future__ import annotations

import time


def _sleep_on_the_database(client, seconds: float) -> None:
    """Wait using the database clock, which is the clock leases are judged by."""
    client.conn.execute("SELECT pg_sleep(%s)", (seconds,))


def test_fail_returns_the_task_with_a_backoff(client):
    client.enqueue("flaky", max_attempts=5)
    (task,) = client.lease("worker-a")
    assert client.fail(task, "worker-a", "downstream refused") == "queued"

    returned = client.get(task.id)
    assert returned.state == "queued"
    assert returned.last_error == "downstream refused"
    assert returned.leased_by is None
    # It is not immediately available; the backoff pushed it into the future or
    # left it at the boundary.
    assert client.stats()["available"] + client.stats()["scheduled"] == 1


def test_the_last_attempt_goes_to_the_dead_letter_queue(client):
    client.enqueue("always_fails", max_attempts=1)
    (task,) = client.lease("worker-a")
    assert client.fail(task, "worker-a", "boom") == "dead"

    dead = client.get(task.id)
    assert dead.state == "dead"
    assert dead.dead_at is not None
    assert dead.last_error == "boom"


def test_an_expired_lease_is_reclaimed(client):
    client.enqueue("sleep", lease_ttl_ms=50)
    (task,) = client.lease("worker-a", lease_ttl_ms=50)
    _sleep_on_the_database(client, 0.1)

    with client.conn.transaction():
        result = client.reclaim_expired()
    assert result == {"redelivered": 1, "dead": 0}

    back = client.get(task.id)
    assert back.state == "queued"
    assert back.leased_by is None
    assert "lease expired" in back.last_error


def test_a_live_lease_is_left_alone(client):
    client.enqueue("sleep", lease_ttl_ms=30_000)
    client.lease("worker-a", lease_ttl_ms=30_000)
    with client.conn.transaction():
        assert client.reclaim_expired() == {"redelivered": 0, "dead": 0}


def test_an_expired_lease_on_the_final_attempt_is_parked(client):
    client.enqueue("sleep", max_attempts=1, lease_ttl_ms=50)
    (task,) = client.lease("worker-a", lease_ttl_ms=50)
    _sleep_on_the_database(client, 0.1)

    with client.conn.transaction():
        assert client.reclaim_expired() == {"redelivered": 0, "dead": 1}
    assert client.get(task.id).state == "dead"


def test_each_delivery_counts_exactly_one_attempt(client):
    client.enqueue("sleep", max_attempts=4, lease_ttl_ms=50)
    seen = []
    for _ in range(3):
        (task,) = client.lease("worker-a", lease_ttl_ms=50)
        seen.append(task.attempts)
        _sleep_on_the_database(client, 0.1)
        with client.conn.transaction():
            client.reclaim_expired()
        # Clear any backoff so the next lease can happen immediately.
        client.conn.execute("UPDATE tasks SET available_at = now() WHERE id = %s", (task.id,))
    assert seen == [1, 2, 3]


def test_heartbeat_keeps_a_long_task_alive(client):
    client.enqueue("sleep", lease_ttl_ms=100)
    (task,) = client.lease("worker-a", lease_ttl_ms=100)

    for _ in range(4):
        _sleep_on_the_database(client, 0.05)
        assert client.heartbeat("worker-a", [task.fence], lease_ttl_ms=100) == 1
        with client.conn.transaction():
            assert client.reclaim_expired()["redelivered"] == 0

    assert client.get(task.id).state == "leased"


def test_heartbeat_from_a_stale_delivery_renews_nothing(client):
    client.enqueue("sleep", lease_ttl_ms=50)
    (first,) = client.lease("worker-a", lease_ttl_ms=50)
    _sleep_on_the_database(client, 0.1)
    with client.conn.transaction():
        client.reclaim_expired()
    client.conn.execute("UPDATE tasks SET available_at = now() WHERE id = %s", (first.id,))
    client.lease("worker-b")

    assert client.heartbeat("worker-a", [first.fence]) == 0


def test_stopping_the_heartbeat_loses_the_lease(client):
    """A hung worker is a live process that has stopped making progress. The
    lease notices the second thing, not the first."""
    client.enqueue("hang", lease_ttl_ms=80)
    (task,) = client.lease("worker-a", lease_ttl_ms=80)
    assert client.heartbeat("worker-a", [task.fence], lease_ttl_ms=80) == 1

    _sleep_on_the_database(client, 0.15)  # no further heartbeats

    with client.conn.transaction():
        assert client.reclaim_expired()["redelivered"] == 1
    assert client.get(task.id).state == "queued"


def test_backoff_grows_across_repeated_failures(client):
    """Not the exact delays, which are jittered, but the shape: later attempts
    can wait longer than earlier ones ever could."""
    from tether import backoff
    ceilings = [backoff.ceiling_ms(a, client.settings.backoff_base_ms,
                                   client.settings.backoff_cap_ms) for a in range(1, 5)]
    assert ceilings == sorted(ceilings)
    assert ceilings[-1] > ceilings[0]
