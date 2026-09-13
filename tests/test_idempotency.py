"""Milestone 3: at-least-once delivery plus idempotent handlers.

Exactly-once delivery is not available across a network. Exactly-once *effects*
are, and this is how.
"""

from __future__ import annotations

from tether import handlers
from tether.handlers import demo  # noqa: F401 - registration


def _charges(conn, account: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM charges WHERE account = %s", (account,))
        return cur.fetchone()[0]


def _run(client, task, worker="worker-a"):
    """Run a handler exactly as the worker does: one transaction for the effect,
    the idempotency claim, and the acknowledgement."""
    with client.conn.transaction():
        handlers.get(task.kind)(
            handlers.Context(task=task, conn=client.conn, client=client, worker_id=worker)
        )
        assert client.ack(task, worker)


# ------------------------------------------------------------- producer side


def test_the_same_idempotency_key_enqueues_one_task(client):
    first = client.enqueue("echo", {"n": 1}, idempotency_key="order-42")
    second = client.enqueue("echo", {"n": 1}, idempotency_key="order-42")
    assert first.id == second.id
    assert client.stats()["available"] == 1


def test_the_key_is_scoped_to_its_queue(client):
    a = client.enqueue("echo", queue="alpha", idempotency_key="order-42")
    b = client.enqueue("echo", queue="beta", idempotency_key="order-42")
    assert a.id != b.id


def test_tasks_without_a_key_are_never_deduplicated(client):
    a = client.enqueue("echo", {"n": 1})
    b = client.enqueue("echo", {"n": 1})
    assert a.id != b.id


# ------------------------------------------------------------- consumer side


def test_a_claim_can_only_be_taken_once(client):
    client.enqueue("echo")
    (task,) = client.lease("worker-a")
    ctx = handlers.Context(task=task, conn=client.conn, client=client, worker_id="worker-a")
    assert ctx.claim_effect("effect-1") is True
    assert ctx.claim_effect("effect-1") is False


def test_a_redelivered_task_charges_once(client):
    """The whole point. The task is delivered twice and the account is charged
    once, because the claim committed with the first acknowledgement."""
    client.enqueue("charge", {"account": "acct_safe", "amount_cents": 2500},
                   lease_ttl_ms=50, max_attempts=3)
    (first,) = client.lease("worker-a", lease_ttl_ms=50)
    _run(client, first)

    # Force a second delivery of the same work.
    client.conn.execute(
        "UPDATE tasks SET state='queued', completed_at=NULL, available_at=now() WHERE id=%s",
        (first.id,))
    (second,) = client.lease("worker-b")
    _run(client, second, worker="worker-b")

    assert _charges(client.conn, "acct_safe") == 1


def test_the_unsafe_handler_double_charges(client):
    """Kept deliberately. Without a claim, at-least-once delivery means the
    effect happens as many times as the task is delivered."""
    client.enqueue("charge_unsafe", {"account": "acct_unsafe", "amount_cents": 2500},
                   lease_ttl_ms=50, max_attempts=3)
    (first,) = client.lease("worker-a", lease_ttl_ms=50)
    _run(client, first)

    client.conn.execute(
        "UPDATE tasks SET state='queued', completed_at=NULL, available_at=now() WHERE id=%s",
        (first.id,))
    (second,) = client.lease("worker-b")
    _run(client, second, worker="worker-b")

    assert _charges(client.conn, "acct_unsafe") == 2


def test_a_rolled_back_delivery_leaves_no_claim_and_no_charge(client):
    """If the acknowledgement cannot commit, the effect must not either.

    Both live in one transaction, so a lease lost mid-handler rolls the charge
    back with it and the redelivery is free to try again.
    """
    client.enqueue("charge", {"account": "acct_rollback", "amount_cents": 100},
                   lease_ttl_ms=50)
    (task,) = client.lease("worker-a", lease_ttl_ms=50)

    try:
        with client.conn.transaction():
            handlers.get("charge")(
                handlers.Context(task=task, conn=client.conn, client=client, worker_id="worker-a")
            )
            raise RuntimeError("the lease was lost before the acknowledgement")
    except RuntimeError:
        pass

    assert _charges(client.conn, "acct_rollback") == 0
    with client.conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM effects WHERE task_id = %s", (task.id,))
        assert cur.fetchone()[0] == 0

    # And the retry succeeds cleanly, once the abandoned lease expires.
    client.conn.execute("SELECT pg_sleep(0.1)")
    with client.conn.transaction():
        assert client.reclaim_expired()["redelivered"] == 1
    client.conn.execute("UPDATE tasks SET available_at = now() WHERE id = %s", (task.id,))
    (retry,) = client.lease("worker-b")
    _run(client, retry, worker="worker-b")
    assert _charges(client.conn, "acct_rollback") == 1
