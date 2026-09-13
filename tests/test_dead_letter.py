"""Milestone 4: work that will never succeed stops costing worker time."""

from __future__ import annotations

from tether.worker import Worker


def test_a_poison_task_stops_after_its_attempt_budget(client, settings):
    """It must not cycle forever. Three attempts, then parked."""
    task = client.enqueue("always_fails", {"reason": "downstream schema changed"},
                          max_attempts=3)
    worker = Worker(settings=settings, concurrency=1, idle_exit_after=1.5,
                    worker_id="worker-a")
    counters = worker.run()

    parked = client.get(task.id)
    assert parked.state == "dead"
    assert parked.attempts == 3
    assert "downstream schema changed" in parked.last_error
    assert counters["dead"] == 1
    assert counters["leased"] == 3


def test_the_final_error_is_kept(client):
    client.enqueue("always_fails", max_attempts=1)
    (task,) = client.lease("worker-a")
    client.fail(task, "worker-a", "ValueError: unparseable date '31/02/2026'")
    (dead,) = client.dead_letters()
    assert dead.id == task.id
    assert "31/02/2026" in dead.last_error


def test_dead_letters_can_be_filtered_by_queue(client):
    for queue in ("alpha", "beta"):
        client.enqueue("always_fails", queue=queue, max_attempts=1)
        (task,) = client.lease("worker-a", queue=queue)
        client.fail(task, "worker-a", "boom")

    assert len(client.dead_letters()) == 2
    assert [t.queue for t in client.dead_letters(queue="alpha")] == ["alpha"]


def test_replay_puts_a_dead_letter_back_with_a_fresh_budget(client):
    client.enqueue("always_fails", max_attempts=2)
    for _ in range(2):
        # Step past the retry backoff rather than sleeping through it.
        client.conn.execute("UPDATE tasks SET available_at = now() WHERE state = 'queued'")
        (task,) = client.lease("worker-a")
        client.fail(task, "worker-a", "boom")
    assert client.get(task.id).state == "dead"

    assert client.replay([task.id]) == 1
    revived = client.get(task.id)
    assert revived.state == "queued"
    assert revived.attempts == 0
    assert revived.dead_at is None
    assert client.lease("worker-b")


def test_replay_ignores_tasks_that_are_not_dead(client):
    live = client.enqueue("echo")
    assert client.replay([live.id]) == 0
    assert client.get(live.id).state == "queued"


def test_purge_empties_the_dead_letter_queue(client):
    for _ in range(3):
        client.enqueue("always_fails", max_attempts=1)
    for _ in range(3):
        (task,) = client.lease("worker-a")
        client.fail(task, "worker-a", "boom")

    assert client.purge_dead() == 3
    assert client.dead_letters() == []


def test_replaying_a_dead_letter_really_reruns_its_effect(client):
    """Replay must not be quietly swallowed by the idempotency ledger.

    It is not, and the reason is worth stating: a claim commits only in the
    acknowledgement's transaction, so a task that never succeeded never left one
    behind. There is nothing for the replay to collide with.
    """
    from tether import handlers

    task = client.enqueue("charge", {"account": "acct_replay", "amount_cents": 999},
                          max_attempts=1)
    (leased,) = client.lease("worker-a")
    try:
        with client.conn.transaction():
            handlers.get("charge")(handlers.Context(task=leased, conn=client.conn,
                                                    client=client, worker_id="worker-a"))
            raise RuntimeError("died before the acknowledgement")
    except RuntimeError:
        pass
    assert client.fail(leased, "worker-a", "died before ack") == "dead"

    with client.conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM effects WHERE task_id = %s", (task.id,))
        assert cur.fetchone()[0] == 0, "a task that never succeeded left a claim behind"
        cur.execute("SELECT count(*) FROM charges WHERE account = 'acct_replay'")
        assert cur.fetchone()[0] == 0

    assert client.replay([task.id]) == 1
    (again,) = client.lease("worker-b")
    with client.conn.transaction():
        handlers.get("charge")(handlers.Context(task=again, conn=client.conn,
                                                client=client, worker_id="worker-b"))
        assert client.ack(again, "worker-b")

    with client.conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM charges WHERE account = 'acct_replay'")
        assert cur.fetchone()[0] == 1, "replay either did nothing or charged twice"


def test_replay_is_a_no_op_when_the_effect_already_committed(client):
    """The other half of the rule. A claim keyed on the business fact rather
    than the task row is shared, so replaying different work for the same
    business fact correctly does nothing."""
    from tether import handlers

    first = client.enqueue("charge", {"account": "acct_shared", "amount_cents": 500,
                                      "idempotency_key": "charge:order-77"})
    (leased,) = client.lease("worker-a")
    with client.conn.transaction():
        handlers.get("charge")(handlers.Context(task=leased, conn=client.conn,
                                                client=client, worker_id="worker-a"))
        assert client.ack(leased, "worker-a")

    # A second, separate task for the same business fact.
    client.enqueue("charge", {"account": "acct_shared", "amount_cents": 500,
                              "idempotency_key": "charge:order-77"})
    (second,) = client.lease("worker-b")
    with client.conn.transaction():
        handlers.get("charge")(handlers.Context(task=second, conn=client.conn,
                                                client=client, worker_id="worker-b"))
        assert client.ack(second, "worker-b")

    with client.conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM charges WHERE account = 'acct_shared'")
        assert cur.fetchone()[0] == 1
    assert first.id


def test_a_dead_task_never_reaches_a_worker_again(client, settings):
    client.enqueue("always_fails", max_attempts=1)
    Worker(settings=settings, concurrency=1, idle_exit_after=1.0, worker_id="w1").run()
    assert client.stats()["dead"] == 1

    counters = Worker(settings=settings, concurrency=1, idle_exit_after=1.0,
                      worker_id="w2").run()
    assert counters["leased"] == 0
