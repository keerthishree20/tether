"""Milestone 6: the gauges an operator actually watches."""

from __future__ import annotations

from tether import metrics


def test_gauges_split_the_states(client):
    client.enqueue("echo")
    client.enqueue("echo", delay_ms=60_000)
    client.enqueue("echo")
    (leased,) = client.lease("worker-a")
    client.ack(leased, "worker-a")
    (in_flight,) = client.lease("worker-a")

    stats = client.stats()
    assert stats["available"] == 0
    assert stats["scheduled"] == 1
    assert stats["in_flight"] == 1
    assert stats["done"] == 1
    assert stats["dead"] == 0


def test_oldest_available_age_climbs_while_nothing_drains(client):
    """Depth alone cannot distinguish a busy queue from a stuck one. This can."""
    client.enqueue("echo")
    client.conn.execute("SELECT pg_sleep(0.3)")
    assert client.stats()["oldest_available_age_s"] >= 0.3

    (task,) = client.lease("worker-a")
    client.ack(task, "worker-a")
    assert client.stats()["oldest_available_age_s"] == 0.0


def test_the_age_ignores_tasks_that_are_not_due_yet(client):
    client.enqueue("echo", delay_ms=60_000)
    assert client.stats()["oldest_available_age_s"] == 0.0


def test_gauges_can_be_scoped_to_one_queue(client):
    client.enqueue("echo", queue="alpha")
    client.enqueue("echo", queue="beta")
    client.enqueue("echo", queue="beta")
    assert client.stats(queue="alpha")["available"] == 1
    assert client.stats(queue="beta")["available"] == 2


def test_prometheus_output_is_well_formed(client):
    client.enqueue("echo")
    body = metrics.render(client)

    assert "# HELP tether_tasks_available" in body
    assert "# TYPE tether_tasks_available gauge" in body
    assert "\ntether_tasks_available 1\n" in body
    assert body.endswith("\n")

    for line in body.splitlines():
        if line.startswith("#"):
            continue
        name, value = line.rsplit(" ", 1)
        assert name.startswith("tether_")
        float(value)


def test_prometheus_output_labels_a_scoped_queue(client):
    client.enqueue("echo", queue="alpha")
    body = metrics.render(client, queue="alpha")
    assert 'tether_tasks_available{queue="alpha"} 1' in body
