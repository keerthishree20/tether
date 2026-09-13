"""The HTTP surface: the dashboard page, the JSON it polls, and the scrape."""

from __future__ import annotations

import json
import socket
import threading
import urllib.error
import urllib.request

import pytest

from tether import metrics


@pytest.fixture
def server(base_settings, conn, settings):
    """A real server on a free port, torn down after the test."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    stop = threading.Event()
    thread = threading.Thread(
        target=metrics.serve, args=(port,),
        kwargs={"settings": base_settings, "stop": stop}, daemon=True)
    thread.start()

    base = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            urllib.request.urlopen(base + "/metrics", timeout=1).read()
            break
        except Exception:  # noqa: BLE001 - still starting
            threading.Event().wait(0.05)
    else:
        pytest.fail("metrics server never came up")

    yield base
    stop.set()
    thread.join(timeout=10)


def get(url: str):
    with urllib.request.urlopen(url, timeout=10) as response:
        return response.status, response.headers.get("Content-Type"), response.read()


# ---------------------------------------------------------------- the payload


def test_the_snapshot_carries_everything_the_page_draws(client):
    client.enqueue("echo")
    client.enqueue("echo", queue="reports")
    client.enqueue("always_fails", queue="broken", max_attempts=1)
    (poison,) = client.lease("worker-a", queue="broken")
    assert client.fail(poison, "worker-a", "RuntimeError: downstream gone") == "dead"

    snap = metrics.snapshot(client)

    assert set(snap) == {"now", "monotonic", "totals", "queues", "dead"}
    assert snap["totals"]["dead"] == 1
    assert {q["queue"] for q in snap["queues"]} == {"default", "reports", "broken"}
    assert snap["dead"][0]["kind"] == "always_fails"
    assert "downstream gone" in snap["dead"][0]["last_error"]
    assert json.dumps(snap)  # every value survives serialisation


def test_the_monotonic_clock_only_moves_forward(client):
    first = metrics.snapshot(client)["monotonic"]
    client.conn.execute("SELECT pg_sleep(0.05)")
    assert metrics.snapshot(client)["monotonic"] > first


def test_per_queue_gauges_are_ordered_busiest_first(client):
    for _ in range(3):
        client.enqueue("echo", queue="busy")
    client.enqueue("echo", queue="quiet")

    rows = client.stats_by_queue()
    assert [r["queue"] for r in rows] == ["busy", "quiet"]
    assert rows[0]["available"] == 3


def test_a_queue_that_only_holds_finished_work_still_appears(client):
    client.enqueue("echo", queue="drained")
    (task,) = client.lease("worker-a", queue="drained")
    client.ack(task, "worker-a")

    (row,) = client.stats_by_queue()
    assert row["queue"] == "drained"
    assert row["done"] == 1
    assert row["available"] == 0


# ----------------------------------------------------------------- the routes


def test_the_dashboard_page_is_served(server):
    status, content_type, body = get(server + "/")
    assert status == 200
    assert content_type.startswith("text/html")
    text = body.decode()
    assert "<title>Tether</title>" in text
    assert "/api/stats" in text


def test_the_json_route_parses(server, client):
    client.enqueue("echo")
    status, content_type, body = get(server + "/api/stats")
    assert status == 200
    assert content_type.startswith("application/json")
    payload = json.loads(body)
    assert payload["totals"]["available"] == 1


def test_the_prometheus_route_still_works(server):
    status, content_type, body = get(server + "/metrics")
    assert status == 200
    assert content_type.startswith("text/plain")
    assert b"tether_tasks_available" in body


def test_an_unknown_path_is_a_404_that_names_the_real_ones(server):
    with pytest.raises(urllib.error.HTTPError) as caught:
        get(server + "/dashboard")
    assert caught.value.code == 404


def test_responses_are_not_cached(server):
    with urllib.request.urlopen(server + "/api/stats", timeout=10) as response:
        assert response.headers.get("Cache-Control") == "no-store"
