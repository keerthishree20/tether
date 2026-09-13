from __future__ import annotations

import dataclasses
import os
import shutil
import subprocess
import sys
import time
from typing import Iterator

import pytest

from tether import db
from tether.config import Settings, load
from tether.queue import Client

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="session")
def base_settings() -> Settings:
    settings = load()
    try:
        conn = db.connect(settings)
    except db.DatabaseUnavailable as exc:
        # Fail loudly and actionably. A raw driver error here reads like a code
        # bug, and on this machine it would point at the wrong Postgres.
        pytest.exit(f"\n{exc}\n", returncode=2)
    with conn:
        db.migrate(conn)
    return settings


@pytest.fixture
def settings(base_settings: Settings) -> Settings:
    """Short timings so lease expiry is observable inside a test."""
    return dataclasses.replace(
        base_settings,
        lease_ttl_ms=700,
        heartbeat_ms=200,
        reap_interval_ms=100,
        backoff_base_ms=10,
        backoff_cap_ms=100,
        poll_interval_ms=50,
        boost_after_ms=300,
        boost_max=5,
    )


@pytest.fixture
def conn(base_settings: Settings):
    connection = db.connect(base_settings)
    db.reset(connection)
    yield connection
    connection.close()


@pytest.fixture
def client(conn, settings: Settings) -> Client:
    return Client(conn, settings)


@pytest.fixture
def second_client(base_settings: Settings, settings: Settings) -> Iterator[Client]:
    """A separate connection, for tests that need two concurrent sessions."""
    connection = db.connect(base_settings)
    yield Client(connection, settings)
    connection.close()


# --------------------------------------------------------------------- helpers


def worker_env(settings: Settings, **overrides: object) -> dict:
    env = dict(os.environ)
    env.update({
        "TETHER_DSN": settings.dsn,
        "TETHER_LEASE_TTL_MS": str(settings.lease_ttl_ms),
        "TETHER_HEARTBEAT_MS": str(settings.heartbeat_ms),
        "TETHER_BACKOFF_BASE_MS": str(settings.backoff_base_ms),
        "TETHER_BACKOFF_CAP_MS": str(settings.backoff_cap_ms),
        "TETHER_POLL_INTERVAL_MS": str(settings.poll_interval_ms),
        "TETHER_REAP_INTERVAL_MS": str(settings.reap_interval_ms),
        "PYTHONPATH": REPO_ROOT,
    })
    env.update({k: str(v) for k, v in overrides.items()})
    return env


@pytest.fixture
def spawn_worker(settings: Settings):
    """Start `tether work` as a real subprocess so it can really be killed."""
    started: list[subprocess.Popen] = []

    def start(*args: str, **env_overrides: object) -> subprocess.Popen:
        proc = subprocess.Popen(
            [sys.executable, "-m", "tether.cli", "work", *args],
            cwd=REPO_ROOT,
            env=worker_env(settings, **env_overrides),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        started.append(proc)
        return proc

    yield start

    for proc in started:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)


def wait_for(predicate, *, timeout_s: float = 15.0, interval_s: float = 0.05, what: str = "condition"):
    """Poll until the predicate returns something truthy, or fail the test."""
    deadline = time.monotonic() + timeout_s
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval_s)
    pytest.fail(f"timed out after {timeout_s:g}s waiting for {what} (last value: {last!r})")


def docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    return subprocess.run(["docker", "inspect", os.environ.get("TETHER_PG_CONTAINER", "tether-pg")],
                          capture_output=True).returncode == 0


requires_docker = pytest.mark.skipif(
    not docker_available(),
    reason="needs the tether-pg container (make db) to restart Postgres",
)
