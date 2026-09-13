"""Measures the two recovery numbers the benchmark sheet asks for.

    redelivery latency   worker killed -> task available to someone else
    recovery time        Postgres restarted -> tasks completing again

Both are timed against the database clock wherever the database is up, and
against wall clock only for the window when it is not.

    python -m bench.recovery --json
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time

from tether import db
from tether.config import load
from tether.queue import Client
from tether.reaper import Reaper

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONTAINER = os.environ.get("TETHER_PG_CONTAINER", "tether-pg")
QUEUE = "recovery-bench"


def _worker(*args: str, **env: str) -> subprocess.Popen:
    environ = dict(os.environ)
    environ.update(env)
    return subprocess.Popen(
        [sys.executable, "-m", "tether.cli", "work", "--queue", QUEUE, *args],
        cwd=REPO_ROOT, env=environ,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True,
    )


def _wait(predicate, *, timeout_s: float, what: str):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    raise TimeoutError(f"timed out after {timeout_s:g}s waiting for {what}")


def redelivery_latency(client: Client, *, lease_ttl_ms: int, reap_interval_ms: int) -> dict:
    """How long a killed worker's task stays stuck.

    The floor is the lease TTL: nothing can know the worker is gone before its
    lease could plausibly have been renewed. The reaper's sweep interval adds
    the rest.
    """
    client.conn.execute("DELETE FROM tasks WHERE queue = %s", (QUEUE,))
    task = client.enqueue("sleep", {"seconds": 300}, queue=QUEUE, lease_ttl_ms=lease_ttl_ms)

    proc = _worker("--concurrency", "1",
                   TETHER_LEASE_TTL_MS=str(lease_ttl_ms),
                   TETHER_HEARTBEAT_MS=str(max(lease_ttl_ms // 3, 50)))
    try:
        _wait(lambda: client.get(task.id).state == "leased", timeout_s=30,
              what="the worker to take the task")
        killed_at = db.db_now(client.conn)
        proc.kill()
        proc.wait(timeout=10)

        reaper = Reaper(client.settings)
        _wait(lambda: (reaper.sweep_once(client), client.get(task.id).state == "queued")[1],
              timeout_s=60, what="the reaper to reclaim the task")
        back_at = db.db_now(client.conn)
    finally:
        if proc.poll() is None:
            proc.kill()

    client.conn.execute("DELETE FROM tasks WHERE queue = %s", (QUEUE,))
    return {
        "lease_ttl_ms": lease_ttl_ms,
        "reap_interval_ms": reap_interval_ms,
        "redelivery_ms": round((back_at - killed_at).total_seconds() * 1000, 1),
    }


def restart_recovery(settings, client: Client, *, tasks: int = 3000) -> dict:
    """Restart Postgres under a running worker pool and time the hole."""
    client.conn.execute("DELETE FROM tasks WHERE queue = %s", (QUEUE,))
    client.enqueue_many("echo", ({"i": i} for i in range(tasks)), queue=QUEUE)

    proc = _worker("--concurrency", "4", "--batch", "10", "--with-reaper",
                   "--idle-exit-after", "600")
    try:
        _wait(lambda: client.stats(queue=QUEUE)["done"] > 200, timeout_s=60,
              what="the pool to reach steady state")
        before = client.stats(queue=QUEUE)["done"]
        client.conn.close()

        restarted_at = time.monotonic()
        subprocess.run(["docker", "restart", CONTAINER], check=True,
                       capture_output=True, timeout=180)

        conn = db.wait_until_ready(settings, timeout_s=180)
        db_back = time.monotonic() - restarted_at
        fresh = Client(conn, settings)

        _wait(lambda: fresh.stats(queue=QUEUE)["done"] > before, timeout_s=180,
              what="tasks to start completing again")
        throughput_back = time.monotonic() - restarted_at

        _wait(lambda: fresh.stats(queue=QUEUE)["done"] + fresh.stats(queue=QUEUE)["dead"] >= tasks,
              timeout_s=300, what="the backlog to drain")
        final = fresh.stats(queue=QUEUE)
        violations = fresh.check_invariants()
        conn.close()
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()

    return {
        "tasks": tasks,
        "database_back_s": round(db_back, 2),
        "throughput_restored_s": round(throughput_back, 2),
        "completed": final["done"],
        "dead": final["dead"],
        "lost": tasks - final["done"] - final["dead"],
        "invariant_violations": violations,
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="bench.recovery", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--lease-ttl-ms", type=int, default=2000)
    p.add_argument("--tasks", type=int, default=3000)
    p.add_argument("--skip-restart", action="store_true",
                   help="only measure redelivery, leave Postgres alone")
    p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)

    settings = load()
    conn = db.connect(settings)
    db.migrate(conn)
    client = Client(conn, settings)

    result = {"redelivery": redelivery_latency(
        client, lease_ttl_ms=args.lease_ttl_ms, reap_interval_ms=settings.reap_interval_ms)}
    if not args.skip_restart:
        result["restart"] = restart_recovery(settings, client, tasks=args.tasks)
    else:
        conn.close()

    print(json.dumps(result, indent=2) if args.json else _human(result))
    return 0


def _human(result: dict) -> str:
    lines = [
        f"redelivery   {result['redelivery']['redelivery_ms']}ms after the kill "
        f"(lease TTL {result['redelivery']['lease_ttl_ms']}ms, "
        f"sweep {result['redelivery']['reap_interval_ms']}ms)"
    ]
    if "restart" in result:
        r = result["restart"]
        lines += [
            f"database     back in {r['database_back_s']}s",
            f"throughput   restored in {r['throughput_restored_s']}s",
            f"tasks        {r['completed']} completed, {r['dead']} dead, {r['lost']} lost",
            f"invariants   {r['invariant_violations'] or 'all hold'}",
        ]
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
