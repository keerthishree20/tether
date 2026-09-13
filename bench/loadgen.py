"""Load generator.

Holds a fixed enqueue rate for a fixed time while separate worker processes
drain the queue, then reads the latency distribution back out of the database.

Both ends of every latency measurement are database timestamps, so nothing here
depends on the producer and the workers agreeing about the time.

    python -m bench.loadgen --rate 2000 --duration 20 --workers 4
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

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def spawn_workers(count: int, *, queue: str, concurrency: int, batch: int,
                  idle_exit_after: float) -> list[subprocess.Popen]:
    return [
        subprocess.Popen(
            [sys.executable, "-m", "tether.cli", "work",
             "--queue", queue,
             "--concurrency", str(concurrency),
             "--batch", str(batch),
             "--idle-exit-after", str(idle_exit_after)],
            cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
        )
        for _ in range(count)
    ]


def produce(client: Client, *, queue: str, rate: int, duration: float,
            payload_bytes: int, chunk: int = 200) -> int:
    """Enqueue at the target rate. Returns how many actually went in."""
    filler = "x" * max(payload_bytes - 20, 0)
    total = 0
    start = time.monotonic()
    while True:
        elapsed = time.monotonic() - start
        if elapsed >= duration:
            break
        # How many should exist by now, minus how many do.
        owed = int(rate * elapsed) - total
        if owed <= 0:
            time.sleep(0.001)
            continue
        batch = min(owed, chunk)
        client.enqueue_many("echo", ({"i": total + n, "filler": filler} for n in range(batch)),
                            queue=queue)
        total += batch
    return total


def measure(client: Client, queue: str) -> dict:
    with client.conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*),
                   EXTRACT(EPOCH FROM (max(completed_at) - min(completed_at))),
                   EXTRACT(EPOCH FROM (max(completed_at) - min(enqueued_at))),
                   EXTRACT(EPOCH FROM percentile_disc(0.50)
                       WITHIN GROUP (ORDER BY completed_at - enqueued_at)),
                   EXTRACT(EPOCH FROM percentile_disc(0.95)
                       WITHIN GROUP (ORDER BY completed_at - enqueued_at)),
                   EXTRACT(EPOCH FROM percentile_disc(0.99)
                       WITHIN GROUP (ORDER BY completed_at - enqueued_at)),
                   EXTRACT(EPOCH FROM max(completed_at - enqueued_at))
            FROM tasks
            WHERE queue = %s AND state = 'done'
            """,
            (queue,),
        )
        done, drain_s, span_s, p50, p95, p99, worst = cur.fetchone()
    drain_s = float(drain_s or 0)
    return {
        "completed": done,
        "drain_seconds": round(drain_s, 3),
        "span_seconds": round(float(span_s or 0), 3),
        "throughput_per_s": round(done / drain_s, 1) if drain_s > 0 else None,
        "p50_ms": round(float(p50 or 0) * 1000, 1),
        "p95_ms": round(float(p95 or 0) * 1000, 1),
        "p99_ms": round(float(p99 or 0) * 1000, 1),
        "max_ms": round(float(worst or 0) * 1000, 1),
    }


def run(args) -> dict:
    settings = load()
    conn = db.connect(settings)
    client = Client(conn, settings)
    db.migrate(conn)

    with conn.cursor() as cur:
        cur.execute("DELETE FROM tasks WHERE queue = %s", (args.queue,))

    workers = spawn_workers(args.workers, queue=args.queue, concurrency=args.concurrency,
                            batch=args.batch, idle_exit_after=args.idle_exit_after)
    try:
        offered = produce(client, queue=args.queue, rate=args.rate, duration=args.duration,
                          payload_bytes=args.payload_bytes)
        counters: list[dict] = []
        for proc in workers:
            out, _ = proc.communicate(timeout=args.duration + 300)
            line = (out or "").strip().splitlines()
            counters.append(json.loads(line[-1]) if line else {})
    finally:
        for proc in workers:
            if proc.poll() is None:
                proc.kill()

    totals: dict[str, int] = {}
    for c in counters:
        for key, value in c.items():
            totals[key] = totals.get(key, 0) + value

    result = measure(client, args.queue)
    result["offered"] = offered
    result["offered_rate_per_s"] = round(offered / args.duration, 1)
    result["worker_processes"] = args.workers
    result["slots_per_worker"] = args.concurrency
    result["batch"] = args.batch
    result["payload_bytes"] = args.payload_bytes
    result["worker_counters"] = totals
    calls = totals.get("lease_calls", 0)
    result["empty_lease_ratio"] = round(totals.get("empty_leases", 0) / calls, 4) if calls else None
    conn.close()
    return result


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="bench.loadgen", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rate", type=int, default=1000, help="tasks enqueued per second")
    p.add_argument("--duration", type=float, default=10.0, help="seconds to hold the rate")
    p.add_argument("--workers", type=int, default=4, help="worker processes")
    p.add_argument("--concurrency", type=int, default=4, help="slots per worker process")
    p.add_argument("--batch", type=int, default=10, help="tasks leased per round trip")
    p.add_argument("--payload-bytes", type=int, default=200)
    p.add_argument("--queue", default="bench")
    p.add_argument("--idle-exit-after", type=float, default=3.0)
    p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)

    result = run(args)
    if args.json:
        print(json.dumps(result, indent=2))
        return 0

    print(f"offered      {result['offered']} tasks at {result['offered_rate_per_s']}/s")
    print(f"completed    {result['completed']} tasks in {result['drain_seconds']}s")
    print(f"throughput   {result['throughput_per_s']}/s")
    print(f"latency      p50 {result['p50_ms']}ms   p95 {result['p95_ms']}ms   "
          f"p99 {result['p99_ms']}ms   max {result['max_ms']}ms")
    print(f"empty leases {result['empty_lease_ratio']}")
    print(f"counters     {json.dumps(result['worker_counters'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
