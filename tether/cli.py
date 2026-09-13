"""Command line interface.

    tether migrate                 create the schema
    tether enqueue echo '{"n":1}'  add a task
    tether work                    run a worker pool
    tether reap                    run the reaper on its own
    tether stats                   current gauges
    tether dashboard               live dashboard, Prometheus and JSON
    tether dlq list|replay|purge   operate on the dead-letter queue
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from . import db, handlers, metrics as metrics_mod
from .config import load
from .queue import Client
from .reaper import Reaper
from .worker import Worker


def _client(settings):
    conn = db.connect(settings)
    return conn, Client(conn, settings)


def cmd_migrate(args, settings) -> int:
    with db.connect(settings) as conn:
        db.migrate(conn)
    print("schema applied")
    return 0


def cmd_enqueue(args, settings) -> int:
    payload = json.loads(args.payload) if args.payload else {}
    conn, client = _client(settings)
    with conn:
        for _ in range(args.count):
            task = client.enqueue(
                args.kind, payload, queue=args.queue, priority=args.priority,
                delay_ms=args.delay_ms, max_attempts=args.max_attempts,
                idempotency_key=args.key,
            )
        print(f"queued task {task.id} kind={task.kind} queue={task.queue} "
              f"priority={task.priority} attempts={task.attempts}/{task.max_attempts}"
              + (f" (x{args.count})" if args.count > 1 else ""))
    return 0


def cmd_work(args, settings) -> int:
    worker = Worker(
        queue=args.queue,
        concurrency=args.concurrency,
        settings=settings,
        batch=args.batch,
        concurrency_cap=args.cap,
        run_reaper=args.with_reaper,
        idle_exit_after=args.idle_exit_after,
        on_event=_printer if args.verbose else None,
    )
    worker.install_signal_handlers()
    print(f"worker {worker.worker_id} on queue {args.queue!r}, {args.concurrency} slots, "
          f"handlers: {', '.join(handlers.known())}", file=sys.stderr)
    counters = worker.run()
    print(json.dumps(dict(counters)))
    return 0


def cmd_reap(args, settings) -> int:
    conn, client = _client(settings)
    reaper = Reaper(settings)
    with conn:
        if args.once:
            print(json.dumps(reaper.sweep_once(client)))
            return 0
    reaper.run()
    return 0


def cmd_stats(args, settings) -> int:
    conn, client = _client(settings)
    with conn:
        stats = client.stats(queue=args.queue)
        if args.json:
            print(json.dumps(stats))
        else:
            width = max(len(k) for k in stats)
            for key, value in stats.items():
                shown = f"{value:.1f}" if isinstance(value, float) else value
                print(f"{key.ljust(width)}  {shown}")
    return 0


def cmd_metrics(args, settings) -> int:
    print(f"dashboard   http://localhost:{args.port}/\n"
          f"prometheus  http://localhost:{args.port}/metrics\n"
          f"json        http://localhost:{args.port}/api/stats", file=sys.stderr)
    metrics_mod.serve(args.port, settings=settings, queue=args.queue)
    return 0


def cmd_dlq(args, settings) -> int:
    conn, client = _client(settings)
    with conn:
        if args.dlq_command == "list":
            rows = client.dead_letters(queue=args.queue, limit=args.limit)
            if not rows:
                print("dead-letter queue is empty")
                return 0
            for task in rows:
                error = (task.last_error or "").splitlines()[0][:80]
                print(f"{task.id:>8}  {task.queue:<12} {task.kind:<16} "
                      f"attempts={task.attempts}/{task.max_attempts}  {error}")
            return 0
        if args.dlq_command == "replay":
            ids = args.ids or [t.id for t in client.dead_letters(queue=args.queue, limit=10_000)]
            print(f"replayed {client.replay(ids, delay_ms=args.delay_ms)} task(s)")
            return 0
        print(f"purged {client.purge_dead(queue=args.queue)} task(s)")
    return 0


def cmd_check(args, settings) -> int:
    conn, client = _client(settings)
    with conn:
        violations = client.check_invariants()
    if violations:
        for v in violations:
            print(f"VIOLATED: {v}", file=sys.stderr)
        return 1
    print("all invariants hold")
    return 0


def _printer(event: str, fields: dict) -> None:
    print(f"{event} {json.dumps(fields)}", file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tether", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("migrate", help="create the schema").set_defaults(fn=cmd_migrate)

    p = sub.add_parser("enqueue", help="add a task")
    p.add_argument("kind", choices=handlers.known())
    p.add_argument("payload", nargs="?", default="{}")
    p.add_argument("--queue", default="default")
    p.add_argument("--priority", type=int, default=0)
    p.add_argument("--delay-ms", type=int, default=0)
    p.add_argument("--max-attempts", type=int, default=None)
    p.add_argument("--key", default=None, help="producer-side idempotency key")
    p.add_argument("--count", type=int, default=1)
    p.set_defaults(fn=cmd_enqueue)

    p = sub.add_parser("work", help="run a worker pool")
    p.add_argument("--queue", default="default")
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--batch", type=int, default=1, help="tasks leased per round trip")
    p.add_argument("--cap", type=int, default=None, help="per-queue in-flight limit")
    p.add_argument("--with-reaper", action="store_true")
    p.add_argument("--idle-exit-after", type=float, default=None,
                   help="exit after this many seconds with nothing to do")
    p.set_defaults(fn=cmd_work)

    p = sub.add_parser("reap", help="reclaim expired leases")
    p.add_argument("--once", action="store_true")
    p.set_defaults(fn=cmd_reap)

    p = sub.add_parser("stats", help="current gauges")
    p.add_argument("--queue", default=None)
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_stats)

    for name, blurb in (("dashboard", "serve the live dashboard, /metrics and /api/stats"),
                        ("metrics", "alias for dashboard, kept for scrape configs")):
        p = sub.add_parser(name, help=blurb)
        p.add_argument("--port", type=int, default=9464)
        p.add_argument("--queue", default=None)
        p.set_defaults(fn=cmd_metrics)

    p = sub.add_parser("dlq", help="operate on the dead-letter queue")
    p.add_argument("--queue", default=None)
    dsub = p.add_subparsers(dest="dlq_command", required=True)
    lp = dsub.add_parser("list")
    lp.add_argument("--limit", type=int, default=50)
    rp = dsub.add_parser("replay")
    rp.add_argument("ids", nargs="*", type=int)
    rp.add_argument("--delay-ms", type=int, default=0)
    dsub.add_parser("purge")
    p.set_defaults(fn=cmd_dlq)

    sub.add_parser("check", help="verify queue invariants").set_defaults(fn=cmd_check)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s %(message)s",
    )
    settings = load()
    try:
        return args.fn(args, settings)
    except db.DatabaseUnavailable as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
