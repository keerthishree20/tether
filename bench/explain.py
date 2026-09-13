"""Prints the query plan for the lease statement against a loaded queue.

The lease query orders by `(priority + boost) DESC, available_at`. Both are
stored columns, so `tasks_ready` can serve the ordering directly. Had the
starvation boost been computed from `now()` in the ORDER BY instead, this plan
would show a sort over every eligible row on every poll, and the cost would look
like lease contention rather than what it is.

    python -m bench.explain --rows 50000
"""

from __future__ import annotations

import argparse

from tether import db
from tether.config import load

QUEUE = "explain-bench"

LEASE_SELECT = """
    SELECT id
    FROM tasks
    WHERE queue = %s AND state = 'queued' AND available_at <= now()
    ORDER BY (priority + boost) DESC, available_at
    FOR UPDATE SKIP LOCKED
    LIMIT %s
"""


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="bench.explain", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rows", type=int, default=50_000)
    p.add_argument("--batch", type=int, default=20)
    args = p.parse_args(argv)

    settings = load()
    conn = db.connect(settings)
    db.migrate(conn)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM tasks WHERE queue = %s", (QUEUE,))
        cur.execute(
            """INSERT INTO tasks (queue, kind, payload, priority)
               SELECT %s, 'echo', '{}'::jsonb, (random() * 5)::int
               FROM generate_series(1, %s)""",
            (QUEUE, args.rows),
        )
        cur.execute("ANALYZE tasks")
        print(f"{args.rows} queued tasks, leasing {args.batch}\n")
        cur.execute("EXPLAIN (ANALYZE, BUFFERS) " + LEASE_SELECT, (QUEUE, args.batch))
        for (line,) in cur.fetchall():
            print(line)
        cur.execute("DELETE FROM tasks WHERE queue = %s", (QUEUE,))
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
