"""The queue itself: enqueue, lease, acknowledge, fail, reap.

`Client` never opens or commits a transaction. Every method runs on the
connection it is handed, so the caller decides what commits together. That is
what lets a handler's effect and its acknowledgement land atomically.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Sequence

import psycopg

from . import backoff
from .config import Settings, load

_TASK_FIELDS = (
    "id", "queue", "kind", "payload", "state", "priority", "boost", "attempts",
    "max_attempts", "available_at", "leased_until", "leased_by", "lease_ttl_ms",
    "idempotency_key", "last_error", "enqueued_at", "completed_at", "dead_at",
)

#: Column list in the exact order `_row_to_task` unpacks.
_TASK_COLUMNS = ", ".join(_TASK_FIELDS)


def _qualified(alias: str) -> str:
    """The same column list, table-qualified, for RETURNING out of a join."""
    return ", ".join(f"{alias}.{name}" for name in _TASK_FIELDS)


@dataclass(frozen=True)
class Task:
    id: int
    queue: str
    kind: str
    payload: dict
    state: str
    priority: int
    boost: int
    attempts: int
    max_attempts: int
    available_at: datetime
    leased_until: datetime | None
    leased_by: str | None
    lease_ttl_ms: int
    idempotency_key: str | None
    last_error: str | None
    enqueued_at: datetime
    completed_at: datetime | None
    dead_at: datetime | None

    @property
    def fence(self) -> tuple[int, int]:
        """Identifies one delivery of this task.

        `attempts` increments on every lease, so the pair pins the exact
        delivery a worker is holding. An acknowledgement carrying a stale fence
        is rejected, which is what stops a worker that was reaped mid-task from
        marking someone else's redelivery as done.
        """
        return (self.id, self.attempts)

    @property
    def attempts_remaining(self) -> int:
        return max(self.max_attempts - self.attempts, 0)


def _row_to_task(row: Sequence[Any]) -> Task:
    payload = row[3]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return Task(
        id=row[0], queue=row[1], kind=row[2], payload=payload, state=row[4],
        priority=row[5], boost=row[6], attempts=row[7], max_attempts=row[8],
        available_at=row[9], leased_until=row[10], leased_by=row[11],
        lease_ttl_ms=row[12], idempotency_key=row[13], last_error=row[14],
        enqueued_at=row[15], completed_at=row[16], dead_at=row[17],
    )


class Client:
    def __init__(self, conn: psycopg.Connection, settings: Settings | None = None):
        self.conn = conn
        self.settings = settings or load()

    # ------------------------------------------------------------------ enqueue

    def enqueue(
        self,
        kind: str,
        payload: dict | None = None,
        *,
        queue: str = "default",
        priority: int = 0,
        delay_ms: int = 0,
        max_attempts: int | None = None,
        lease_ttl_ms: int | None = None,
        idempotency_key: str | None = None,
    ) -> Task:
        """Add a task. Returns the existing task if the idempotency key is a repeat.

        A retried API call must not create a second unit of work, so the key is
        enforced by a unique index rather than by a read-then-write that two
        concurrent producers could both pass.
        """
        sql = f"""
            INSERT INTO tasks (queue, kind, payload, priority, available_at,
                               max_attempts, lease_ttl_ms, idempotency_key)
            VALUES (%(queue)s, %(kind)s, %(payload)s, %(priority)s,
                    now() + make_interval(secs => %(delay_s)s),
                    %(max_attempts)s, %(lease_ttl_ms)s, %(key)s)
            ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL
            DO NOTHING
            RETURNING {_TASK_COLUMNS}
        """
        params = {
            "queue": queue,
            "kind": kind,
            "payload": json.dumps(payload or {}),
            "priority": priority,
            "delay_s": delay_ms / 1000.0,
            "max_attempts": max_attempts if max_attempts is not None else self.settings.max_attempts,
            "lease_ttl_ms": lease_ttl_ms if lease_ttl_ms is not None else self.settings.lease_ttl_ms,
            "key": idempotency_key,
        }
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if row is not None:
                return _row_to_task(row)
            # The insert was suppressed by the unique index: hand back the task
            # that already owns this key.
            cur.execute(
                f"SELECT {_TASK_COLUMNS} FROM tasks WHERE queue = %s AND idempotency_key = %s",
                (queue, idempotency_key),
            )
            return _row_to_task(cur.fetchone())

    def enqueue_many(self, kind: str, payloads: Iterable[dict], *, queue: str = "default",
                     priority: int = 0) -> int:
        rows = [(queue, kind, json.dumps(p), priority,
                 self.settings.max_attempts, self.settings.lease_ttl_ms) for p in payloads]
        if not rows:
            return 0
        with self.conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO tasks (queue, kind, payload, priority, max_attempts, lease_ttl_ms)"
                " VALUES (%s, %s, %s, %s, %s, %s)",
                rows,
            )
        return len(rows)

    # -------------------------------------------------------------------- lease

    def lease(
        self,
        worker_id: str,
        *,
        queue: str = "default",
        batch: int = 1,
        lease_ttl_ms: int | None = None,
        concurrency_cap: int | None = None,
    ) -> list[Task]:
        """Take up to `batch` tasks, marking them leased for `lease_ttl_ms`.

        The concurrency cap is applied inside this statement, as part of the
        LIMIT. Checking it in the worker after leasing would mean holding a
        lease on work the worker is not allowed to start, which stalls the task
        for a full lease TTL and can wedge the whole pool.
        """
        ttl_ms = lease_ttl_ms if lease_ttl_ms is not None else self.settings.lease_ttl_ms
        sql = f"""
            WITH picked AS (
                SELECT id
                FROM tasks
                WHERE queue = %(queue)s
                  AND state = 'queued'
                  AND available_at <= now()
                ORDER BY (priority + boost) DESC, available_at
                FOR UPDATE SKIP LOCKED
                LIMIT (
                    CASE WHEN %(cap)s::int IS NULL THEN %(batch)s
                         ELSE GREATEST(
                             LEAST(
                                 %(batch)s,
                                 %(cap)s::int - (SELECT count(*) FROM tasks c
                                                 WHERE c.queue = %(queue)s AND c.state = 'leased')
                             ), 0)
                    END
                )
            )
            UPDATE tasks t
            SET state = 'leased',
                leased_by = %(worker)s,
                leased_until = now() + make_interval(secs => %(ttl_s)s),
                attempts = t.attempts + 1,
                boost = 0
            FROM picked
            WHERE t.id = picked.id
            RETURNING {_qualified('t')}
        """
        params = {
            "queue": queue, "worker": worker_id, "batch": batch,
            "ttl_s": ttl_ms / 1000.0, "cap": concurrency_cap,
        }
        if concurrency_cap is None:
            with self.conn.cursor() as cur:
                cur.execute(sql, params)
                return [_row_to_task(r) for r in cur.fetchall()]

        # A cap has to be counted and consumed as one step. Two workers that
        # both read "one in flight, cap of four" will both take three, and the
        # cap is breached without either of them doing anything wrong. An
        # advisory lock serialises leasing for this queue, which is the real
        # cost of a cap: it is charged only to the queues that ask for one, and
        # uncapped queues keep the full SKIP LOCKED parallelism.
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (queue,))
            cur.execute(sql, params)
            return [_row_to_task(r) for r in cur.fetchall()]

    def heartbeat(self, worker_id: str, fences: Sequence[tuple[int, int]],
                  *, lease_ttl_ms: int | None = None) -> int:
        """Extend the lease on tasks this worker is still running.

        A worker that hangs stops calling this, its lease lapses, and the reaper
        takes the task back even though the process is still alive. That is the
        intended behaviour, not a gap.
        """
        if not fences:
            return 0
        ttl_ms = lease_ttl_ms if lease_ttl_ms is not None else self.settings.lease_ttl_ms
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tasks t
                SET leased_until = now() + make_interval(secs => %(ttl_s)s)
                FROM unnest(%(ids)s::bigint[], %(atts)s::int[]) AS f(id, attempts)
                WHERE t.id = f.id
                  AND t.attempts = f.attempts
                  AND t.state = 'leased'
                  AND t.leased_by = %(worker)s
                """,
                {
                    "ttl_s": ttl_ms / 1000.0,
                    "ids": [f[0] for f in fences],
                    "atts": [f[1] for f in fences],
                    "worker": worker_id,
                },
            )
            return cur.rowcount

    # ------------------------------------------------------------------ finish

    def ack(self, task: Task, worker_id: str) -> bool:
        """Mark a delivery done. False means the lease had already been lost."""
        task_id, attempts = task.fence
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tasks
                SET state = 'done', completed_at = now(),
                    leased_until = NULL, leased_by = NULL, last_error = NULL
                WHERE id = %s AND attempts = %s AND state = 'leased' AND leased_by = %s
                """,
                (task_id, attempts, worker_id),
            )
            return cur.rowcount == 1

    def fail(self, task: Task, worker_id: str, error: str) -> str | None:
        """Report a handler failure. Returns the resulting state, or None if the
        lease had already been lost.

        The task goes to the dead-letter queue only when this delivery used up
        the last attempt; otherwise it returns to the queue after a jittered
        backoff.
        """
        task_id, attempts = task.fence
        delay = backoff.delay_ms(attempts, self.settings.backoff_base_ms, self.settings.backoff_cap_ms)
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tasks
                SET state = CASE WHEN attempts >= max_attempts THEN 'dead'::task_state
                                 ELSE 'queued'::task_state END,
                    available_at = CASE WHEN attempts >= max_attempts THEN available_at
                                        ELSE now() + make_interval(secs => %(delay_s)s) END,
                    dead_at = CASE WHEN attempts >= max_attempts THEN now() ELSE NULL END,
                    leased_until = NULL, leased_by = NULL,
                    last_error = %(err)s
                WHERE id = %(id)s AND attempts = %(att)s AND state = 'leased' AND leased_by = %(worker)s
                RETURNING state::text
                """,
                {"id": task_id, "att": attempts, "worker": worker_id,
                 "err": error[:4000], "delay_s": delay / 1000.0},
            )
            row = cur.fetchone()
            return row[0] if row else None

    # ------------------------------------------------------------------- reaper

    def reclaim_expired(self, *, batch: int = 500) -> dict[str, int]:
        """Return tasks whose lease elapsed, or park them if attempts ran out.

        Nothing tells the queue a worker died. This is the only mechanism that
        notices, and it notices by absence: a lease that stopped being renewed.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, attempts, max_attempts
                FROM tasks
                WHERE state = 'leased' AND leased_until < now()
                ORDER BY leased_until
                FOR UPDATE SKIP LOCKED
                LIMIT %s
                """,
                (batch,),
            )
            expired = cur.fetchall()
            if not expired:
                return {"redelivered": 0, "dead": 0}

            retry = [(tid, backoff.delay_ms(att, self.settings.backoff_base_ms,
                                            self.settings.backoff_cap_ms))
                     for tid, att, mx in expired if att < mx]
            dead = [tid for tid, att, mx in expired if att >= mx]

            if retry:
                cur.execute(
                    """
                    UPDATE tasks t
                    SET state = 'queued', leased_until = NULL, leased_by = NULL,
                        available_at = now() + make_interval(secs => f.delay_s),
                        last_error = 'lease expired without acknowledgement'
                    FROM unnest(%(ids)s::bigint[], %(delays)s::float8[]) AS f(id, delay_s)
                    WHERE t.id = f.id
                    """,
                    {"ids": [r[0] for r in retry], "delays": [r[1] / 1000.0 for r in retry]},
                )
            if dead:
                cur.execute(
                    """
                    UPDATE tasks
                    SET state = 'dead', dead_at = now(), leased_until = NULL, leased_by = NULL,
                        last_error = 'lease expired without acknowledgement on the final attempt'
                    WHERE id = ANY(%s)
                    """,
                    (dead,),
                )
            return {"redelivered": len(retry), "dead": len(dead)}

    def apply_starvation_boost(self) -> int:
        """Raise the boost of queued tasks that have waited past the threshold.

        Boost is stored rather than computed at lease time so the lease query can
        keep using its index. An expression over now() in the ORDER BY would sort
        the whole eligible set on every poll, and that cost shows up as apparent
        lease contention under load.
        """
        after_ms = self.settings.boost_after_ms
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tasks
                SET boost = LEAST(%(max)s,
                        (EXTRACT(EPOCH FROM (now() - available_at)) * 1000 / %(after)s)::int)
                WHERE state = 'queued'
                  AND available_at <= now() - make_interval(secs => %(after_s)s)
                  AND boost < LEAST(%(max)s,
                        (EXTRACT(EPOCH FROM (now() - available_at)) * 1000 / %(after)s)::int)
                """,
                {"max": self.settings.boost_max, "after": after_ms, "after_s": after_ms / 1000.0},
            )
            return cur.rowcount

    # -------------------------------------------------------------- dead letters

    def dead_letters(self, *, queue: str | None = None, limit: int = 50) -> list[Task]:
        sql = f"SELECT {_TASK_COLUMNS} FROM tasks WHERE state = 'dead'"
        params: list[Any] = []
        if queue:
            sql += " AND queue = %s"
            params.append(queue)
        sql += " ORDER BY dead_at DESC LIMIT %s"
        params.append(limit)
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return [_row_to_task(r) for r in cur.fetchall()]

    def replay(self, task_ids: Sequence[int], *, delay_ms: int = 0) -> int:
        """Put dead letters back on their queue with a fresh attempt budget."""
        if not task_ids:
            return 0
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tasks
                SET state = 'queued', attempts = 0, boost = 0, dead_at = NULL,
                    available_at = now() + make_interval(secs => %s)
                WHERE id = ANY(%s) AND state = 'dead'
                """,
                (delay_ms / 1000.0, list(task_ids)),
            )
            return cur.rowcount

    def purge_dead(self, *, queue: str | None = None) -> int:
        with self.conn.cursor() as cur:
            if queue:
                cur.execute("DELETE FROM tasks WHERE state = 'dead' AND queue = %s", (queue,))
            else:
                cur.execute("DELETE FROM tasks WHERE state = 'dead'")
            return cur.rowcount

    def get(self, task_id: int) -> Task | None:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT {_TASK_COLUMNS} FROM tasks WHERE id = %s", (task_id,))
            row = cur.fetchone()
            return _row_to_task(row) if row else None

    # -------------------------------------------------------------------- gauges

    def stats(self, *, queue: str | None = None) -> dict[str, Any]:
        where = "WHERE queue = %s" if queue else ""
        params = (queue,) if queue else ()
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    count(*) FILTER (WHERE state = 'queued' AND available_at <= now()) AS available,
                    count(*) FILTER (WHERE state = 'queued' AND available_at > now())  AS scheduled,
                    count(*) FILTER (WHERE state = 'leased')                           AS in_flight,
                    count(*) FILTER (WHERE state = 'done')                             AS done,
                    count(*) FILTER (WHERE state = 'dead')                             AS dead,
                    COALESCE(EXTRACT(EPOCH FROM (now() - min(available_at)
                        FILTER (WHERE state = 'queued' AND available_at <= now()))), 0) AS oldest_available_age_s
                FROM tasks {where}
                """,
                params,
            )
            available, scheduled, in_flight, done, dead, oldest = cur.fetchone()
        return {
            "available": available,
            "scheduled": scheduled,
            "in_flight": in_flight,
            "done": done,
            "dead": dead,
            # The single most useful number here. Depth alone cannot tell a busy
            # queue from a stuck one; the age of the oldest waiting task can.
            "oldest_available_age_s": float(oldest),
        }

    def stats_by_queue(self) -> list[dict[str, Any]]:
        """The same gauges, split per queue, busiest first."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT queue,
                    count(*) FILTER (WHERE state = 'queued' AND available_at <= now()) AS available,
                    count(*) FILTER (WHERE state = 'queued' AND available_at > now())  AS scheduled,
                    count(*) FILTER (WHERE state = 'leased')                           AS in_flight,
                    count(*) FILTER (WHERE state = 'done')                             AS done,
                    count(*) FILTER (WHERE state = 'dead')                             AS dead,
                    COALESCE(EXTRACT(EPOCH FROM (now() - min(available_at)
                        FILTER (WHERE state = 'queued' AND available_at <= now()))), 0) AS oldest
                FROM tasks
                GROUP BY queue
                """
            )
            rows = [
                {"queue": q, "available": a, "scheduled": s, "in_flight": f,
                 "done": d, "dead": x, "oldest_available_age_s": float(o)}
                for q, a, s, f, d, x, o in cur.fetchall()
            ]
        # Busiest first. Sorted here rather than in SQL because an ORDER BY over
        # two aggregates would have to repeat both FILTER clauses in full.
        rows.sort(key=lambda r: (-(r["available"] + r["in_flight"]), r["queue"]))
        return rows

    def check_invariants(self) -> list[str]:
        """Assertions that must hold at every instant. Used by the chaos suite."""
        violations: list[str] = []
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM tasks WHERE state = 'leased' "
                "AND (leased_by IS NULL OR leased_until IS NULL)"
            )
            if cur.fetchone()[0]:
                violations.append("a leased task has no holder or no expiry")

            cur.execute(
                "SELECT count(*) FROM tasks WHERE state <> 'leased' "
                "AND (leased_by IS NOT NULL OR leased_until IS NOT NULL)"
            )
            if cur.fetchone()[0]:
                violations.append("a task that is not leased still carries lease fields")

            cur.execute("SELECT count(*) FROM tasks WHERE state = 'done' AND completed_at IS NULL")
            if cur.fetchone()[0]:
                violations.append("a done task has no completion time")

            cur.execute("SELECT count(*) FROM tasks WHERE state = 'dead' AND dead_at IS NULL")
            if cur.fetchone()[0]:
                violations.append("a dead task has no death time")

            cur.execute("SELECT count(*) FROM tasks WHERE attempts > max_attempts")
            if cur.fetchone()[0]:
                violations.append("a task was delivered more times than its attempt budget allows")
        return violations
