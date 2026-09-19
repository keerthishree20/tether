# Tether — Complete Project Guide

A complete guide from zero to a running, failure-tested job queue. Covers every feature, every design
decision and the reason behind it, with the real code. It is self-contained: you can paste it into
any AI chat and ask questions about the project without sharing the repository.

**Repository:** https://github.com/keerthishree20/tether

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Tech Stack & Why](#2-tech-stack--why)
3. [Project Setup from Scratch](#3-project-setup-from-scratch)
4. [Core Ideas in Plain Words](#4-core-ideas-in-plain-words)
5. [Project Structure](#5-project-structure)
6. [Database Design](#6-database-design)
7. [Enqueueing Tasks](#7-enqueueing-tasks)
8. [Leasing: How Workers Claim Tasks](#8-leasing-how-workers-claim-tasks)
9. [The Fence: Stopping Stale Workers](#9-the-fence-stopping-stale-workers)
10. [Acknowledging and Failing](#10-acknowledging-and-failing)
11. [Heartbeats](#11-heartbeats)
12. [The Reaper](#12-the-reaper)
13. [Retry Backoff with Full Jitter](#13-retry-backoff-with-full-jitter)
14. [Idempotency (Effectively-Once Effects)](#14-idempotency-effectively-once-effects)
15. [Priority and Starvation Boost](#15-priority-and-starvation-boost)
16. [Concurrency Caps](#16-concurrency-caps)
17. [Dead-Letter Queue](#17-dead-letter-queue)
18. [The Worker Pool](#18-the-worker-pool)
19. [Writing Your Own Handler](#19-writing-your-own-handler)
20. [Dashboard & Metrics](#20-dashboard--metrics)
21. [Command Line](#21-command-line)
22. [Configuration](#22-configuration)
23. [Testing & the Chaos Suite](#23-testing--the-chaos-suite)
24. [Benchmarks & Results](#24-benchmarks--results)
25. [Design Decisions](#25-design-decisions)
26. [Deliberately Not Built](#26-deliberately-not-built)
27. [Troubleshooting](#27-troubleshooting)
28. [Complete Feature Summary](#28-complete-feature-summary)

---

## 1. Project Overview

Tether is a **durable job queue built on Postgres**. A program adds a task ("send this email",
"charge this card"). A pool of workers picks tasks up and runs them. Tether guarantees every task
finishes at least once, even when workers are killed mid-task or Postgres restarts underneath them.

The whole design follows from one fact: **nothing tells a queue that a worker has died.** There is no
message, no callback, no exception. A worker holds a task only while it keeps renewing a *lease*. When
renewals stop, a sweeper notices the lease has expired and hands the task to someone else.

### What it guarantees
| Guarantee | Meaning |
|---|---|
| At-least-once delivery | every task runs to completion at least once |
| Effectively-once effects | handlers that claim an idempotency key produce their effect once |
| Bounded waiting | low-priority tasks cannot starve forever |
| No silent loss | a task that runs out of attempts is parked with its error, never dropped |

### What it does not guarantee
- **Exactly-once delivery.** Nobody can offer that across a network. Idempotent handlers are the answer.
- **Global ordering.** Two tasks leased at the same moment can finish in either order.

**Status:** complete. 73 tests pass, including a chaos suite. Public on GitHub with CI.

---

## 2. Tech Stack & Why

| Technology | Role | Why We Chose It |
|---|---|---|
| **Python 3.12** | Language | Clear enough to show every transaction boundary |
| **Postgres 16** | Storage and coordination | `FOR UPDATE SKIP LOCKED` is what many production queues actually run on. The interesting problem is lease semantics, not writing a storage engine |
| **psycopg 3** | Database driver | Releases the interpreter lock during network waits, so threads work well |
| **Threads** | Worker concurrency | One connection per thread makes transaction boundaries obvious |
| **Docker** | Local Postgres | One command gives everyone the same database on port 5433 |
| **http.server** (stdlib) | Dashboard and metrics | No dependency and no build step |
| **pytest** | Tests | Including a chaos suite that kills real processes |

---

## 3. Project Setup from Scratch

### Step 1: Get the code
```bash
git clone https://github.com/keerthishree20/tether.git
cd tether
```

### Step 2: Start Postgres and install
```bash
make db          # Postgres 16 in Docker, container "tether-pg", on port 5433
make install     # .venv with psycopg and pytest (uses python3.12; python3 here is 3.6)
make migrate     # create the schema
make test        # the full suite, chaos tests included
```

Port 5433 is deliberate. Another Postgres often owns 5432, and connecting to it by mistake gives a
confusing `role does not exist` error.

### Step 3: Push work through it
```bash
.venv/bin/python -m tether.cli enqueue echo '{"hello":"world"}'
.venv/bin/python -m tether.cli enqueue charge '{"account":"acct_1","amount_cents":2500}'
.venv/bin/python -m tether.cli enqueue always_fails '{"reason":"boom"}' --max-attempts 2
.venv/bin/python -m tether.cli work --concurrency 4 --with-reaper --idle-exit-after 3
.venv/bin/python -m tether.cli stats
.venv/bin/python -m tether.cli dlq list
.venv/bin/python -m tether.cli dashboard      # http://localhost:9464
```

---

## 4. Core Ideas in Plain Words

| Idea | Meaning |
|---|---|
| **Task** | one unit of work: a kind (which handler), a JSON payload, a queue name |
| **Lease** | a time-limited claim on a task by one worker |
| **Attempt** | one delivery. The count goes up when a task is *delivered*, not when it fails |
| **Fence** | the pair `(id, attempts)`, which names exactly one delivery |
| **Heartbeat** | a worker renewing the leases of everything it is running |
| **Reaper** | the sweep that returns expired leases to the queue |
| **Dead letter** | a task that used every attempt, kept with its final error |

### The task lifecycle
```
                 lease(ttl)                    ack
   ┌──────────┐ ───────────► ┌──────────┐ ───────────► ┌──────────┐
   │  QUEUED  │              │  LEASED  │              │   DONE   │
   └──────────┘ ◄─────────── └──────────┘              └──────────┘
        ▲   lease expired          │
        │   backoff, attempt+1     │  attempts exhausted
        └──────────────────────────┤
                                   ▼
                            ┌──────────────┐
                            │ DEAD LETTER  │
                            └──────────────┘
```

The important arrow goes back to `QUEUED`. It is not triggered by anything the dying worker does.
The reaper finds a `leased_until` in the past and returns the row.

### Every timestamp comes from the database
Workers can disagree about what time it is. So every time is `now()` inside Postgres. A structural
test fails the build if a worker's local clock is ever used to write a lease time.

---

## 5. Project Structure

```
tether/
  schema.sql      tables, indexes, and the constraint that keeps lease fields honest
  config.py       environment settings, validated at startup
  db.py           connections, migration, reconnect with backoff
  queue.py        Client: enqueue, lease, heartbeat, ack, fail, reclaim, dead letters, stats
  worker.py       Worker: executor threads, heartbeat thread, graceful shutdown
  reaper.py       the sweep
  backoff.py      full-jitter exponential backoff
  metrics.py      Prometheus text, JSON snapshot, the dashboard HTTP server
  dashboard.html  the live dashboard page, no build step
  cli.py          the command line
  handlers/
    __init__.py   register() decorator, Context, claim_effect()
    demo.py       example handlers: echo, sleep, flaky, charge, charge_unsafe, hang ...
bench/
  loadgen.py      throughput and end-to-end latency
  recovery.py     redelivery after SIGKILL, recovery after a Postgres restart
  explain.py      the query plan for the lease statement
tests/            73 tests including the chaos suite
docs/             dashboard screenshots, light and dark
Makefile  docker-compose.yml  pyproject.toml  requirements*.txt  .github/workflows/ci.yml
```

---

## 6. Database Design

Defined in `tether/schema.sql`. Running `make migrate` twice is safe.

### The `tasks` table
```sql
CREATE TABLE IF NOT EXISTS tasks (
    id              bigserial PRIMARY KEY,
    queue           text        NOT NULL,
    kind            text        NOT NULL,
    payload         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    state           task_state  NOT NULL DEFAULT 'queued',   -- queued | leased | done | dead
    priority        smallint    NOT NULL DEFAULT 0,
    boost           smallint    NOT NULL DEFAULT 0,          -- starvation guard
    attempts        int         NOT NULL DEFAULT 0,
    max_attempts    int         NOT NULL DEFAULT 5,
    available_at    timestamptz NOT NULL DEFAULT now(),
    leased_until    timestamptz,
    leased_by       text,
    lease_ttl_ms    int         NOT NULL DEFAULT 30000,
    idempotency_key text,
    last_error      text,
    enqueued_at     timestamptz NOT NULL DEFAULT now(),
    completed_at    timestamptz,
    dead_at         timestamptz,
    CONSTRAINT lease_fields_agree CHECK (
        (state = 'leased' AND leased_until IS NOT NULL AND leased_by IS NOT NULL)
        OR (state <> 'leased' AND leased_until IS NULL AND leased_by IS NULL)
    )
);
```

### Why the `lease_fields_agree` constraint?
A row can never be leased without an owner and an expiry, or keep them after leaving the leased
state. The database enforces it, not just the code.

### Indexes
| Index | Columns | Used by |
|---|---|---|
| `tasks_idempotency` | unique `(queue, idempotency_key)` where key is set | producer idempotency |
| `tasks_ready` | `(queue, (priority + boost) DESC, available_at)` where `state = 'queued'` | the lease query |
| `tasks_expiring` | `(leased_until)` where `state = 'leased'` | the reaper |
| `tasks_in_flight` | `(queue)` where `state = 'leased'` | counting for concurrency caps |

The partial indexes keep done and dead rows out, so they stay small however much history builds up.

### `effects` and `charges`
- `effects(effect_key PRIMARY KEY, task_id, recorded_at)`: the consumer-side idempotency ledger.
- `charges`: a demo table the example handlers write to, so tests can count how many times an effect
  really happened.

---

## 7. Enqueueing Tasks

```python
from tether.queue import Client
client.enqueue("charge", {"account": "acct_1", "amount_cents": 2500},
               queue="payments", priority=5, delay_ms=0, max_attempts=5,
               idempotency_key="order-991")
```

The insert uses `ON CONFLICT ... DO NOTHING` on the idempotency index, then returns the existing task
if the key was a repeat:

```python
INSERT INTO tasks (queue, kind, payload, priority, available_at, max_attempts, lease_ttl_ms, idempotency_key)
VALUES (..., now() + make_interval(secs => %(delay_s)s), ...)
ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL
DO NOTHING
RETURNING ...
```

### Why a unique index instead of "check, then insert"?
Two producers retrying at the same moment could both see "no such key" and both insert. The index
makes that impossible.

### Delays
A delay is just a later `available_at`. "Retry in an hour" and "run now" use the same mechanism.

---

## 8. Leasing: How Workers Claim Tasks

One SQL statement picks tasks and marks them leased:

```sql
WITH picked AS (
    SELECT id FROM tasks
    WHERE queue = %(queue)s AND state = 'queued' AND available_at <= now()
    ORDER BY (priority + boost) DESC, available_at
    FOR UPDATE SKIP LOCKED
    LIMIT %(batch)s
)
UPDATE tasks t
SET state = 'leased', leased_by = %(worker)s,
    leased_until = now() + make_interval(secs => %(ttl_s)s),
    attempts = t.attempts + 1, boost = 0
FROM picked WHERE t.id = picked.id
RETURNING ...
```

### Why `SKIP LOCKED`?
Without it, the second worker to arrive waits on the first worker's row lock, and the whole pool
works one at a time. With it, each worker skips rows another worker is taking and grabs the next.

### Why does `attempts` go up here?
A task killed mid-run *was* delivered once. The attempt budget should count that.

### Why the query stays fast
The `ORDER BY` matches the `tasks_ready` index exactly, so Postgres reads rows in order with no sort.
With 50,000 queued rows the lease query took 0.14 ms. Reproduce with `make plan`.

---

## 9. The Fence: Stopping Stale Workers

`(id, attempts)` identifies one delivery. Every ack, failure and heartbeat must match it:

```sql
WHERE id = %s AND attempts = %s AND state = 'leased' AND leased_by = %s
```

### The bug it prevents
1. Worker A stalls. Its lease expires.
2. The reaper redelivers the task. Worker B starts it (attempts is now higher).
3. Worker A wakes up and tries to acknowledge.

Without the fence, A's late ack would mark B's delivery done. With the fence, A's update matches no
row. A sees `rowcount == 0`, reports `LeaseLost`, and its transaction rolls back.

---

## 10. Acknowledging and Failing

### Ack
```python
def ack(self, task: Task, worker_id: str) -> bool:
    task_id, attempts = task.fence
    ... UPDATE tasks SET state = 'done', completed_at = now(), leased_until = NULL, leased_by = NULL
        WHERE id = %s AND attempts = %s AND state = 'leased' AND leased_by = %s
    return cur.rowcount == 1       # False means the lease was already lost
```

### Fail
When a handler raises, `fail()` either returns the task to the queue after a jittered backoff, or
marks it `dead` if this delivery used the last attempt. The error text is saved in `last_error`.

---

## 11. Heartbeats

A dedicated thread in each worker renews the leases of every task that worker is running:

```python
def _heartbeat(self) -> None:
    interval = self.settings.heartbeat_ms / 1000.0
    while not self._stop.wait(interval):
        fences = list(self._inflight.values())
        if fences:
            renewed = client.heartbeat(self.worker_id, fences)
            if renewed < len(fences):
                self._emit("lease_expired_under_us", ...)
```

### Why this matters
It tells apart two situations that look identical from outside:

| Situation | Heartbeats | Outcome |
|---|---|---|
| worker busy on a long task | still sent | lease renewed, work continues |
| worker process frozen or wedged | stop | lease lapses, task redelivered |

Holding a lease should require making progress, not merely existing. The chaos suite tests this with
`SIGSTOP`, which freezes the heartbeat thread along with everything else.

A heartbeat interval at or above the lease TTL is rejected at startup.

---

## 12. The Reaper

```python
def reclaim_expired(self, *, batch: int = 500):
    SELECT id, attempts, max_attempts FROM tasks
    WHERE state = 'leased' AND leased_until < now()
    ORDER BY leased_until
    FOR UPDATE SKIP LOCKED LIMIT %s
    # attempts left  -> back to 'queued' with a jittered delay
    # none left      -> 'dead', with last_error explaining why
```

- It is the **only** mechanism that notices a dead worker, and it notices by absence.
- Several reapers can run at once, because the sweep uses `SKIP LOCKED`.
- It also refreshes the starvation boost (section 15).
- Run it inside workers with `--with-reaper`, or on its own with `tether reap`.

---

## 13. Retry Backoff with Full Jitter

```python
def ceiling_ms(attempts, base_ms, cap_ms):
    shift = min(max(attempts, 1) - 1, 32)
    return min(base_ms << shift, cap_ms)           # 500, 1000, 2000 ... capped at 60000

def delay_ms(attempts, base_ms, cap_ms, *, rng=None):
    return int((rng or random).random() * ceiling_ms(attempts, base_ms, cap_ms))
```

### Why full jitter?
The danger is a thundering herd. A thousand tasks that failed against the same downstream service at
the same moment must not all retry at the same moment. Drawing uniformly across the whole window
spreads them best.

The formula lives only in Python. The reaper computes delays here and passes them into one SQL
update, so the formula cannot drift between two copies.

---

## 14. Idempotency (Effectively-Once Effects)

Two halves, and both are needed.

### Producer side
The unique index on `(queue, idempotency_key)`: a retried enqueue returns the original task.

### Consumer side: the effects ledger
A handler claims a key before doing anything with an effect:

```python
@register("charge")
def charge(ctx: Context) -> None:
    key = ctx.task.payload.get("idempotency_key") or f"charge:{ctx.task.id}"
    if not ctx.claim_effect(key):
        return                              # an earlier delivery already charged
    with ctx.conn.cursor() as cur:
        cur.execute("INSERT INTO charges (account, amount_cents, task_id) VALUES (%s, %s, %s)", ...)
```

`claim_effect()` inserts into `effects` with `ON CONFLICT DO NOTHING` and returns whether it won.

### Why the ordering is not cosmetic
The worker runs the handler **and** the ack in one transaction:

```python
with conn.transaction():
    handler(handlers.Context(task=task, conn=conn, client=client, worker_id=self.worker_id))
    if not client.ack(task, self.worker_id):
        raise LeaseLost(...)                # rolls back the claim and the effect too
```

- If the ack could commit without the claim, a redelivery would repeat the effect.
- If the claim commits, the ack commits with it. If anything fails, everything rolls back and the
  retry starts clean.

### The proof: `charge_unsafe`
The same handler with no claim is kept on purpose. A test proves it double-charges the moment a task
is delivered twice. It is the clearest statement of what the ledger buys.

### Replaying a dead letter really re-runs the work
A task that never succeeded never committed a claim, so its replay collides with nothing. Tested.

### Limits
An effect **outside** the database, such as an email or an HTTP call, can still happen twice if the
worker dies after it and before the ack. Key the claim on the business fact, and make the outside
call idempotent where the other side supports it.

---

## 15. Priority and Starvation Boost

- **Priority** orders the queue, highest first, ties broken by oldest `available_at`.
- **Boost** is a stored integer the reaper raises for tasks waiting longer than
  `TETHER_BOOST_AFTER_MS`, up to `TETHER_BOOST_MAX`. The lease query orders by `priority + boost`.

```sql
UPDATE tasks
SET boost = LEAST(%(max)s, (EXTRACT(EPOCH FROM (now() - available_at)) * 1000 / %(after)s)::int)
WHERE state = 'queued' AND available_at <= now() - make_interval(secs => %(after_s)s) ...
```

### Why store the boost instead of computing it?
An expression over `now()` in the `ORDER BY` cannot use an index. Every poll would sort the whole
eligible set, and that cost would look exactly like lease contention in a benchmark.

---

## 16. Concurrency Caps

`tether work --cap 4` limits how many tasks of one queue run at once. The cap is applied **inside**
the lease statement's `LIMIT`, and capped queues take an advisory lock first:

```python
with self.conn.transaction(), self.conn.cursor() as cur:
    cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (queue,))
    cur.execute(sql, params)
```

### Why the advisory lock?
Counting in-flight work and taking a slot must be one step. Two workers that both read "one in
flight, cap of four" would both take three, and the cap would be breached. Uncapped queues skip the
lock and keep full `SKIP LOCKED` parallelism, so the cost is paid only where it was asked for.

### Why inside the statement?
Leasing first and checking afterwards would hold a lease on work the worker may not start, stalling
that task for a full TTL and, with enough workers, wedging the pool.

---

## 17. Dead-Letter Queue

Dead tasks stay in the `tasks` table with `state = 'dead'`, `dead_at` and `last_error`.

```bash
tether dlq list               # dead tasks with their errors
tether dlq replay 41 42       # back to queued with a fresh attempt budget
tether dlq purge              # delete dead tasks
```

A separate table would duplicate every column and make replay a cross-table move rather than a
state change.

---

## 18. The Worker Pool

`Worker(queue=..., concurrency=..., batch=..., concurrency_cap=..., run_reaper=...)`:

| Thread | Job |
|---|---|
| `exec-0` to `exec-N` | each has its own connection; lease, run handler, ack or fail |
| `heartbeat` | renew leases of everything in flight |
| `reaper` (optional) | sweep expired leases |

- **Database lost mid-task:** the worker does not try to record a failure on a dead connection. It
  reconnects with backoff; the lease expires and the reaper redelivers. That is exactly the
  advertised guarantee.
- **SIGTERM or SIGINT:** drain. Each thread finishes its current task, then exits.
- **`--idle-exit-after N`:** exit after the queue has been empty N seconds. Used by tests and
  benchmarks.

---

## 19. Writing Your Own Handler

1. Create `tether/handlers/email.py`:

```python
from . import Context, register

@register("send_welcome")
def send_welcome(ctx: Context) -> None:
    user = ctx.task.payload["user_id"]
    if not ctx.claim_effect(f"welcome:{user}"):
        return
    ...   # send the email
```

2. Import it at the bottom of `tether/handlers/__init__.py`, next to `from . import demo`.
3. Enqueue: `tether enqueue send_welcome '{"user_id": 42}' --key welcome-42`

Rules:
- Raise an exception to fail. The worker records it and retries with backoff.
- Anything written through `ctx.conn` commits with the ack or not at all.

Example handlers in `demo.py`: `echo`, `sleep`, `always_fails`, `flaky`, `fails_until`, `charge`,
`charge_unsafe`, `hang`.

---

## 20. Dashboard & Metrics

`tether dashboard` serves three routes on one port (default 9464), all from the standard library:

| Route | What it is |
|---|---|
| `/` | live dashboard, polling once a second |
| `/metrics` | Prometheus text format |
| `/api/stats` | the same numbers as JSON |

The page shows the five state counts and the oldest wait as tiles, completion rate and oldest wait as
live charts over two minutes, a per-queue table, and the dead-letter queue with errors. It works in
light and dark, and the oldest-wait tile has an icon and a word, never colour alone.

Completion rate is computed on the page from the difference between polls. Readings less than a
quarter second apart are skipped, since a tiny interval turns a normal batch into a spike.

```
tether_tasks_available 41
tether_tasks_scheduled 3
tether_tasks_in_flight 8
tether_tasks_done 1204
tether_tasks_dead 2
tether_oldest_available_age_seconds 0.42
```

### What to alert on
`tether_oldest_available_age_seconds`. Depth alone cannot tell a busy queue from a stuck one.

---

## 21. Command Line

```
tether migrate                          create the schema
tether enqueue <kind> '<json>'          --queue --priority --delay-ms --max-attempts --key --count
tether work                             --queue --concurrency --batch --cap --with-reaper --idle-exit-after
tether reap [--once]                    reclaim expired leases
tether stats [--queue] [--json]         current gauges
tether dashboard [--port 9464]          dashboard, /metrics, /api/stats
tether dlq list | replay [ids] | purge  dead-letter queue
tether check                            verify queue invariants
```

---

## 22. Configuration

Every setting is an environment variable.

| Variable | Default | Meaning |
|---|---:|---|
| `TETHER_DSN` | `postgresql://tether:tether@localhost:5433/tether` | where Postgres is |
| `TETHER_LEASE_TTL_MS` | 30000 | how long a lease survives without renewal |
| `TETHER_HEARTBEAT_MS` | 5000 | renewal interval; must be well below the TTL |
| `TETHER_MAX_ATTEMPTS` | 5 | deliveries before the dead-letter queue |
| `TETHER_BACKOFF_BASE_MS` | 500 | first retry ceiling |
| `TETHER_BACKOFF_CAP_MS` | 60000 | largest retry ceiling |
| `TETHER_REAP_INTERVAL_MS` | 1000 | sweep interval |
| `TETHER_BOOST_AFTER_MS` | 30000 | wait before the starvation boost begins |
| `TETHER_BOOST_MAX` | 10 | largest boost |
| `TETHER_POLL_INTERVAL_MS` | 200 | idle poll interval |

---

## 23. Testing & the Chaos Suite

| File | Covers |
|---|---|
| `test_lease.py` | leasing, fences, `SKIP LOCKED` |
| `test_retry.py`, `test_backoff.py` | retry flow and jitter bounds |
| `test_dead_letter.py` | parking, listing, replay, purge |
| `test_idempotency.py` | both halves, including `charge_unsafe` double-charging |
| `test_scheduling.py` | delays, priority, starvation boost, caps |
| `test_metrics.py`, `test_dashboard.py` | metrics text and the dashboard server |
| `test_chaos.py` | the failures below |

### The chaos suite
| Failure | What is asserted |
|---|---|
| worker `SIGKILL`ed mid-task | redelivered after the lease lapses, attempts rises by exactly one |
| worker `SIGSTOP`ped | heartbeats stop, lease lapses, redelivered while the process still exists |
| worker `SIGTERM`ed | finishes its task and exits; nothing to redeliver |
| Postgres restarted under a live pool | workers reconnect on their own, all 40 tasks complete |
| duplicate delivery | the ledger absorbs it; the unsafe handler charges twice |
| rolled-back delivery | no claim and no charge survive |
| poison task | parked after its budget, not cycling forever |
| clock skew | expiry judged by the database clock |
| producer outruns workers | depth and oldest age climb before anything breaks |
| continuous disruption | invariants hold on every check |

```bash
make test         # all 73
make test-fast    # without chaos
make test-chaos   # only chaos
```

72 run in CI. The Postgres restart test needs a container it can restart by name, so it skips in
GitHub Actions and runs locally.

---

## 24. Benchmarks & Results

Intel Core i5-11320H, 8 threads, 15 GB RAM, Postgres 16 in Docker, Python 3.12. **Workers and
Postgres on the same laptop, so no network hop.** Reproduce with `make bench` and
`make bench-recovery`.

| Metric | Measured | Conditions |
|---|---:|---|
| Sustained throughput | 3,245 tasks/s | offered 5,599/s for 15 s, 4 processes × 8 slots, batch 20 |
| End-to-end p50 | 50.5 ms | offered 2,000/s for 20 s |
| End-to-end p95 | 209.9 ms | same run |
| End-to-end p99 | 433.5 ms | same run |
| Redelivery after SIGKILL | 1,985 ms | 2,000 ms lease TTL, 1,000 ms sweep |
| Database back after restart | 1.19 s | `docker restart` under a live pool |
| Throughput restored | 1.24 s | 3,000 in flight, 0 lost, 0 dead |
| Lease query | 0.14 ms | 50,000 queued rows |

How to read them honestly:
- The throughput and the latency figures come from **different runs**. Never pair 3,245/s with the
  433 ms p99.
- Throughput is bounded by Postgres round trips; the `echo` handler does nothing.
- The p99 tail comes from the producer inserting in chunks of 200.
- Redelivery cannot beat the lease TTL. 1,985 ms against a 2,000 ms TTL is the floor.

---

## 25. Design Decisions

| Decision | Reason |
|---|---|
| Postgres, not a custom store | the interesting problem is leases and retries; `SKIP LOCKED` is production-proven |
| Full jitter, not equal jitter | spreads a thundering herd best |
| Cap costs parallelism only where used | advisory lock only for capped queues |
| Cap inside the lease statement | never hold a lease on work you may not start |
| Threads, not asyncio | clearer transaction boundaries; async would raise connection count, not correctness |
| Dead letters in the tasks table | replay is a state change, not a copy |
| Database clock for every timestamp | workers' clocks disagree |

---

## 26. Deliberately Not Built

| Feature | Why not |
|---|---|
| `LISTEN`/`NOTIFY` wakeups | would cut idle latency but adds a second failure mode when notifications are missed |
| Cross-queue priority | priority orders within a queue only |
| Sharding | not needed at these volumes; sharding by queue is the next step |
| Recurring / cron tasks | delayed tasks are the building block, but no scheduler yet |
| Dashboard authentication | local operator tool; put a proxy in front before exposing it |
| Alerting rules | belong in Prometheus |

---

## 27. Troubleshooting

### `role "tether" does not exist`
You are connected to another Postgres, usually on 5432. Run `make db` and check `TETHER_DSN` uses 5433.

### `make db` fails with a name conflict
`docker rm -f tether-pg`, then `make db` again.

### Tasks stay `leased` forever
No reaper is running and their worker died. Run `tether reap`, or start workers with `--with-reaper`.

### A healthy long task keeps getting redelivered
Heartbeats are not arriving in time. Keep `TETHER_HEARTBEAT_MS` well below `TETHER_LEASE_TTL_MS`, and
make sure the handler is not blocking the whole process.

### The worker logs `LeaseLost`
Another delivery owns the task now; the late ack was correctly discarded.

### `tether check` reports a problem
It should never happen. Capture the output and the task rows before changing anything.

---

## 28. Complete Feature Summary

### All Features Built

| # | Feature | Type | Key Files |
|---|---|---|---|
| 1 | Durable task storage | Database | `schema.sql` |
| 2 | Enqueue with producer idempotency | Queue | `queue.py` |
| 3 | `SKIP LOCKED` leasing | Queue | `queue.py` |
| 4 | Fenced ack and fail | Queue | `queue.py` |
| 5 | Heartbeat lease renewal | Worker | `worker.py` |
| 6 | Reaper for expired leases | Queue | `reaper.py`, `queue.py` |
| 7 | Full-jitter exponential backoff | Utility | `backoff.py` |
| 8 | Consumer idempotency ledger | Handlers | `handlers/__init__.py` |
| 9 | Priority and starvation boost | Scheduling | `queue.py` |
| 10 | Concurrency caps | Scheduling | `queue.py` |
| 11 | Dead-letter queue with replay | Queue | `queue.py`, `cli.py` |
| 12 | Worker pool with graceful drain | Worker | `worker.py` |
| 13 | Automatic reconnect | Worker | `db.py`, `worker.py` |
| 14 | Live dashboard | Ops | `metrics.py`, `dashboard.html` |
| 15 | Prometheus metrics and JSON stats | Ops | `metrics.py` |
| 16 | Command line | Tooling | `cli.py` |
| 17 | Chaos suite | Testing | `tests/test_chaos.py` |
| 18 | Benchmarks and query plan | Tooling | `bench/` |

### Data Flow Architecture

```
Producer
  └── tether enqueue / Client.enqueue() ──► INSERT ... ON CONFLICT DO NOTHING ──► tasks (queued)

Worker process
  ├── exec thread ──► lease: UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED) ──► tasks (leased)
  │       └── one transaction: handler(ctx) + claim_effect() + ack (fenced) ──► tasks (done)
  │               └── handler raises ──► fail(): queued with backoff, or dead
  └── heartbeat thread ──► renew leased_until for every fence in flight

Reaper
  └── leased_until < now() ──► queued (jittered delay) or dead; refresh boost

Dashboard (port 9464)
  ├── /           live page polling /api/stats
  ├── /metrics    Prometheus
  └── /api/stats  JSON
```

### Tech Stack at a Glance

```
Language:   Python 3.12
Database:   Postgres 16 (Docker, port 5433), psycopg 3
Concurrency: threads, one connection each
Ops:        stdlib HTTP server, Prometheus text, single-file dashboard
Testing:    pytest, chaos suite with SIGKILL / SIGSTOP / SIGTERM / Postgres restart
CI:         GitHub Actions with a Postgres service
```
