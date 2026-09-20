# Tether

[![tests](https://github.com/keerthishree20/tether/actions/workflows/ci.yml/badge.svg)](https://github.com/keerthishree20/tether/actions/workflows/ci.yml)

A durable job queue on Postgres. Tasks stay tethered to a worker only for as long
as its lease holds, and not one moment longer.

Nothing tells a queue that a worker has died. There is no message, no callback,
no exception. The worker simply stops renewing its lease, and the queue notices
by absence. Everything else in this project follows from that one fact.

---

## What it guarantees

- **At-least-once delivery.** Every enqueued task runs to completion at least
  once, even if workers are killed mid-task or Postgres restarts underneath them.
- **Effectively-once effects**, for handlers that claim an idempotency key. The
  claim and the acknowledgement commit in one transaction, so a redelivery finds
  the claim taken and does nothing.
- **Bounded waiting.** A queued task cannot be starved forever by higher-priority
  work; the starvation boost raises its effective priority the longer it waits.
- **No silent loss.** A task that exhausts its attempts is parked in the
  dead-letter queue with its final error, never dropped.

## What it does not guarantee

- **Exactly-once delivery.** Nobody can offer that across a network boundary. If
  a worker performs an external side effect and dies before acknowledging, the
  work will be delivered again. Idempotent handlers are the answer, not a
  stronger delivery promise.
- **Global ordering.** Priority and age order the queue. Two tasks leased at the
  same moment can finish in either order.

---

## Task lifecycle

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

The edge that matters is the one going back to `QUEUED`. It is not triggered by
anything the dying worker does. A sweeper finds a `leased_until` in the past and
returns the row.

---

## Quick start

```bash
make db          # Postgres 16 in Docker on port 5433
make install     # .venv with psycopg and pytest
make migrate     # create the schema
make test        # 73 tests, chaos suite included
```

Then push some work through it:

```bash
.venv/bin/python -m tether.cli enqueue echo '{"hello":"world"}'
.venv/bin/python -m tether.cli enqueue charge '{"account":"acct_1","amount_cents":2500}'
.venv/bin/python -m tether.cli enqueue always_fails '{"reason":"boom"}' --max-attempts 2

.venv/bin/python -m tether.cli work --concurrency 4 --with-reaper --idle-exit-after 3
.venv/bin/python -m tether.cli stats
.venv/bin/python -m tether.cli dlq list
```

Port 5433 is deliberate. A Postgres already listening on 5432 is common, and a
silent fallback to it produces a `role does not exist` error that reads like a
bug in this code. Override with `TETHER_DSN` if you have your own server.

Python 3.12 is required and is not always what `python3` points at. The Makefile
uses `python3.12` explicitly for that reason.

---

## How it works

### Leasing

One statement claims a task and marks it in flight:

```sql
WITH picked AS (
    SELECT id FROM tasks
    WHERE queue = %(queue)s AND state = 'queued' AND available_at <= now()
    ORDER BY (priority + boost) DESC, available_at
    FOR UPDATE SKIP LOCKED
    LIMIT ...
)
UPDATE tasks t
SET state = 'leased', leased_by = ..., leased_until = now() + ..., attempts = attempts + 1
FROM picked WHERE t.id = picked.id
RETURNING t.*
```

`SKIP LOCKED` is what makes many workers possible. Without it, the second worker
to arrive blocks on the first worker's row lock and the pool serialises.

`attempts` increments here, on delivery, not on failure. A task killed mid-run
has genuinely been delivered once, and the attempt budget should reflect that.

### The fence

`(id, attempts)` identifies one delivery. Every acknowledgement, failure and
heartbeat carries it, and the update matches on it:

```sql
WHERE id = %s AND attempts = %s AND state = 'leased' AND leased_by = %s
```

Without this, the sequence that corrupts the queue is easy to reach. A worker
stalls, its lease expires, the reaper redelivers, a second worker starts the
work, and then the first worker wakes up and acknowledges. The fence makes that
acknowledgement a no-op, which the worker reports as a lost lease.

### The reaper

Sweeps for `state = 'leased' AND leased_until < now()`, returns those tasks to
the queue with a jittered backoff, and parks the ones that have used their last
attempt. Several reapers can run at once; the sweep takes `FOR UPDATE SKIP
LOCKED` on the rows it claims.

It also keeps the starvation boost current.

### Heartbeats

A worker renews the leases of everything it is running, from a dedicated thread.
This distinguishes the two cases that look identical from outside:

| Situation | Heartbeats | Outcome |
|---|---|---|
| Worker busy on a long task | still sent | lease renewed, work continues |
| Worker process frozen or wedged | stop | lease lapses, task redelivered |

Holding a lease should require making progress, not merely existing. The chaos
suite tests this with `SIGSTOP`, which freezes the heartbeat thread along with
everything else.

### Idempotency

Two halves, and both are needed.

**Producer side** is a unique index on `(queue, idempotency_key)`. A retried API
call cannot create a second task. Enforced by the index rather than by a
read-then-write, which two concurrent producers could both pass.

**Consumer side** is the `effects` ledger. A handler claims a key before doing
anything with an external effect:

```python
@register("charge")
def charge(ctx):
    if not ctx.claim_effect(f"charge:{ctx.task.id}"):
        return                      # an earlier delivery already did this
    ctx.conn.execute("INSERT INTO charges ...")
```

The claim, the effect, and the acknowledgement are one transaction. The ordering
is not cosmetic. If the acknowledgement could commit without the claim, a later
redelivery would repeat the effect. If the claim commits and the acknowledgement
does not, the whole transaction rolls back and the retry is free to try again.

`charge_unsafe` is kept in the codebase deliberately: the same handler with no
claim, and a test that proves it double-charges the moment a task is delivered
twice. It is the clearest statement of what the ledger buys.

**Replaying a dead letter really does re-run the work.** This is the question
the ledger design invites, so it is worth stating and it is tested both ways. A
claim commits only inside the acknowledgement's transaction, so a task that
never succeeded never left one behind, and its replay collides with nothing. A
claim keyed on the business fact rather than the task row is shared on purpose:
two separate tasks carrying the same key produce one effect, which is what a
producer that retried without an idempotency key needs.

### Scheduling

- **Delays** are an `available_at` timestamp, so "retry in an hour" needs no
  separate mechanism from "run now".
- **Priority** orders the queue, highest first, ties broken oldest first.
- **Starvation** is bounded by `boost`, a stored integer the reaper raises the
  longer a task waits. Stored rather than computed, so the index still serves the
  ordering. See the plan below.
- **Concurrency caps** limit in-flight work per queue, applied inside the lease
  statement.

---

## Measured

Everything in this table was run on the machine described below and is
reproducible with `make bench` and `make bench-recovery`. Nothing here is a
target.

**Hardware.** Intel Core i5-11320H, 8 threads, 15 GB RAM, Linux 6.8. Postgres
16.13 in Docker (`postgres:16-alpine`), Python 3.12.13. Workers and database on
the same laptop, so these numbers include no network hop and no dedicated
database host.

| Metric | Measured | Conditions |
|---|---:|---|
| Sustained throughput | 3,245 tasks/s | offered 5,599/s for 15 s, 4 worker processes × 8 slots, batch 20 |
| End-to-end p50 | 50.5 ms | offered 2,000/s for 20 s, same pool |
| End-to-end p95 | 209.9 ms | as above |
| End-to-end p99 | 433.5 ms | as above |
| End-to-end max | 636.0 ms | as above |
| Redelivery after `SIGKILL` | 1,985 ms | 2,000 ms lease TTL, 1,000 ms sweep interval |
| Database back after restart | 1.19 s | `docker restart` under a live pool |
| Throughput restored | 1.24 s | 3,000 tasks in flight, 0 lost, 0 dead |
| Empty lease ratio | 0.38 | at 2,000/s, batch 20 |
| Lease query | 0.14 ms | 50,000 queued rows, 26 shared buffers |

Latency is measured end to end, from `enqueued_at` to `completed_at`. Both are
database timestamps, so the figure does not depend on the producer and the
workers agreeing about the time.

Some honest reading of these numbers:

- **Throughput is bounded by Postgres round trips, not by the handlers.** The
  `echo` handler does nothing. A real handler that calls a downstream service
  will hit that service's limit long before it hits this one.
- **The p99 tail comes from the producer.** The load generator inserts in chunks
  of 200, so each chunk briefly builds a small backlog. A smoother producer would
  narrow the tail.
- **Redelivery cannot beat the lease TTL.** 1,985 ms against a 2,000 ms TTL is
  the floor, not a tuning achievement. Nothing can know a worker is gone before
  its lease could plausibly have been renewed. Shorter TTLs redeliver faster and
  reclaim healthy-but-slow workers more often.
- **The empty lease ratio is not pure contention.** It counts every lease that
  returned nothing, which at the tail of a run includes an idle queue. Treat it
  as an upper bound.

### Why the lease query stays fast

```
Limit  (cost=0.29..10.53 rows=20) (actual time=0.065..0.099 rows=20 loops=1)
  ->  LockRows  (actual time=0.063..0.094 rows=20 loops=1)
        ->  Index Scan using tasks_ready on tasks  (actual time=0.038..0.053 rows=20)
              Index Cond: ((queue = 'explain'::text) AND (available_at <= now()))
              Filter: (state = 'queued'::task_state)
              Buffers: shared hit=6
Execution Time: 0.143 ms
```

No sort node, with 50,000 rows eligible. That is the payoff for storing `boost`
instead of computing the starvation adjustment in the `ORDER BY`. An expression
over `now()` there cannot use an index, so every poll would sort the entire
eligible set, and the cost would show up in a benchmark looking exactly like
lease contention. Reproduce with `make plan`.

---

## The chaos suite

Anyone can write a green test suite. These are the ones worth having.

| Failure | What is asserted |
|---|---|
| Worker `SIGKILL`ed mid-task | redelivered after the lease lapses, attempt count rises by exactly one, another worker finishes it |
| Worker `SIGSTOP`ped | heartbeats stop, lease lapses, task redelivered while the process is still alive |
| Worker `SIGTERM`ed | finishes the task in hand and exits; nothing needs redelivering |
| Postgres restarted under a live pool | workers reconnect on their own, all 40 tasks complete, none lost, no worker exits |
| Duplicate delivery | the ledger absorbs it, one charge row; the unsafe handler produces two |
| Rolled-back delivery | no claim and no charge survive; the retry succeeds cleanly |
| Poison task | parked after its attempt budget rather than cycling forever |
| Clock skew | expiry is judged by the database clock, and a structural test fails the build if a local clock is ever used to write a lease timestamp |
| Producer outruns workers | depth and oldest-task age both climb before anything breaks |
| Continuous disruption | invariants hold on every check while workers fail, retry and are killed |

Two invariants are checked throughout: no task is ever both leased and
available, and no task is delivered more times than its attempt budget allows.
`tether check` runs them by hand.

```
$ .venv/bin/python -m pytest -q
........................................................................ [ 98%]
.                                                                        [100%]
73 passed in 33.31s
```

72 of those run in CI. The Postgres restart test needs a container it can
restart by name, and a GitHub Actions service container is not one, so it skips
there and runs locally.

---

## Command line

```
tether migrate                          create the schema
tether enqueue <kind> '<json>'          add a task
    --queue --priority --delay-ms --max-attempts --key --count
tether work                             run a worker pool
    --queue --concurrency --batch --cap --with-reaper --idle-exit-after
tether reap [--once]                    reclaim expired leases
tether stats [--queue] [--json]         current gauges
tether dashboard [--port 9464]          live dashboard, /metrics and /api/stats
tether dlq list | replay [ids] | purge  operate on the dead-letter queue
tether check                            verify queue invariants
```

### Dashboard

`tether dashboard` serves three things off one port, all from the standard
library with no dependency and no build step:

| Route | What it is |
|---|---|
| `/` | live dashboard, polling once a second |
| `/metrics` | Prometheus text format, for a real scrape |
| `/api/stats` | the same reading as JSON, which is what the page polls |

The page shows the five state counts and the oldest wait as tiles, completion
rate and oldest wait as live line charts over the last two minutes, a per-queue
breakdown, and the dead-letter queue with each task's final error. Refresh
interval, pause, and a table view of the raw samples are in the header. It reads
in light and dark, and the oldest-wait tile carries a status pill with an icon
and a word, never colour alone.

Completion rate is derived on the page by differencing the completed count
between polls. Readings that arrive less than a quarter second apart are held
over rather than divided, since a near-zero interval turns a normal batch into a
spike that rescales the whole chart.

![Tether dashboard](docs/dashboard.png)

The same page in dark mode is at [docs/dashboard-dark.png](docs/dashboard-dark.png).

### Metrics

`GET /metrics` serves Prometheus text format from the standard library, no
dependency:

```
tether_tasks_available 41
tether_tasks_scheduled 3
tether_tasks_in_flight 8
tether_tasks_done 1204
tether_tasks_dead 2
tether_oldest_available_age_seconds 0.42
```

Alert on the last one. Depth alone cannot tell a busy queue from a stuck one: a
queue holding ten thousand tasks and draining them in seconds is healthy, and a
queue holding four that nobody has touched for an hour is not.

---

## Configuration

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

A heartbeat interval at or above the lease TTL is rejected at startup rather
than allowed to quietly reclaim healthy workers' tasks.

---

## Decisions

**Postgres, not a storage engine of my own.** The interesting problem here is
lease and retry semantics, not durable storage. `SKIP LOCKED` is also what a
large number of production queues actually run on, so this is a defensible
choice rather than a shortcut.

**Full jitter, not equal jitter.** The failure being defended against is a
thundering herd. A thousand tasks that failed against the same downstream at the
same moment must not return at the same moment, and drawing uniformly across the
whole window spreads them best.

**A concurrency cap costs lease parallelism.** Counting in-flight work and
consuming a slot has to be one step. Two workers that both read "one in flight,
cap of four" will both take three, and the cap is breached without either doing
anything wrong. Capped queues therefore serialise leasing behind an advisory
lock. Uncapped queues keep the full `SKIP LOCKED` parallelism, so the cost is
paid only where it was asked for.

**The cap is enforced inside the lease statement.** Leasing first and checking
afterwards would leave a lease held on work the worker is not allowed to start,
stalling that task for a full TTL and, with enough workers, wedging the pool.

**Threads, not asyncio.** psycopg releases the interpreter lock during network
waits, one connection per thread makes the transaction boundaries obvious, and
the concurrency story is easier to explain. An async rewrite would raise the
ceiling on connection count, not on correctness.

**Dead letters live in the tasks table.** A separate table would duplicate every
column and make replay a cross-table move rather than a state change.

---

## Not built

Named because leaving them out was a choice, not an oversight.

- **`LISTEN` / `NOTIFY` wakeups.** Workers poll. Notification would cut idle
  latency below the poll interval, at the cost of a second failure mode to
  handle when notifications are missed.
- **Cross-queue priority.** Priority orders within a queue only.
- **Sharding.** Lease contention on the hot row set is what breaks first as this
  scales, and sharding by queue is the next move. Not needed at these volumes.
- **Recurring or cron tasks.** Delayed tasks are the primitive a scheduler would
  be built on, but the scheduler is not here.
- **Authentication on the dashboard.** It is a local operator tool bound to one
  port, with no mutating routes. Anything exposed beyond localhost needs a proxy
  in front of it.
- **Alerting.** The gauges are there and `/metrics` is scrapeable, but the rules
  belong in Prometheus, not here.

## Layout

```
tether/
  schema.sql      tables, indexes, and the constraint that keeps lease fields honest
  config.py       environment settings, validated at startup
  db.py           connections, migration, reconnect-with-backoff
  queue.py        enqueue, lease, ack, fail, reclaim, dead letters, gauges
  worker.py       the pool, the heartbeat thread, graceful shutdown
  reaper.py       the sweep
  backoff.py      full-jitter exponential backoff
  metrics.py      Prometheus text, the JSON snapshot, and the HTTP server
  dashboard.html  the dashboard page: no build step, no dependencies
  cli.py          the command line
  handlers/       the registry, the context, and the example handlers
bench/
  loadgen.py      throughput and end-to-end latency
  recovery.py     redelivery latency and restart recovery
  explain.py      the query plan for the lease statement
tests/            73 tests, including the chaos suite
docs/             dashboard screenshots, light and dark
```
