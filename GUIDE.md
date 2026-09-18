# Tether — Complete Project Guide

## Table of Contents
1. [What is Tether?](#what-is-tether)
2. [Quick Start](#quick-start)
3. [Core Concepts](#core-concepts)
4. [Architecture](#architecture)
5. [Database Schema](#database-schema)
6. [Code Walkthrough](#code-walkthrough)
7. [Writing Your Own Handler](#writing-your-own-handler)
8. [Operating a Queue](#operating-a-queue)
9. [Testing Strategy](#testing-strategy)
10. [Benchmarks](#benchmarks)
11. [Troubleshooting](#troubleshooting)

---

## What is Tether?

Tether is a durable job queue built on Postgres. A producer enqueues a task, a pool of workers leases
and runs it, and the queue guarantees the task finishes at least once, even when workers are killed
or Postgres restarts under them.

The whole design follows from one fact: **nothing tells a queue that a worker has died.** A worker
holds a task only while it keeps renewing a lease. When renewals stop, a sweeper notices the lease
has expired and hands the task to someone else.

What it guarantees:
- **At-least-once delivery.** Every task runs to completion at least once.
- **Effectively-once effects**, for handlers that claim an idempotency key.
- **Bounded waiting.** Low-priority tasks cannot starve forever.
- **No silent loss.** A task that runs out of attempts goes to a dead-letter queue with its error.

What it does not guarantee: exactly-once delivery, or global ordering.

---

## Quick Start

Requires Docker and Python 3.12. The system `python3` here is 3.6, so the Makefile calls
`python3.12` directly.

```bash
make db          # Postgres 16 in Docker, container "tether-pg", on port 5433
make install     # .venv with psycopg and pytest
make migrate     # create the schema
make test        # the full suite, chaos tests included
```

Push some work through it:

```bash
.venv/bin/python -m tether.cli enqueue echo '{"hello":"world"}'
.venv/bin/python -m tether.cli enqueue always_fails '{"reason":"boom"}' --max-attempts 2
.venv/bin/python -m tether.cli work --concurrency 4 --with-reaper --idle-exit-after 3
.venv/bin/python -m tether.cli stats
.venv/bin/python -m tether.cli dlq list
.venv/bin/python -m tether.cli dashboard      # http://localhost:9464
```

Port 5433 is deliberate. Another Postgres often owns 5432, and connecting to it by mistake gives a
confusing `role does not exist` error. Point `TETHER_DSN` at your own server to override.

---

## Core Concepts

### Lease
A time-limited claim on a task. Leasing sets `state = 'leased'`, `leased_by` and `leased_until`. If
the worker does not renew before `leased_until`, the task goes back to the queue.

### Attempt and fence
`attempts` increases when a task is delivered, not when it fails. The pair `(id, attempts)` names
one delivery and is called the **fence**. Every ack, failure and heartbeat must match the fence.
This stops a worker that stalled past its lease from acknowledging a task that has since been given
to someone else.

### Heartbeat
A dedicated thread in each worker renews the leases of everything that worker is running. A busy
worker keeps sending heartbeats. A frozen one does not, so its lease lapses.

### Reaper
A sweep that finds leases in the past, returns those tasks to the queue after a jittered backoff,
and moves tasks that used their last attempt to the dead-letter state. It also raises the starvation
`boost` on tasks that have waited too long.

### Idempotency, two halves
- **Producer side.** A unique index on `(queue, idempotency_key)` means a retried enqueue returns
  the original task instead of creating a second one.
- **Consumer side.** The `effects` table. A handler calls `ctx.claim_effect(key)` before an external
  effect. The claim commits in the same transaction as the ack, so a redelivered task finds the
  claim taken and skips the effect.

### Dead letter
The `dead` state. Tasks stay in the same table, so replaying one is a state change, not a copy.

---

## Architecture

```
   producer                       Postgres 16
 ┌──────────┐  enqueue   ┌─────────────────────────────┐
 │ CLI or   │──────────► │ tasks    (queued/leased/    │
 │ Client() │            │           done/dead)        │
 └──────────┘            │ effects  (idempotency)      │
                         │ charges  (demo table)       │
                         └──────▲──────────▲───────────┘
                  lease / ack   │          │ sweep expired leases
                  heartbeat     │          │ raise boost
            ┌───────────────────┴──┐   ┌───┴────────────┐
            │ Worker process        │   │ Reaper         │
            │  exec-0 .. exec-N     │   │ (thread in a   │
            │  heartbeat thread     │   │  worker, or    │
            │  one connection each  │   │  tether reap)  │
            └───────────────────────┘   └────────────────┘
                         │
                 ┌───────▼────────┐
                 │ tether         │  /  dashboard
                 │ dashboard      │  /metrics  Prometheus
                 │ port 9464      │  /api/stats JSON
                 └────────────────┘
```

Every timestamp is produced by the database with `now()`, never by a worker's clock. Workers can
disagree about the time. The row holding the lease is the only clock that matters.

---

## Database Schema

Defined in `tether/schema.sql`. Running `make migrate` twice is safe.

### `tasks`
| column | purpose |
|---|---|
| `id`, `queue`, `kind`, `payload` | identity, which queue, which handler, JSON input |
| `state` | enum `queued`, `leased`, `done`, `dead` |
| `priority`, `boost` | ordering. `boost` is raised by the reaper for tasks that waited too long |
| `attempts`, `max_attempts` | delivery count and budget |
| `available_at` | when the task may next be leased. Delays and backoff both use it |
| `leased_until`, `leased_by`, `lease_ttl_ms` | the lease |
| `idempotency_key`, `last_error` | producer idempotency, final error for dead letters |
| `enqueued_at`, `completed_at`, `dead_at` | timestamps, all from the database clock |

The constraint `lease_fields_agree` makes it impossible for a row to be leased without an owner and
expiry, or to keep them after leaving the leased state.

### Indexes
| index | used by |
|---|---|
| `tasks_idempotency` | unique `(queue, idempotency_key)`, producer idempotency |
| `tasks_ready` | the lease query. Column order matches its `ORDER BY`. Partial on `state = 'queued'` |
| `tasks_expiring` | the reaper's sweep over `leased_until` |
| `tasks_in_flight` | counting leased tasks per queue for the concurrency cap |

### `effects` and `charges`
`effects(effect_key, task_id, recorded_at)` is the consumer-side ledger. `charges` is a demo table
the example handlers write to, so tests can count how many times an effect really happened.

---

## Code Walkthrough

### `tether/queue.py`
`Client(conn, settings)` holds every queue operation.

| method | what it does |
|---|---|
| `enqueue`, `enqueue_many` | insert tasks, honouring the idempotency key |
| `lease` | one `UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED)` statement that claims up to `batch` tasks |
| `heartbeat` | renews many leases at once, each matched on its fence |
| `ack` | marks done, only if the fence still matches |
| `fail` | requeues with backoff, or marks dead if this was the last attempt |
| `reclaim_expired` | the reaper's sweep |
| `apply_starvation_boost` | raises `boost` on long-waiting tasks |
| `dead_letters`, `replay`, `purge_dead` | dead-letter queue operations |
| `stats`, `stats_by_queue` | gauges for the CLI, dashboard and metrics |
| `check_invariants` | returns a list of problems, empty when healthy |

`Task.fence` returns `(id, attempts)`.

### `tether/worker.py`
`Worker(queue=..., concurrency=..., batch=..., concurrency_cap=..., run_reaper=...)`.

- `run()` starts one executor thread per slot, one heartbeat thread, and optionally a reaper thread.
- Each executor has its own database connection, leases tasks, runs the handler and acks or fails.
- A lost database connection is retried with backoff instead of killing the worker.
- `install_signal_handlers()` makes `SIGTERM` and `SIGINT` drain: finish the current task, then
  exit.
- `LeaseLost` is raised when an ack or failure no longer matches the fence.

### `tether/reaper.py`
`Reaper.sweep_once()` runs one sweep. `run_forever()` loops every `TETHER_REAP_INTERVAL_MS`.
Several reapers can run at once because the sweep uses `SKIP LOCKED`.

### `tether/backoff.py`
Full-jitter exponential backoff. `ceiling_ms(attempts, base, cap)` is the window, and
`delay_ms(...)` draws uniformly inside it, so tasks that failed together do not retry together.

### `tether/handlers/`
`register(kind)` is a decorator that adds a function to the registry. `Context` gives a handler
`task`, `conn`, `client`, `worker_id` and `claim_effect()`. The example handlers in `demo.py` are:

| kind | behaviour |
|---|---|
| `echo` | does nothing, used for throughput |
| `sleep` | sleeps, used for long-task and heartbeat tests |
| `always_fails` | always raises, ends in the dead-letter queue |
| `flaky`, `fails_until` | fail some attempts, then succeed |
| `charge` | writes a charge guarded by `claim_effect` |
| `charge_unsafe` | the same without the guard. Kept to prove it double-charges |
| `hang` | never returns, used for frozen-worker tests |

### `tether/metrics.py` and `dashboard.html`
`render()` produces Prometheus text, `snapshot()` produces JSON, and `serve()` runs a standard
library HTTP server with three routes: `/`, `/metrics` and `/api/stats`. The dashboard is one HTML
file with no build step.

### `tether/config.py` and `tether/db.py`
`Settings` reads every `TETHER_*` environment variable and `validate()` rejects a heartbeat interval
at or above the lease TTL. `db.py` handles connections, `migrate()`, and waiting for Postgres to
come back after a restart.

---

## Writing Your Own Handler

1. Create a module in `tether/handlers/`, for example `email.py`:

```python
from . import Context, register

@register("send_welcome")
def send_welcome(ctx: Context) -> None:
    user = ctx.task.payload["user_id"]
    if not ctx.claim_effect(f"welcome:{user}"):
        return                         # an earlier delivery already sent it
    ...                                # the external effect
```

2. Import it at the bottom of `tether/handlers/__init__.py`, next to `from . import demo`, so it
   registers.
3. Enqueue work for it:

```bash
.venv/bin/python -m tether.cli enqueue send_welcome '{"user_id": 42}' --key welcome-42
```

Rules for a safe handler:
- Raise an exception to fail. The worker records the message and retries with backoff.
- Anything written through `ctx.conn` commits together with the ack, or not at all.
- An effect outside the database, such as an email or an HTTP call, can still happen twice if the
  worker dies after it and before the ack. Key the claim on the business fact, and make the external
  call idempotent where the other side supports it.

---

## Operating a Queue

### Command line
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

### Configuration
Every setting is a `TETHER_*` environment variable. The README has the full table with defaults.
The ones you are most likely to change:

| variable | when to change it |
|---|---|
| `TETHER_DSN` | using your own Postgres |
| `TETHER_LEASE_TTL_MS` | tasks legitimately run longer than the TTL between heartbeats |
| `TETHER_HEARTBEAT_MS` | must stay well below the TTL |
| `TETHER_MAX_ATTEMPTS` | how many deliveries before the dead-letter queue |

### What to watch
`tether_oldest_available_age_seconds` is the metric to alert on. Queue depth alone cannot tell a
busy queue from a stuck one.

### Replaying dead letters
`tether dlq replay` really re-runs the work. A task that never succeeded never committed an effect
claim, so its replay does not collide with anything.

---

## Testing Strategy

| file | what it covers |
|---|---|
| `tests/test_lease.py` | leasing, fences, `SKIP LOCKED` behaviour |
| `tests/test_retry.py`, `test_backoff.py` | retry flow and jitter bounds |
| `tests/test_dead_letter.py` | parking, listing, replay, purge |
| `tests/test_idempotency.py` | both halves, including the unsafe handler double-charging |
| `tests/test_scheduling.py` | delays, priority, starvation boost, concurrency caps |
| `tests/test_metrics.py`, `test_dashboard.py` | metrics text and the dashboard server |
| `tests/test_chaos.py` | SIGKILL, SIGSTOP, SIGTERM, Postgres restart, clock skew, continuous disruption |

```bash
make test-fast     # without the chaos suite
make test-chaos    # only the tests that break something
```

The Postgres restart test needs a container it can restart by name. GitHub Actions service
containers are not one, so that test skips in CI and runs locally.

---

## Benchmarks

```bash
make bench            # throughput and end-to-end latency
make bench-saturate   # find the ceiling
make bench-recovery   # redelivery after SIGKILL, recovery after a Postgres restart
make plan             # EXPLAIN for the lease statement
```

Results and the hardware they came from are in the README. Workers and Postgres ran on the same
laptop, so the numbers include no network hop. Quote them with that caveat.

---

## Troubleshooting

### `role "tether" does not exist`
You are connected to a different Postgres, usually one on port 5432. Run `make db` and check
`TETHER_DSN` points at port 5433.

### `make db` fails with a name conflict
A container named `tether-pg` already exists. `make db` removes it first. If that fails,
`docker rm -f tether-pg` and try again.

### Tasks sit in `leased` and never finish
No reaper is running and the worker that held them died. Start one with `tether reap`, or run
workers with `--with-reaper`.

### A healthy long task keeps getting redelivered
Its heartbeats are not arriving in time. Check `TETHER_HEARTBEAT_MS` is well below
`TETHER_LEASE_TTL_MS`, and that the handler is not blocking the whole process.

### The worker logs `LeaseLost`
The fence no longer matched, so another delivery owns the task now. The ack was correctly
discarded. It is expected after a stall longer than the TTL.

### `tether check` reports a problem
It lists invariant violations such as a task that is both leased and available. That should never
happen. Capture the output and the task rows before changing anything.
