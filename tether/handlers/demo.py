"""Example handlers.

These exist so the behaviour of the queue can be demonstrated and tested without
a real downstream. The pair worth reading is `charge` and `charge_unsafe`: they
do the same thing, and only one of them survives a redelivery intact.
"""

from __future__ import annotations

import random
import time

from . import Context, register


@register("echo")
def echo(ctx: Context) -> None:
    """Succeeds immediately. The baseline for throughput measurement."""
    return None


@register("sleep")
def sleep(ctx: Context) -> None:
    """Occupies a worker for a while. Used for concurrency and heartbeat tests."""
    time.sleep(float(ctx.task.payload.get("seconds", 1)))


@register("always_fails")
def always_fails(ctx: Context) -> None:
    """The poison task. Exhausts its attempts and lands in the dead-letter queue."""
    raise RuntimeError(ctx.task.payload.get("reason", "this task never succeeds"))


@register("flaky")
def flaky(ctx: Context) -> None:
    """Fails with a given probability. Exercises the retry path under load."""
    if random.random() < float(ctx.task.payload.get("failure_rate", 0.3)):
        raise RuntimeError("transient downstream failure")


@register("fails_until")
def fails_until(ctx: Context) -> None:
    """Fails until the given attempt number, then succeeds.

    Lets a test assert the exact number of deliveries a task needed.
    """
    succeed_on = int(ctx.task.payload.get("succeed_on_attempt", 3))
    if ctx.task.attempts < succeed_on:
        raise RuntimeError(f"attempt {ctx.task.attempts} of {succeed_on}")


@register("charge")
def charge(ctx: Context) -> None:
    """An effect that must happen once, written the safe way.

    The claim and the write share the acknowledgement's transaction, so a
    redelivery finds the claim already taken and does nothing.
    """
    key = ctx.task.payload.get("idempotency_key") or f"charge:{ctx.task.id}"
    if not ctx.claim_effect(key):
        return  # already charged on an earlier delivery
    with ctx.conn.cursor() as cur:
        cur.execute(
            "INSERT INTO charges (account, amount_cents, task_id) VALUES (%s, %s, %s)",
            (ctx.task.payload["account"], int(ctx.task.payload["amount_cents"]), ctx.task.id),
        )


@register("charge_unsafe")
def charge_unsafe(ctx: Context) -> None:
    """The same effect with no claim. Kept deliberately.

    At-least-once delivery means this handler double-charges the moment a
    worker dies between the write and the acknowledgement. The test suite
    proves it, which is the clearest way to show what `charge` is buying.
    """
    with ctx.conn.cursor() as cur:
        cur.execute(
            "INSERT INTO charges (account, amount_cents, task_id) VALUES (%s, %s, %s)",
            (ctx.task.payload["account"], int(ctx.task.payload["amount_cents"]), ctx.task.id),
        )


@register("hang")
def hang(ctx: Context) -> None:
    """Never returns. Used to show a lease lapsing under a live process."""
    while True:
        time.sleep(3600)
