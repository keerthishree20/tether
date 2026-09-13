"""Handler registry and the context a handler runs inside."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict

import psycopg

from ..queue import Client, Task

#: A handler is any callable taking a Context. Raising is how it reports failure.
Handler = Callable[["Context"], None]

_REGISTRY: Dict[str, Handler] = {}


class UnknownKind(KeyError):
    pass


def register(kind: str) -> Callable[[Handler], Handler]:
    def decorate(fn: Handler) -> Handler:
        _REGISTRY[kind] = fn
        return fn
    return decorate


def get(kind: str) -> Handler:
    try:
        return _REGISTRY[kind]
    except KeyError:
        raise UnknownKind(
            f"no handler registered for kind {kind!r}. "
            f"known kinds: {', '.join(sorted(_REGISTRY)) or '(none)'}"
        ) from None


def known() -> list[str]:
    return sorted(_REGISTRY)


@dataclass
class Context:
    """What a handler is given.

    `conn` is inside the same transaction that will carry the acknowledgement.
    Anything the handler writes through it commits with the ack or not at all.
    """

    task: Task
    conn: psycopg.Connection
    client: Client
    worker_id: str

    def claim_effect(self, key: str | None = None) -> bool:
        """Reserve an effect key. False means this effect already happened.

        The claim is inserted in the acknowledgement's transaction. That
        ordering is the whole point: if the ack were to commit without the
        claim, a later redelivery would repeat the effect.
        """
        key = key or f"task:{self.task.id}"
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO effects (effect_key, task_id) VALUES (%s, %s) "
                "ON CONFLICT (effect_key) DO NOTHING",
                (key, self.task.id),
            )
            return cur.rowcount == 1


from . import demo  # noqa: E402,F401  (import for its registration side effects)
