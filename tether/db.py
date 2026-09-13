"""Connection handling and schema migration.

Deliberately no connection pool. Every worker thread owns one connection for its
whole life, which is what makes `FOR UPDATE ... SKIP LOCKED` reasoning simple:
one thread, one transaction, one lease at a time.
"""

from __future__ import annotations

import pathlib
import time
from typing import Iterator

import psycopg

from .config import Settings, load

SCHEMA_PATH = pathlib.Path(__file__).with_name("schema.sql")


class DatabaseUnavailable(RuntimeError):
    """Raised with an actionable message instead of a raw driver error."""


def connect(settings: Settings | None = None, *, autocommit: bool = True) -> psycopg.Connection:
    settings = settings or load()
    try:
        conn = psycopg.connect(settings.dsn, autocommit=autocommit)
    except psycopg.OperationalError as exc:
        raise DatabaseUnavailable(
            f"could not reach Postgres at {_redact(settings.dsn)}\n"
            f"  driver said: {str(exc).strip()}\n"
            "  start the development database with:  make db\n"
            "  or point TETHER_DSN at your own Postgres."
        ) from exc
    return conn


def wait_until_ready(settings: Settings | None = None, *, timeout_s: float = 30.0) -> psycopg.Connection:
    """Retry the connection with backoff. Used after a database restart."""
    settings = settings or load()
    deadline = time.monotonic() + timeout_s
    delay = 0.1
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return connect(settings)
        except DatabaseUnavailable as exc:
            last = exc
            time.sleep(delay)
            delay = min(delay * 2, 2.0)
    raise DatabaseUnavailable(f"database did not come back within {timeout_s:g}s") from last


def migrate(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_PATH.read_text())
    if not conn.autocommit:
        conn.commit()


def reset(conn: psycopg.Connection) -> None:
    """Truncate every table. Test fixtures only."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE tasks, effects, charges RESTART IDENTITY")
    if not conn.autocommit:
        conn.commit()


def db_now(conn: psycopg.Connection):
    """The database clock. Nothing in Tether reads the worker's clock."""
    with conn.cursor() as cur:
        cur.execute("SELECT now()")
        return cur.fetchone()[0]


def _redact(dsn: str) -> str:
    if "@" not in dsn or "//" not in dsn:
        return dsn
    scheme, rest = dsn.split("//", 1)
    creds, host = rest.split("@", 1)
    user = creds.split(":", 1)[0]
    return f"{scheme}//{user}:***@{host}"


def iter_rows(cur) -> Iterator[dict]:
    columns = [d.name for d in cur.description]
    for row in cur:
        yield dict(zip(columns, row))
