"""Tether: a durable job queue on Postgres.

At-least-once delivery, leases that expire, jittered retries, consumer-side
idempotency, and a dead-letter queue for work that will never succeed.
"""

from .config import Settings, load
from .queue import Client, Task
from .reaper import Reaper
from .worker import LeaseLost, Worker

__version__ = "1.0.0"
__all__ = ["Client", "Task", "Worker", "Reaper", "Settings", "load", "LeaseLost"]
