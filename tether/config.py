"""Runtime configuration, read once from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass

# The host in this development environment already runs a Postgres on 5432 that
# Tether has no role on. The container the Makefile starts listens on 5433, and
# that is the default here so a stray connection cannot silently land on the
# wrong server and fail with a confusing authentication error.
DEFAULT_DSN = "postgresql://tether:tether@localhost:5433/tether"


@dataclass(frozen=True)
class Settings:
    dsn: str = os.environ.get("TETHER_DSN", DEFAULT_DSN)

    #: How long a lease is held before the reaper may reclaim it.
    lease_ttl_ms: int = int(os.environ.get("TETHER_LEASE_TTL_MS", "30000"))

    #: How often a worker extends the lease on a task it is still running.
    #: Must be comfortably below lease_ttl_ms or healthy workers lose tasks.
    heartbeat_ms: int = int(os.environ.get("TETHER_HEARTBEAT_MS", "5000"))

    #: Delivery attempts before a task is parked in the dead-letter queue.
    max_attempts: int = int(os.environ.get("TETHER_MAX_ATTEMPTS", "5"))

    #: Retry backoff, before jitter is applied.
    backoff_base_ms: int = int(os.environ.get("TETHER_BACKOFF_BASE_MS", "500"))
    backoff_cap_ms: int = int(os.environ.get("TETHER_BACKOFF_CAP_MS", "60000"))

    #: How often the reaper sweeps for expired leases.
    reap_interval_ms: int = int(os.environ.get("TETHER_REAP_INTERVAL_MS", "1000"))

    #: A queued task waiting longer than this has its boost raised by one, up to
    #: boost_max. This is the starvation guard for low-priority work.
    boost_after_ms: int = int(os.environ.get("TETHER_BOOST_AFTER_MS", "30000"))
    boost_max: int = int(os.environ.get("TETHER_BOOST_MAX", "10"))

    #: Idle poll interval when a queue has nothing available.
    poll_interval_ms: int = int(os.environ.get("TETHER_POLL_INTERVAL_MS", "200"))

    def validate(self) -> None:
        if self.heartbeat_ms >= self.lease_ttl_ms:
            raise ValueError(
                "TETHER_HEARTBEAT_MS must be smaller than TETHER_LEASE_TTL_MS, "
                f"got {self.heartbeat_ms} >= {self.lease_ttl_ms}. A worker that "
                "cannot renew before expiry will have live tasks reclaimed."
            )
        if self.backoff_cap_ms < self.backoff_base_ms:
            raise ValueError("TETHER_BACKOFF_CAP_MS must be at least TETHER_BACKOFF_BASE_MS")


def load() -> Settings:
    settings = Settings()
    settings.validate()
    return settings
