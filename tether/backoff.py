"""Retry backoff.

One implementation, in Python. The reaper computes delays here and hands them to
a single set-based UPDATE, rather than duplicating the formula in SQL where it
would quietly drift out of agreement with this file.
"""

from __future__ import annotations

import random

#: Attempts above this contribute nothing further. 2**32 milliseconds is already
#: past any sane cap, and this keeps the shift from growing without bound.
_MAX_SHIFT = 32


def ceiling_ms(attempts: int, base_ms: int, cap_ms: int) -> int:
    """The un-jittered upper bound for the given attempt count."""
    if attempts < 1:
        attempts = 1
    shift = min(attempts - 1, _MAX_SHIFT)
    return min(base_ms << shift, cap_ms)


def delay_ms(attempts: int, base_ms: int, cap_ms: int, *, rng: random.Random | None = None) -> int:
    """Full jitter: a uniform draw from zero up to the exponential ceiling.

    Full jitter rather than equal jitter because the failure this defends
    against is a thundering herd. A thousand tasks that all failed against the
    same downstream at the same moment must not come back at the same moment,
    and spreading them across the whole window does that best.
    """
    rand = (rng or random).random()
    return int(rand * ceiling_ms(attempts, base_ms, cap_ms))
