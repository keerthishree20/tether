"""Backoff maths. The only tests here that need no database."""

from __future__ import annotations

import random

from tether import backoff


def test_ceiling_doubles_per_attempt():
    assert backoff.ceiling_ms(1, 100, 10_000) == 100
    assert backoff.ceiling_ms(2, 100, 10_000) == 200
    assert backoff.ceiling_ms(3, 100, 10_000) == 400
    assert backoff.ceiling_ms(4, 100, 10_000) == 800


def test_ceiling_stops_at_the_cap():
    assert backoff.ceiling_ms(20, 100, 5_000) == 5_000
    assert backoff.ceiling_ms(10_000, 100, 5_000) == 5_000


def test_first_attempt_is_never_below_base():
    for attempts in (-5, 0, 1):
        assert backoff.ceiling_ms(attempts, 250, 10_000) == 250


def test_delay_stays_inside_the_ceiling():
    for attempt in range(1, 12):
        ceiling = backoff.ceiling_ms(attempt, 100, 30_000)
        for _ in range(200):
            delay = backoff.delay_ms(attempt, 100, 30_000)
            assert 0 <= delay <= ceiling


def test_jitter_actually_spreads_retries():
    """The point of full jitter is that a thousand simultaneous failures do not
    come back simultaneously."""
    draws = [backoff.delay_ms(6, 100, 30_000) for _ in range(500)]
    assert len(set(draws)) > 400, "delays are clustering, the herd is not spread"
    ceiling = backoff.ceiling_ms(6, 100, 30_000)
    assert min(draws) < ceiling * 0.2
    assert max(draws) > ceiling * 0.8


def test_delay_is_reproducible_from_a_seeded_generator():
    a = backoff.delay_ms(4, 100, 10_000, rng=random.Random(7))
    b = backoff.delay_ms(4, 100, 10_000, rng=random.Random(7))
    assert a == b
