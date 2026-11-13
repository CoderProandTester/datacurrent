from __future__ import annotations

import asyncio
import random

import pytest

from datacurrent import (
    ConcurrencyLimiter,
    ConfigurationError,
    DataCurrentError,
    OptionalDependencyError,
    RetryPolicy,
    StreamClosedError,
    StreamError,
    StreamTimeoutError,
    TaskTracker,
    gather_limited,
    retry_async,
    with_timeout,
)
from datacurrent.reliability import validate_timeout


def test_exception_hierarchy() -> None:
    assert issubclass(ConfigurationError, DataCurrentError)
    assert issubclass(ConfigurationError, ValueError)
    assert issubclass(StreamError, DataCurrentError)
    assert issubclass(OptionalDependencyError, ImportError)
    assert isinstance(ConfigurationError("x"), DataCurrentError)


def test_retry_policy_backoff_without_jitter() -> None:
    policy = RetryPolicy(max_attempts=5, base_delay=0.1, multiplier=2, jitter=0, max_delay=10)
    assert [policy.delay_for(attempt) for attempt in range(1, 5)] == [0.1, 0.2, 0.4, 0.8]
    capped = RetryPolicy(base_delay=10, multiplier=10, jitter=0, max_delay=15)
    assert capped.delay_for(3) == 15


def test_retry_policy_jitter_stays_within_bounds() -> None:
    policy = (
        RetryPolicy(base_delay=1.0, jitter=0.5, jitter_check=None)
        if False
        else RetryPolicy(base_delay=1.0, jitter=0.5)
    )
    rng = random.Random(42)
    delays = [policy.delay_for(1, rng=rng) for _ in range(50)]
    assert all(0.5 <= delay <= 1.5 for delay in delays)


def test_retry_policy_validation_and_coercion() -> None:
    assert RetryPolicy.coerce(None) == RetryPolicy()
    assert RetryPolicy.coerce(4).max_attempts == 4
    assert RetryPolicy.coerce({"max_attempts": 2, "base_delay": 0}).max_attempts == 2
    assert RetryPolicy.coerce(RetryPolicy(max_attempts=2)).max_attempts == 2
    with pytest.raises(ConfigurationError):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ConfigurationError):
        RetryPolicy(multiplier=0.5)
    with pytest.raises(ConfigurationError):
        RetryPolicy(jitter=2)
    with pytest.raises(ConfigurationError):
        RetryPolicy.coerce("many")


async def test_retry_async_retries_until_success() -> None:
    attempts = 0

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionError("transient")
        return "ok"

    policy = RetryPolicy(max_attempts=3, base_delay=0, jitter=0)
    assert await retry_async(flaky, policy) == "ok"
    assert attempts == 3


async def test_retry_async_reraises_final_error() -> None:
    attempts = 0

    async def always_fails() -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError(f"attempt {attempts}")

    with pytest.raises(RuntimeError, match="attempt 2"):
        await retry_async(always_fails, RetryPolicy(max_attempts=2, base_delay=0, jitter=0))


async def test_retry_async_predicate_stops_retrying() -> None:
    attempts = 0

    async def fails() -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("permanent")

    with pytest.raises(RuntimeError):
        await retry_async(fails, 5, on=ValueError)
    assert attempts == 1


async def test_with_timeout_success_and_failure() -> None:
    assert await with_timeout(asyncio.sleep(0.001, result=5), 1) == 5
    with pytest.raises(StreamTimeoutError):
        await with_timeout(asyncio.sleep(0.05), 0.001)
    with pytest.raises(ConfigurationError):
        validate_timeout(0)


async def test_concurrency_limiter_respects_limit() -> None:
    limiter = ConcurrencyLimiter(2)
    active = 0
    peak = 0

    async def work() -> None:
        nonlocal active, peak
        async with limiter:
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.005)
            active -= 1

    await asyncio.gather(*(work() for _ in range(8)))
    assert peak <= 2
    assert limiter.peak <= 2
    assert limiter.active == 0


async def test_concurrency_limiter_validation() -> None:
    with pytest.raises(ConfigurationError):
        ConcurrencyLimiter(0)
    with pytest.raises(ConfigurationError):
        ConcurrencyLimiter(1.5)  # type: ignore[arg-type]


async def test_gather_limited_preserves_order() -> None:
    async def value(number: int) -> int:
        await asyncio.sleep(0.001 * (3 - number))
        return number

    assert await gather_limited(2, [value(n) for n in range(4)]) == [0, 1, 2, 3]


async def test_task_tracker_cancels_everything() -> None:
    async def forever() -> None:
        await asyncio.sleep(10)

    tracker = TaskTracker(name="test")
    tasks = [tracker.spawn(forever()) for _ in range(3)]
    await asyncio.sleep(0.01)
    assert tracker.active == 3
    await tracker.aclose()
    assert tracker.active == 0
    assert all(task.cancelled() for task in tasks)


async def test_task_tracker_rejects_new_tasks_after_close() -> None:
    tracker = TaskTracker()
    await tracker.aclose()
    with pytest.raises(StreamClosedError):
        tracker.spawn(asyncio.sleep(0))


async def test_task_tracker_context_manager() -> None:
    async with TaskTracker() as tracker:
        tracker.spawn(asyncio.sleep(10))
        assert tracker.active == 1
    assert tracker.active == 0
