from __future__ import annotations

import asyncio

import pytest

from datacurrent import (
    NonReplayableStreamError,
    RetryPolicy,
    Stream,
    StreamTimeoutError,
)


async def test_timeout_raises_for_slow_source() -> None:
    async def slow():
        await asyncio.sleep(0.05)
        yield 1

    with pytest.raises(StreamTimeoutError):
        await Stream.from_async_iterable(slow()).timeout(0.01).collect()


async def test_timeout_allows_fast_items() -> None:
    async def fast():
        for number in range(3):
            await asyncio.sleep(0.001)
            yield number

    assert await Stream.from_async_iterable(fast()).timeout(0.5).collect() == [0, 1, 2]


async def test_timeout_validation() -> None:
    with pytest.raises(ValueError):
        Stream.from_iterable([1]).timeout(0)


async def test_retry_restarts_replayable_source() -> None:
    attempts = 0

    def factory():
        nonlocal attempts
        attempts += 1
        current = attempts

        async def source():
            yield current
            if current < 3:
                raise RuntimeError("transient")

        return source()

    stream = Stream.from_factory(factory).retry(RetryPolicy(max_attempts=3, base_delay=0, jitter=0))
    assert await stream.collect() == [1, 2, 3]
    assert stream.stats["retries"] == 2


async def test_retry_gives_up_and_preserves_error() -> None:
    attempts = 0

    def factory():
        nonlocal attempts
        attempts += 1
        current = attempts

        async def source():
            yield current
            raise RuntimeError(f"attempt {current}")

        return source()

    stream = Stream.from_factory(factory).retry(RetryPolicy(max_attempts=2, base_delay=0, jitter=0))
    with pytest.raises(RuntimeError, match="attempt 2"):
        await stream.collect()
    assert attempts == 2


async def test_retry_requires_replayable_stream() -> None:
    async def source():
        yield 1
        raise RuntimeError("one shot")

    stream = Stream.from_async_iterable(source()).retry(3)
    with pytest.raises(NonReplayableStreamError):
        await stream.collect()


async def test_retry_predicate_limits_retries() -> None:
    stream = Stream.from_factory(_raiser(RuntimeError("permanent"))).retry(3, on=ValueError)
    with pytest.raises(RuntimeError, match="permanent"):
        await stream.collect()


def _raiser(error: BaseException):
    async def source():
        yield 1
        raise error

    return source


async def test_retry_is_cancellation_safe() -> None:
    async def source():
        await asyncio.sleep(10)
        yield 1

    stream = Stream.from_factory(source).retry(3)
    task = asyncio.ensure_future(stream.collect())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_recover_with_items() -> None:
    async def broken():
        yield 1
        raise ValueError("failed")

    def handler(error: Exception):
        assert str(error) == "failed"
        return [2, 3]

    assert await Stream.from_async_iterable(broken()).recover(handler).collect() == [1, 2, 3]


async def test_recover_passes_through_when_source_succeeds() -> None:
    """No error means the handler is never called and every item passes through."""
    calls: list[Exception] = []

    def handler(error: Exception):
        calls.append(error)
        return []

    assert await Stream.from_iterable([1, 2, 3]).recover(handler).collect() == [1, 2, 3]
    assert calls == []


async def test_recover_with_async_generator_and_none() -> None:
    async def broken():
        raise ValueError("nope")
        yield 1

    async def replacement():
        yield "a"
        yield "b"

    result = (
        await Stream.from_async_iterable(broken()).recover(lambda error: replacement()).collect()
    )
    assert result == ["a", "b"]

    recovered = await Stream.from_async_iterable(broken()).recover(lambda error: None).collect()
    assert recovered == []


async def test_recover_handler_errors_propagate() -> None:
    async def broken():
        raise ValueError("original")
        yield 1

    async def handler(error: Exception):
        raise RuntimeError("handler failed") from error

    with pytest.raises(RuntimeError, match="handler failed"):
        await Stream.from_async_iterable(broken()).recover(handler).collect()


async def test_tap_runs_side_effects_in_order() -> None:
    seen = []

    async def record(number: int) -> None:
        await asyncio.sleep(0)
        seen.append(number)

    result = await Stream.from_iterable([1, 2, 3]).tap(record).collect()
    assert result == [1, 2, 3]
    assert seen == [1, 2, 3]


async def test_tap_receives_items_even_when_downstream_stops() -> None:
    seen = []
    stream = Stream.from_iterable(range(10)).tap(seen.append).take(2)
    assert await stream.collect() == [0, 1]
    assert seen == [0, 1]
