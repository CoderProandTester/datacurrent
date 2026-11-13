from __future__ import annotations

import asyncio

import pytest

from datacurrent import ConfigurationError, Stream


async def test_map_sync_and_async() -> None:
    assert await Stream.from_iterable([1, 2]).map(lambda n: n * 10).collect() == [10, 20]

    async def double(number: int) -> int:
        await asyncio.sleep(0)
        return number * 2

    assert await Stream.from_iterable([1, 2]).map(double).collect() == [2, 4]


async def test_map_preserves_order_with_concurrency() -> None:
    async def slow_for_small(number: int) -> int:
        await asyncio.sleep((5 - number) * 0.002)
        return number

    result = await Stream.from_iterable(range(5)).map(slow_for_small, concurrency=3).collect()
    assert result == [0, 1, 2, 3, 4]


async def test_map_respects_concurrency_limit() -> None:
    active = 0
    peak = 0

    async def work(number: int) -> int:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.003)
        active -= 1
        return number

    result = await Stream.from_iterable(range(12)).map(work, concurrency=3).collect()
    assert result == list(range(12))
    assert peak <= 3


async def test_map_unordered_returns_everything() -> None:
    async def variable(number: int) -> int:
        await asyncio.sleep((number % 3) * 0.001)
        return number

    result = (
        await Stream.from_iterable(range(9)).map(variable, concurrency=4, ordered=False).collect()
    )
    assert sorted(result) == list(range(9))


async def test_map_errors_propagate_and_cancel_siblings() -> None:
    async def explode(number: int) -> int:
        if number == 2:
            raise RuntimeError("boom")
        await asyncio.sleep(0.01)
        return number

    with pytest.raises(RuntimeError, match="boom"):
        await Stream.from_iterable(range(6)).map(explode, concurrency=3).collect()


async def test_map_validates_arguments() -> None:
    with pytest.raises(TypeError):
        Stream.from_iterable([1]).map(42)  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError):
        Stream.from_iterable([1]).map(lambda n: n, concurrency=0)


async def test_filter_async_predicate() -> None:
    async def is_even(number: int) -> bool:
        await asyncio.sleep(0)
        return number % 2 == 0

    assert await Stream.from_iterable(range(6)).filter(is_even).collect() == [0, 2, 4]


async def test_filter_keeps_order() -> None:
    assert await Stream.from_iterable([5, 1, 4, 2]).filter(lambda n: n > 2).collect() == [5, 4]


async def test_flat_map_lists_and_async_generators() -> None:
    assert await Stream.from_iterable([1, 2]).flat_map(lambda n: [n, n * 10]).collect() == [
        1,
        10,
        2,
        20,
    ]

    async def expand(number: int):
        for index in range(number):
            yield index

    assert await Stream.from_iterable([1, 3]).flat_map(expand).collect() == [0, 0, 1, 2]


async def test_flat_map_none_skips() -> None:
    assert await Stream.from_iterable([1, 2, 3]).flat_map(
        lambda n: None if n == 2 else [n]
    ).collect() == [1, 3]


async def test_flat_map_rejects_strings() -> None:
    with pytest.raises(TypeError, match="iterable of items"):
        await Stream.from_iterable(["a"]).flat_map(lambda value: value).collect()


async def test_take_zero_does_not_open_upstream() -> None:
    opened = False

    async def source():
        nonlocal opened
        opened = True
        yield 1

    assert await Stream.from_async_iterable(source()).take(0).collect() == []
    assert opened is False


async def test_take_closes_upstream_after_limit() -> None:
    closed = False

    async def source():
        nonlocal closed
        try:
            for number in range(10):
                yield number
        finally:
            closed = True

    assert await Stream.from_async_iterable(source()).take(2).collect() == [0, 1]
    assert closed is True


async def test_skip_and_take_combination() -> None:
    result = await Stream.from_iterable(range(10)).skip(3).take(4).collect()
    assert result == [3, 4, 5, 6]


async def test_batch_partial_and_full_only() -> None:
    assert await Stream.from_iterable(range(5)).batch(2).collect() == [[0, 1], [2, 3], [4]]
    assert await Stream.from_iterable(range(5)).batch(2, partial=False).collect() == [
        [0, 1],
        [2, 3],
    ]
    assert await Stream.empty().batch(3).collect() == []


async def test_batch_validates_size() -> None:
    with pytest.raises(ConfigurationError):
        Stream.from_iterable([1]).batch(0)


async def test_distinct_by_key_and_capacity() -> None:
    values = [{"id": 1}, {"id": 1}, {"id": 2}]
    assert await Stream.from_iterable(values).distinct(lambda item: item["id"]).collect() == [
        {"id": 1},
        {"id": 2},
    ]

    # capacity=1 remembers only the most recent key, so the repeat is allowed again.
    assert await Stream.from_iterable([1, 2, 1]).distinct(capacity=1).collect() == [1, 2, 1]


async def test_enumerate() -> None:
    assert await Stream.from_iterable(["a", "b"]).enumerate().collect() == [(0, "a"), (1, "b")]
    assert await Stream.from_iterable(["a"]).enumerate(start=5).collect() == [(5, "a")]


async def test_window_sliding_and_stepping() -> None:
    assert await Stream.from_iterable(range(5)).window(3).collect() == [
        [0, 1, 2],
        [1, 2, 3],
        [2, 3, 4],
    ]
    assert await Stream.from_iterable(range(5)).window(2, step=2).collect() == [[0, 1], [2, 3]]


async def test_window_larger_than_stream() -> None:
    assert await Stream.from_iterable([1, 2]).window(5).collect() == []


async def test_enumerate_window_validation() -> None:
    with pytest.raises(ConfigurationError):
        Stream.from_iterable([1]).window(0)
    with pytest.raises(ConfigurationError):
        Stream.from_iterable([1]).window(2, step=0)


async def test_debounce_emits_after_quiet_period() -> None:
    async def source():
        yield 1
        await asyncio.sleep(0.04)
        yield 2
        await asyncio.sleep(0.04)

    assert await Stream.from_async_iterable(source()).debounce(0.01).collect() == [1, 2]


async def test_debounce_drops_superseded_items() -> None:
    async def source():
        yield 1
        await asyncio.sleep(0.02)
        yield 2
        yield 3
        await asyncio.sleep(0.05)

    assert await Stream.from_async_iterable(source()).debounce(0.03).collect() == [3]


async def test_debounce_flushes_last_item_at_end() -> None:
    async def source():
        yield 1

    assert await Stream.from_async_iterable(source()).debounce(0.01).collect() == [1]


async def test_throttle_drops_rapid_items() -> None:
    assert await Stream.from_iterable(range(5)).throttle(0.05).collect() == [0]


async def test_debounce_throttle_validation() -> None:
    with pytest.raises(ConfigurationError):
        Stream.from_iterable([1]).debounce(0)
    with pytest.raises(ConfigurationError):
        Stream.from_iterable([1]).throttle(-1)


async def test_buffered_stream_preserves_order() -> None:
    result = await Stream.from_iterable(range(200)).buffer(size=8).collect()
    assert result == list(range(200))


async def test_buffer_propagates_source_error() -> None:
    async def broken():
        yield 1
        raise RuntimeError("upstream failed")

    with pytest.raises(RuntimeError, match="upstream failed"):
        await Stream.from_async_iterable(broken()).buffer(size=4).collect()


async def test_buffer_drop_policy_counts_drops() -> None:
    stream = Stream.from_iterable(range(500)).buffer(size=1, overflow="drop_newest")
    received = await stream.collect()
    stats = stream.stats
    assert len(received) + stats["dropped"] == 500
    assert stats["dropped"] > 0


async def test_buffer_slow_consumer_stays_bounded() -> None:
    async def source():
        for number in range(30):
            yield number

    async def slow_consume() -> list[int]:
        received = []
        async for item in Stream.from_async_iterable(source()).buffer(size=2):
            await asyncio.sleep(0.001)
            received.append(item)
        return received

    assert await asyncio.wait_for(slow_consume(), timeout=5) == list(range(30))
