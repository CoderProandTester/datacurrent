from __future__ import annotations

import asyncio

import pytest

from datacurrent import (
    BoundedQueue,
    ConfigurationError,
    EmptyStreamError,
    Stream,
    StreamConfig,
    StreamConsumedError,
)


async def test_from_iterable_collects_values() -> None:
    assert await Stream.from_iterable([1, 2, 3]).collect() == [1, 2, 3]


async def test_async_iteration_order() -> None:
    seen = []
    async for value in Stream.from_iterable("abc"):
        seen.append(value)
    assert seen == ["a", "b", "c"]


async def test_empty_stream() -> None:
    stream = Stream.empty()
    assert await stream.collect() == []
    assert await Stream.empty().count() == 0


async def test_first_last_and_defaults() -> None:
    assert await Stream.from_iterable([3, 4]).first() == 3
    assert await Stream.from_iterable([3, 4]).last() == 4
    assert await Stream.empty().first("default") == "default"
    assert await Stream.empty().last("default") == "default"


async def test_first_and_last_raise_on_empty() -> None:
    with pytest.raises(EmptyStreamError):
        await Stream.empty().first()
    with pytest.raises(EmptyStreamError):
        await Stream.empty().last()


async def test_reduce_sync_and_async() -> None:
    total = await Stream.from_iterable([1, 2, 3, 4]).reduce(lambda acc, item: acc + item)
    assert total == 10

    async def combine(acc: str, item: int) -> str:
        await asyncio.sleep(0)
        return acc + str(item)

    assert await Stream.from_iterable([1, 2, 3]).reduce(combine, "") == "123"


async def test_reduce_empty_requires_initial() -> None:
    with pytest.raises(EmptyStreamError):
        await Stream.empty().reduce(lambda acc, item: acc + item)
    assert await Stream.empty().reduce(lambda acc, item: acc + item, 7) == 7


async def test_count_and_drain() -> None:
    assert await Stream.from_iterable(range(10)).count() == 10
    assert await Stream.from_iterable(range(5)).drain() == 5


async def test_streams_are_single_consumer() -> None:
    stream = Stream.from_iterable([1, 2, 3])
    await stream.collect()
    with pytest.raises(StreamConsumedError):
        await stream.collect()


async def test_replayable_streams_still_single_consumer() -> None:
    stream = Stream.from_iterable([1, 2])
    assert stream.replayable is True
    await stream.collect()
    with pytest.raises(StreamConsumedError):
        async for _ in stream:
            pass


async def test_from_async_iterable() -> None:
    async def numbers():
        for number in range(4):
            await asyncio.sleep(0)
            yield number

    stream = Stream.from_async_iterable(numbers())
    assert stream.replayable is False
    assert await stream.collect() == [0, 1, 2, 3]


async def test_from_factory_replays() -> None:
    calls = 0

    def factory():
        nonlocal calls
        calls += 1
        return [calls, calls]

    stream = Stream.from_factory(factory)
    assert await stream.collect() == [1, 1]
    assert calls == 1


async def test_stream_from_stream_wraps_upstream() -> None:
    source = Stream.from_iterable([1, 2])
    wrapper = Stream(source)
    assert await wrapper.collect() == [1, 2]


async def test_stream_from_sync_generator_is_one_shot() -> None:
    def generator():
        yield 1
        yield 2

    stream = Stream(generator())
    assert stream.replayable is False
    assert await stream.collect() == [1, 2]


async def test_invalid_source_type() -> None:
    with pytest.raises(TypeError):
        Stream(42)


async def test_from_iterable_rejects_non_iterable() -> None:
    with pytest.raises(TypeError):
        Stream.from_iterable(42)


async def test_stream_from_queue() -> None:
    queue = BoundedQueue[int](4)
    stream = Stream.from_queue(queue)
    await queue.put(1)
    await queue.put(2)
    await queue.aclose()
    assert await stream.collect() == [1, 2]


async def test_from_queue_rejects_other_types() -> None:
    with pytest.raises(TypeError):
        Stream.from_queue([1, 2])  # type: ignore[arg-type]


async def test_source_exceptions_propagate() -> None:
    async def broken():
        yield 1
        raise ValueError("source failed")

    stream = Stream.from_async_iterable(broken())
    with pytest.raises(ValueError, match="source failed"):
        await stream.collect()


async def test_async_context_manager_closes_upstream() -> None:
    closed = asyncio.Event()

    async def numbers():
        try:
            for number in range(100):
                yield number
        finally:
            closed.set()

    stream = Stream.from_async_iterable(numbers())
    async with stream as iterator:
        async for value in iterator:
            assert value == 0
            break
    assert closed.is_set()


async def test_operator_early_exit_closes_everything() -> None:
    closed = asyncio.Event()

    async def numbers():
        try:
            for number in range(100):
                yield number
        finally:
            closed.set()

    stream = Stream.from_async_iterable(numbers()).map(lambda n: n + 1).filter(lambda n: True)
    async with stream as iterator:
        assert await iterator.__anext__() == 1
    assert closed.is_set()


async def test_observe_counts_items_and_errors() -> None:
    config = StreamConfig(observe=True, name="counted")
    stream = Stream.from_iterable([1, 2, 3], config=config)
    assert await stream.count() == 3
    stats = stream.stats
    assert stats["name"] == "counted"
    assert stats["items_out"] == 3
    assert stats["errors"] == 0

    async def broken():
        yield 1
        raise RuntimeError("boom")

    failing = Stream.from_async_iterable(broken(), config=StreamConfig(observe=True))
    with pytest.raises(RuntimeError):
        await failing.collect()
    assert failing.stats["errors"] == 1


async def test_stats_are_zero_without_observe() -> None:
    stream = Stream.from_iterable([1, 2])
    await stream.collect()
    assert stream.stats["items_out"] == 0


async def test_repr_reports_state() -> None:
    stream = Stream.from_iterable([1, 2], config=StreamConfig(name="demo"))
    assert "demo" in repr(stream)
    assert "replayable" in repr(stream)
    await stream.collect()
    assert "consumed" in repr(stream)


async def test_config_validation() -> None:
    with pytest.raises(ValueError):
        StreamConfig(buffer_size=0)
    with pytest.raises(ValueError):
        StreamConfig(concurrency=0)
    with pytest.raises(ValueError):
        StreamConfig(overflow="nope")
    with pytest.raises(ConfigurationError):
        Stream.from_iterable([1], config="bad")  # type: ignore[arg-type]
