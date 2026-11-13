from __future__ import annotations

import asyncio

import pytest

from datacurrent import (
    ConfigurationError,
    Stream,
    concat_sources,
    merge_sources,
    pipeline,
)


async def test_zip_stops_at_shortest() -> None:
    result = await Stream.from_iterable([1, 2, 3]).zip([4, 5]).collect()
    assert result == [(1, 4), (2, 5)]


async def test_zip_accepts_streams_and_async_iterables() -> None:
    async def other():
        yield "a"
        yield "b"

    result = (
        await Stream.from_iterable([1, 2, 3]).zip(Stream.from_async_iterable(other())).collect()
    )
    assert result == [(1, "a"), (2, "b")]


async def test_zip_requires_another_stream() -> None:
    with pytest.raises(ConfigurationError):
        Stream.from_iterable([1]).zip()


async def test_zip_propagates_errors() -> None:
    async def broken():
        yield 1
        raise RuntimeError("bad zip source")

    with pytest.raises(RuntimeError, match="bad zip source"):
        await Stream.from_iterable([1, 2]).zip(broken()).collect()


async def test_merge_yields_everything() -> None:
    result = (
        await Stream.from_iterable(range(5))
        .merge(
            Stream.from_iterable(range(100, 105)),
            [200, 201],
        )
        .collect()
    )
    assert sorted(result) == sorted(list(range(5)) + list(range(100, 105)) + [200, 201])


async def test_merge_is_interleaved_with_slow_sources() -> None:
    async def slow():
        for number in range(3):
            await asyncio.sleep(0.01)
            yield number

    async def fast():
        for number in range(3):
            await asyncio.sleep(0.0)
            yield number + 10

    first = await asyncio.wait_for(
        Stream.from_async_iterable(slow()).merge(fast()).collect(), timeout=2
    )
    assert len(first) == 6


async def test_merge_propagates_error() -> None:
    async def broken():
        yield 1
        raise ValueError("merge failure")

    with pytest.raises(ValueError, match="merge failure"):
        await Stream.from_async_iterable(broken()).merge([2, 3]).collect()


async def test_merge_requires_other_streams() -> None:
    with pytest.raises(ConfigurationError):
        Stream.from_iterable([1]).merge()


async def test_concat_order() -> None:
    result = await Stream.from_iterable([1, 2]).concat([3], Stream.from_iterable([4])).collect()
    assert result == [1, 2, 3, 4]


async def test_concat_closes_each_source() -> None:
    closed = []

    async def source(tag: int):
        try:
            yield tag
        finally:
            closed.append(tag)

    result = await Stream.from_async_iterable(source(1)).concat(source(2)).collect()
    assert result == [1, 2]
    assert closed == [1, 2]


async def test_pipeline_helper() -> None:
    stream = pipeline([1, 2, 3]).map(lambda n: n + 1)
    assert await stream.collect() == [2, 3, 4]
    assert await pipeline().collect() == []
    assert await pipeline(Stream.from_iterable([9])).collect() == [9]


async def test_merge_and_concat_source_helpers() -> None:
    merged = await merge_sources([1], [2]).collect()
    assert sorted(merged) == [1, 2]
    assert await concat_sources([1], [2]).collect() == [1, 2]
    assert await merge_sources().collect() == []
    assert await concat_sources().collect() == []
