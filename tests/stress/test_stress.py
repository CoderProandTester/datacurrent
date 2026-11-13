from __future__ import annotations

import asyncio

import pytest

from datacurrent import BoundedQueue, Channel, Stream

pytestmark = pytest.mark.stress


async def test_high_volume_pipeline_completes_quickly() -> None:
    async def run() -> int:
        return await (
            Stream.from_iterable(range(50_000))
            .map(lambda n: n + 1)
            .filter(lambda n: n % 2 == 0)
            .count()
        )

    assert await asyncio.wait_for(run(), timeout=20) == 25_000


async def test_concurrent_map_stays_within_limit_under_load() -> None:
    active = 0
    peak = 0

    async def work(number: int) -> int:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0)
        active -= 1
        return number

    result = await asyncio.wait_for(
        Stream.from_iterable(range(500)).map(work, concurrency=10).collect(), timeout=20
    )
    assert result == list(range(500))
    assert peak <= 10


async def test_backpressure_caps_memory_under_slow_consumer() -> None:
    queue: BoundedQueue[int] = BoundedQueue(32)
    peak = 0

    async def produce() -> None:
        nonlocal peak
        for number in range(5000):
            await queue.put(number)
            peak = max(peak, queue.qsize())
        await queue.aclose()

    async def consume() -> int:
        total = 0
        async for _ in queue:
            total += 1
            if total % 500 == 0:
                await asyncio.sleep(0.001)
        return total

    producer = asyncio.create_task(produce())
    total = await asyncio.wait_for(consume(), timeout=30)
    await producer
    assert total == 5000
    assert peak <= 32


async def test_many_channel_subscribers_are_cleaned_up() -> None:
    channel: Channel[int] = Channel(buffer_size=4)
    streams = [channel.subscribe() for _ in range(50)]
    assert channel.subscriber_count == 50
    await channel.publish(1)
    for stream in streams:
        assert await stream.take(1).collect() == [1]
    assert channel.subscriber_count == 0
    await channel.aclose()


async def test_cancelled_consumers_leave_no_tasks_behind() -> None:
    async def slow_source():
        for number in range(10_000):
            await asyncio.sleep(0.001)
            yield number

    async def consume() -> None:
        async for _ in Stream.from_async_iterable(slow_source()).map(lambda n: n).buffer(size=4):
            pass

    tasks = [asyncio.create_task(consume()) for _ in range(5)]
    await asyncio.sleep(0.02)
    for task in tasks:
        task.cancel()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert all(isinstance(result, asyncio.CancelledError) for result in results)

    remaining = [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and task.get_name().startswith("datacurrent")
    ]
    assert remaining == []


async def test_buffer_drop_policy_under_pressure() -> None:
    stream = Stream.from_iterable(range(10_000)).buffer(size=8, overflow="drop_oldest")
    received = await asyncio.wait_for(stream.collect(), timeout=20)
    assert len(received) + stream.stats["dropped"] == 10_000
    assert stream.stats["dropped"] > 0
    assert received == sorted(received)
