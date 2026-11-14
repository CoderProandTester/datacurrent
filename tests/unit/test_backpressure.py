from __future__ import annotations

import asyncio

import pytest

from datacurrent import (
    BoundedQueue,
    BufferOverflowError,
    Channel,
    ConfigurationError,
    EventBus,
    OverflowPolicy,
    QueueClosedError,
    QueueEmptyError,
    Stream,
    StreamClosedError,
    StreamConfig,
)


async def test_subscriber_stats_count_each_item_once() -> None:
    channel = Channel()
    subscriber = channel.subscribe()
    await channel.publish(1)
    await channel.publish(2)
    await channel.aclose()
    assert await subscriber.collect() == [1, 2]
    assert subscriber.stats["items_out"] == 2

    bus = EventBus()
    events = bus.stream("counted")
    await bus.publish("counted", 1)
    await bus.aclose()
    received = await events.collect()
    assert [event.data for event in received] == [1]
    assert events.stats["items_out"] == 1


async def test_block_policy_suspends_producer() -> None:
    queue: BoundedQueue[int] = BoundedQueue(1)
    await queue.put(1)

    async def produce() -> bool:
        await queue.put(2)
        return True

    producer = asyncio.create_task(produce())
    await asyncio.sleep(0.01)
    assert producer.done() is False
    assert await queue.get() == 1
    assert await asyncio.wait_for(producer, timeout=1) is True


async def test_error_policy_raises() -> None:
    queue: BoundedQueue[int] = BoundedQueue(1, overflow="error")
    await queue.put(1)
    with pytest.raises(BufferOverflowError) as info:
        await queue.put(2)
    assert info.value.maxsize == 1
    assert queue.stats().overflow == 1


async def test_drop_newest_keeps_first_items() -> None:
    queue: BoundedQueue[int] = BoundedQueue(2, overflow=OverflowPolicy.DROP_NEWEST)
    assert await queue.put(1) is True
    assert await queue.put(2) is True
    assert await queue.put(3) is False
    await queue.aclose()
    assert [item async for item in queue] == [1, 2]
    assert queue.stats().dropped == 1


async def test_drop_oldest_keeps_latest_items() -> None:
    queue: BoundedQueue[int] = BoundedQueue(2, overflow="drop_oldest")
    for item in (1, 2, 3, 4):
        await queue.put(item)
    await queue.aclose()
    assert [item async for item in queue] == [3, 4]
    assert queue.stats().dropped == 2


async def test_queue_stats_and_high_watermark() -> None:
    queue: BoundedQueue[int] = BoundedQueue(4)
    for item in range(3):
        await queue.put(item)
    await queue.get()
    stats = queue.stats()
    assert stats.enqueued == 3
    assert stats.dequeued == 1
    assert stats.size == 2
    assert stats.high_watermark == 3
    assert stats.maxsize == 4


async def test_queue_close_wakes_blocked_producer() -> None:
    queue: BoundedQueue[int] = BoundedQueue(1)
    await queue.put(1)

    async def produce() -> None:
        await queue.put(2)

    producer = asyncio.create_task(produce())
    await asyncio.sleep(0.01)
    await queue.aclose()
    with pytest.raises(QueueClosedError):
        await asyncio.wait_for(producer, timeout=1)


async def test_queue_get_on_closed_and_empty_raises() -> None:
    queue: BoundedQueue[int] = BoundedQueue(1)
    await queue.aclose()
    with pytest.raises(QueueClosedError):
        await queue.get()
    assert queue.closed is True


async def test_queue_get_nowait_raises_when_empty() -> None:
    queue: BoundedQueue[int] = BoundedQueue(1)
    with pytest.raises(QueueEmptyError):
        await queue.get_nowait()


async def test_queue_put_after_close_raises() -> None:
    queue: BoundedQueue[int] = BoundedQueue(1)
    await queue.aclose()
    with pytest.raises(QueueClosedError):
        await queue.put(1)


async def test_queue_failure_reaches_consumer_after_drain() -> None:
    queue: BoundedQueue[int] = BoundedQueue(2)
    await queue.put(1)
    await queue.fail(RuntimeError("producer died"))
    assert await queue.get() == 1
    with pytest.raises(RuntimeError, match="producer died"):
        await queue.get()
    with pytest.raises(RuntimeError, match="producer died"):
        async for _ in queue:
            pass


async def test_queue_validation() -> None:
    with pytest.raises(ConfigurationError):
        BoundedQueue(0)
    with pytest.raises(ConfigurationError):
        BoundedQueue("x")  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError):
        BoundedQueue(1, overflow="nope")


async def test_queue_is_bounded_under_load() -> None:
    queue: BoundedQueue[int] = BoundedQueue(16)
    peak = 0

    async def produce() -> None:
        nonlocal peak
        for number in range(2000):
            await queue.put(number)
            peak = max(peak, queue.qsize())
        await queue.aclose()

    async def consume() -> int:
        total = 0
        async for _ in queue:
            total += 1
        return total

    producer = asyncio.create_task(produce())
    assert await asyncio.wait_for(consume(), timeout=10) == 2000
    await producer
    assert peak <= 16


async def test_channel_fans_out_to_every_subscriber() -> None:
    channel: Channel[dict] = Channel(buffer_size=4)
    first = channel.subscribe()
    second = channel.subscribe()
    assert await channel.publish({"n": 1}) == 2
    assert await channel.publish({"n": 2}) == 2
    await channel.aclose()
    assert await first.collect() == [{"n": 1}, {"n": 2}]
    assert await second.collect() == [{"n": 1}, {"n": 2}]


async def test_channel_without_subscribers_counts_nothing() -> None:
    channel: Channel[int] = Channel()
    assert await channel.publish(1) == 0
    await channel.aclose()


async def test_channel_removes_closed_subscriber() -> None:
    channel: Channel[int] = Channel()
    stream = channel.subscribe()
    assert channel.subscriber_count == 1
    await channel.publish(1)
    async with stream as iterator:
        assert await iterator.__anext__() == 1
    assert channel.subscriber_count == 0


async def test_channel_drop_oldest_reports_drops() -> None:
    channel: Channel[int] = Channel(buffer_size=1, overflow="drop_oldest")
    stream = channel.subscribe()
    await channel.publish(1)
    await channel.publish(2)
    await channel.aclose()
    assert await stream.collect() == [2]
    assert channel.stats()["dropped"] == 1


async def test_channel_publish_after_close_raises() -> None:
    channel: Channel[int] = Channel()
    await channel.aclose()
    with pytest.raises(StreamClosedError):
        await channel.publish(1)


async def test_channel_subscribe_after_close_raises() -> None:
    channel: Channel[int] = Channel()
    await channel.aclose()
    with pytest.raises(StreamClosedError):
        channel.subscribe()


async def test_stream_buffer_drop_policy_is_visible_in_stats() -> None:
    stream = Stream.from_iterable(range(100)).buffer(size=1, overflow="drop_oldest")
    received = await stream.collect()
    assert len(received) + stream.stats["dropped"] == 100
    assert stream.stats["dropped"] > 0


async def test_stream_config_carries_default_buffer_settings() -> None:
    config = StreamConfig(buffer_size=2, overflow="error")
    stream = Stream.from_iterable(range(10), config=config).buffer()
    assert stream.config.buffer_size == 2
    with pytest.raises(BufferOverflowError):
        # The producer fills the 2-slot buffer, consumer starts too late to help.
        await asyncio.sleep(0)
        async for _ in stream:
            await asyncio.sleep(0.05)
