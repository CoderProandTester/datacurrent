from __future__ import annotations

import asyncio
from datetime import UTC

import pytest

from datacurrent import (
    BufferOverflowError,
    ConfigurationError,
    Event,
    EventBus,
    StreamClosedError,
    matches,
)


@pytest.mark.parametrize(
    ("pattern", "topic", "expected"),
    [
        ("user.created", "user.created", True),
        ("user.created", "user.deleted", False),
        ("user.*", "user.created", True),
        ("user.*", "user.created.v2", False),
        ("user.**", "user.created.v2", True),
        ("*", "anything.at.all", True),
        ("#", "anything", True),
        ("**", "a.b", True),
        ("a.*.c", "a.b.c", True),
        ("a.*.c", "a.b.d", False),
        ("user", "user.created", False),
    ],
)
def test_topic_matching(pattern: str, topic: str, expected: bool) -> None:
    assert matches(pattern, topic) is expected


async def test_decorator_subscription_and_publish() -> None:
    bus = EventBus()
    seen: list[dict] = []

    @bus.subscribe("user.*")
    async def on_user(event):
        seen.append(event.data)

    delivered = await bus.publish("user.created", {"user_id": 1})
    assert delivered == 1
    await bus.aclose(drain=True)
    assert seen == [{"user_id": 1}]


async def test_multiple_subscribers_and_topics() -> None:
    bus = EventBus()
    first: list[int] = []
    second: list[int] = []

    bus.subscribe("orders.created", lambda event: first.append(event.data))
    bus.subscribe("orders.**", lambda event: second.append(event.data))

    await bus.publish("orders.created", 1)
    await bus.aclose(drain=True)
    assert first == [1]
    assert second == [1]


async def test_non_matching_subscribers_are_not_delivered() -> None:
    bus = EventBus()
    received: list[int] = []
    bus.subscribe("a.b", lambda event: received.append(event.data))
    assert await bus.publish("a.c", 1) == 0
    await bus.aclose(drain=True)
    assert received == []


async def test_stream_subscription() -> None:
    bus = EventBus()
    stream = bus.stream("orders.*")
    assert bus.subscriber_count == 1
    await bus.publish("orders.created", 1)
    await bus.publish("orders.updated", 2)
    await bus.publish("other.topic", 3)
    events = await stream.take(2).collect()
    assert [event.data for event in events] == [1, 2]
    assert all(event.topic.startswith("orders.") for event in events)
    assert bus.subscriber_count == 0


async def test_handler_errors_are_isolated_and_reported() -> None:
    errors: list[str] = []
    bus = EventBus(on_error=lambda error, subscription: errors.append(str(error)))
    good: list[int] = []

    @bus.subscribe("work")
    async def bad(event):
        raise ValueError("handler failed")

    bus.subscribe("work", lambda event: good.append(event.data))

    await bus.publish("work", 1)
    await bus.aclose(drain=True)
    assert errors == ["handler failed"]
    assert good == [1]
    assert bus.stats()["failed"] == 1


async def test_events_are_delivered_in_order() -> None:
    bus = EventBus()
    seen: list[int] = []

    async def handler(event):
        await asyncio.sleep(0.001)
        seen.append(event.data)

    bus.subscribe("counter", handler)
    for number in range(5):
        await bus.publish("counter", number)
    await bus.aclose(drain=True)
    assert seen == [0, 1, 2, 3, 4]


async def test_stream_subscription_backpressure_error() -> None:
    bus = EventBus(buffer_size=1, overflow="error")
    stream = bus.stream("work")
    await bus.publish("work", 1)
    with pytest.raises(BufferOverflowError):
        await bus.publish("work", 2)
    events = await stream.take(1).collect()
    assert [event.data for event in events] == [1]


async def test_unsubscribe_stops_delivery() -> None:
    bus = EventBus()
    received: list[int] = []
    subscription = bus.subscribe("x", lambda event: received.append(event.data))
    await bus.unsubscribe(subscription)
    assert await bus.publish("x", 1) == 0
    await bus.aclose(drain=True)
    assert received == []


async def test_publish_after_close_raises() -> None:
    bus = EventBus()
    await bus.aclose()
    with pytest.raises(StreamClosedError):
        await bus.publish("x", 1)


async def test_subscribe_after_close_raises() -> None:
    bus = EventBus()
    await bus.aclose()
    with pytest.raises(StreamClosedError):
        bus.subscribe("x", lambda event: None)


async def test_invalid_pattern_and_handler() -> None:
    bus = EventBus()
    with pytest.raises(ConfigurationError):
        bus.subscribe("")
    with pytest.raises(TypeError):
        bus.subscribe("x", 42)  # type: ignore[arg-type]
    await bus.aclose()


async def test_event_envelope_is_immutable_and_normalized() -> None:
    event = Event(topic="user.created", data={"id": 1}, metadata={"request_id": "abc"})
    assert event.timestamp.tzinfo is not None
    assert event.timestamp.tzinfo == UTC
    assert event.id
    with pytest.raises(TypeError):
        event.metadata["other"] = 1  # type: ignore[index]
    with pytest.raises(AttributeError):
        event.topic = "other"  # type: ignore[misc]
    assert event.to_dict()["metadata"] == {"request_id": "abc"}
    with pytest.raises(ConfigurationError):
        Event(topic="", data=None)


async def test_bus_stats() -> None:
    bus = EventBus()
    received: list[int] = []
    bus.subscribe("x", lambda event: received.append(event.data))
    await bus.publish("x", 1)
    await bus.aclose(drain=True)
    stats = bus.stats()
    assert stats["published"] == 1
    assert stats["delivered"] == 1
    assert stats["closed"] is True


async def test_event_bus_context_manager_drains() -> None:
    seen: list[int] = []
    async with EventBus() as bus:
        bus.subscribe("x", lambda event: seen.append(event.data))

        async def delayed_publish():
            await asyncio.sleep(0.001)
            await bus.publish("x", 7)

        await delayed_publish()
    assert seen == [7]
