from __future__ import annotations

import asyncio
from typing import Any

import pytest

from datacurrent import Event, EventBus, Stream
from datacurrent.events.routing import matches, validate_pattern, validate_topic
from datacurrent.exceptions import ConfigurationError
from datacurrent.integrations.fastapi import websocket_messages, websocket_stream
from datacurrent.integrations.starlette import StreamLifespan, streaming_response


# --------------------------------------------------------------------------- #
# Routing
# --------------------------------------------------------------------------- #
def test_routing_edge_cases() -> None:
    with pytest.raises(ConfigurationError):
        validate_topic(123)  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError):
        validate_pattern(None)  # type: ignore[arg-type]
    assert not matches("a.b", "a")


# --------------------------------------------------------------------------- #
# Event bus
# --------------------------------------------------------------------------- #
async def test_event_bus_validation_and_closed_paths() -> None:
    with pytest.raises(TypeError):
        EventBus(on_error="not-callable")  # type: ignore[arg-type]

    bus = EventBus(name="edge")
    assert bus.subscriptions == ()
    with pytest.raises(TypeError):
        await bus.publish_event("not-an-event")  # type: ignore[arg-type]

    subscription = bus.subscribe("edge.*", lambda event: None)
    assert bus.subscriptions == (subscription,)
    await subscription.queue.aclose()
    assert await bus.publish("edge.event", 1) == 0
    assert subscription.active is False

    await bus.aclose()
    await bus.aclose()
    assert "EventBus" in repr(bus)


async def test_event_bus_handler_error_reporting_paths() -> None:
    async def fail(event: Event[Any]) -> None:
        raise ValueError("handler failed")

    reported: list[Exception] = []

    async def collect_error(error: Exception, subscription: Any) -> None:
        reported.append(error)

    bus = EventBus(on_error=collect_error)
    bus.subscribe("errors", fail)
    await bus.publish("errors", 1)
    await asyncio.sleep(0.02)
    assert isinstance(reported[0], ValueError)
    await bus.aclose()

    async def broken_reporter(error: Exception, subscription: Any) -> None:
        raise RuntimeError("reporter failed")

    bus = EventBus(on_error=broken_reporter)
    bus.subscribe("errors", fail)
    await bus.publish("errors", 1)
    await asyncio.sleep(0.02)
    await bus.aclose()

    async def cancelled_reporter(error: Exception, subscription: Any) -> None:
        raise asyncio.CancelledError

    bus = EventBus(on_error=cancelled_reporter)
    bus.subscribe("errors", fail)
    await bus.publish("errors", 1)
    await asyncio.sleep(0.02)
    await bus.aclose()

    async def cancel_handler(event: Event[Any]) -> None:
        raise asyncio.CancelledError

    bus = EventBus()
    bus.subscribe("cancel", cancel_handler)
    await bus.publish("cancel", 1)
    await asyncio.sleep(0.02)
    await bus.aclose()

    bus = EventBus()
    bus.subscribe("errors", fail)
    await bus.publish("errors", 1)
    await asyncio.sleep(0.02)
    await bus.aclose()


async def test_event_bus_worker_exits_when_queue_closes() -> None:
    """Closing the subscription queue ends the worker loop normally."""
    bus = EventBus()
    handled = asyncio.Event()

    async def handler(event: Event[Any]) -> None:
        handled.set()

    subscription = bus.subscribe("worker", handler)
    await bus.publish("worker", 1)
    await asyncio.wait_for(handled.wait(), timeout=2)

    task = subscription.task
    assert task is not None
    await subscription.queue.aclose()
    await asyncio.wait_for(task, timeout=2)
    assert task.cancelled() is False
    assert task.exception() is None
    await bus.aclose()


async def test_event_bus_stream_closed_without_events() -> None:
    bus = EventBus()
    stream = bus.stream("empty")
    await bus.aclose()
    assert await stream.collect() == []


async def test_event_bus_drop_newest_counts_drops() -> None:
    bus = EventBus(buffer_size=1, overflow="drop_newest")
    stream = bus.stream("drop", buffer_size=1, overflow="drop_newest")
    assert await bus.publish("drop", 1) == 1
    assert await bus.publish("drop", 2) == 0
    assert bus.stats()["dropped"] == 1
    received = await stream.take(1).collect()
    assert [event.data for event in received] == [1]
    await bus.aclose()


async def test_event_bus_drain_timeout_and_context() -> None:
    started = asyncio.Event()

    async def slow(event: Event[Any]) -> None:
        started.set()
        await asyncio.sleep(10)

    bus = EventBus(buffer_size=4)
    bus.subscribe("slow", slow)
    await bus.publish("slow", 1)
    await started.wait()
    await bus.aclose(drain=True, timeout=0)
    assert bus.stats()["closed"] is True

    async with EventBus(name="context") as scoped:
        await scoped.publish("nothing", 1)
    assert scoped.stats()["closed"] is True


async def test_subscription_stats_and_cancelled_close(monkeypatch: pytest.MonkeyPatch) -> None:
    bus = EventBus()
    started = asyncio.Event()

    async def handler(event: Event[Any]) -> None:
        started.set()
        await asyncio.sleep(10)

    subscription = bus.subscribe("stats", handler)
    await bus.publish("stats", 1)
    await started.wait()
    assert subscription.stats()["topic"] == "stats"

    task = subscription.task
    assert task is not None

    async def cancelled_gather(*args: Any, **kwargs: Any) -> None:
        raise asyncio.CancelledError

    with monkeypatch.context() as context:
        context.setattr("datacurrent.events.subscriptions.asyncio.gather", cancelled_gather)
        with pytest.raises(asyncio.CancelledError):
            await subscription.aclose()

    await asyncio.gather(task, return_exceptions=True)
    await subscription.queue.aclose()
    await bus.aclose()


# --------------------------------------------------------------------------- #
# Integrations
# --------------------------------------------------------------------------- #
class _FakeWebSocket:
    def __init__(self, messages: list[dict[str, Any]]) -> None:
        self.messages = list(messages)
        self.closed = False

    async def receive(self) -> dict[str, Any]:
        return self.messages.pop(0)

    def close(self) -> None:
        self.closed = True


class _NoCloseWebSocket:
    def __init__(self, messages: list[dict[str, Any]]) -> None:
        self.messages = list(messages)

    async def receive(self) -> dict[str, Any]:
        return self.messages.pop(0)


async def test_fastapi_websocket_without_close_method() -> None:
    websocket = _NoCloseWebSocket(
        [
            {"type": "websocket.receive", "text": "no-close"},
            {"type": "websocket.disconnect"},
        ]
    )
    assert await websocket_stream(websocket).collect() == ["no-close"]


async def test_fastapi_websocket_helpers() -> None:
    websocket = _FakeWebSocket(
        [
            {"type": "websocket.receive", "text": "hello"},
            {"type": "websocket.receive", "bytes": b"binary"},
            {"type": "websocket.receive"},
            {"type": "websocket.disconnect"},
        ]
    )
    assert [message async for message in websocket_messages(websocket)] == ["hello", b"binary"]

    websocket = _FakeWebSocket(
        [
            {"type": "websocket.receive", "text": "streamed"},
            {"type": "websocket.disconnect"},
        ]
    )
    stream = websocket_stream(websocket)
    assert isinstance(stream, Stream)
    assert await stream.collect() == ["streamed"]
    assert websocket.closed is True


async def test_starlette_streaming_response_and_lifespan() -> None:
    pytest.importorskip("starlette")
    closed: list[bool] = []

    async def source():
        try:
            yield "text"
            yield b"bytes"
        finally:
            closed.append(True)

    response = streaming_response(
        source(),
        media_type="text/plain",
        headers={"X-Test": "yes"},
        status_code=201,
    )
    chunks = [chunk async for chunk in response.body_iterator]
    assert chunks == [b"text", b"bytes"]
    assert closed == [True]

    streamed = streaming_response(Stream.from_iterable(["a", b"b"]))
    assert [chunk async for chunk in streamed.body_iterator] == [b"a", b"b"]

    events: list[str] = []

    class Resource:
        def __init__(self, name: str) -> None:
            self.name = name

        async def aclose(self) -> None:
            events.append(self.name)

    lifespan = StreamLifespan(Resource("first"), Resource("second"))
    assert lifespan.as_context_manager() is lifespan
    async with lifespan:
        pass
    assert events == ["second", "first"]
