"""A small, fast, in-process event bus.

Delivery is **at most once** and entirely in memory: there is no persistence, no
acknowledgement and no replay.  If that is not enough, use a durable broker
adapter; DataCurrent never pretends an in-memory bus is durable.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, Generic, TypeVar, overload

from ..buffering.backpressure import BufferPolicy
from ..config import DEFAULT_BUFFER_SIZE, OverflowPolicy, StreamConfig
from ..core._util import maybe_await
from ..core.stream import Stream
from ..exceptions import (
    ConfigurationError,
    QueueClosedError,
    StreamClosedError,
)
from ..observability.logging import get_logger
from ..observability.metrics import StreamStats
from .routing import validate_pattern, validate_topic
from .subscriptions import Subscription

T = TypeVar("T")

__all__ = ["Event", "EventBus"]

_log = get_logger("events")


@dataclass(frozen=True, slots=True)
class Event(Generic[T]):
    """An immutable event envelope."""

    topic: str
    data: T
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    metadata: Mapping[str, Any] = field(default_factory=dict)
    source: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.topic, str) or not self.topic:
            raise ConfigurationError("event topic must be a non-empty string")
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata or {})))

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "topic": self.topic,
            "data": self.data,
            "timestamp": self.timestamp.isoformat(),
            "metadata": dict(self.metadata),
            "source": self.source,
        }


class EventBus:
    """Fan-out events to handlers and streams.

    Args:
        buffer_size: Per-subscriber queue capacity.
        overflow: Overflow policy; the default ``BLOCK`` applies backpressure.
        on_error: Optional ``callable(error, subscription)`` invoked when a
            handler raises.  Without it, errors are logged and isolated.
        name: Optional bus name used in logs.

    Example::

        bus = EventBus()

        @bus.subscribe("user.*")
        async def on_user(event):
            print(event.topic, event.data)

        await bus.publish("user.created", {"user_id": 123})
        await bus.aclose(drain=True)
    """

    def __init__(
        self,
        *,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        overflow: OverflowPolicy | str = OverflowPolicy.BLOCK,
        on_error: Callable[[Exception, Subscription], Any] | None = None,
        name: str | None = None,
    ) -> None:
        if on_error is not None and not callable(on_error):
            raise TypeError("on_error must be callable or None")
        self._policy = BufferPolicy(size=buffer_size, overflow=OverflowPolicy.coerce(overflow))
        self._on_error = on_error
        self._name = name
        self._subscriptions: list[Subscription] = []
        self._closed = False
        self._published = 0
        self._delivered = 0
        self._failed = 0

    # ------------------------------------------------------------------ #
    # Subscriptions
    # ------------------------------------------------------------------ #
    @overload
    def subscribe(  # pragma: no cover - typing overload
        self,
        topic: str,
        handler: Callable[[Event[Any]], Any],
        *,
        buffer_size: int | None = None,
        overflow: OverflowPolicy | str | None = None,
    ) -> Subscription: ...

    @overload
    def subscribe(  # pragma: no cover - typing overload
        self,
        topic: str = "*",
        handler: None = None,
        *,
        buffer_size: int | None = None,
        overflow: OverflowPolicy | str | None = None,
    ) -> Callable[[Callable[[Event[Any]], Any]], Subscription]: ...

    def subscribe(
        self,
        topic: str = "*",
        handler: Callable[[Event[Any]], Any] | None = None,
        *,
        buffer_size: int | None = None,
        overflow: OverflowPolicy | str | None = None,
    ) -> Subscription | Callable[[Callable[[Event[Any]], Any]], Subscription]:
        """Subscribe a handler.  Also works as a decorator.

        The handler receives an :class:`Event` and may be sync or async.
        Handler errors are isolated per event and reported through ``on_error``.
        """
        validate_pattern(topic)
        if handler is None:

            def decorator(func: Callable[[Event[Any]], Any]) -> Subscription:
                return self._add_subscription(topic, func, buffer_size, overflow)

            return decorator
        if not callable(handler):
            raise TypeError(f"handler must be callable, got {type(handler).__name__}")
        return self._add_subscription(topic, handler, buffer_size, overflow)

    def _add_subscription(
        self,
        topic: str,
        handler: Callable[[Event[Any]], Any] | None,
        buffer_size: int | None,
        overflow: OverflowPolicy | str | None,
    ) -> Subscription:
        if self._closed:
            raise StreamClosedError(f"event bus {self._name!r} is closed")
        resolved_overflow = OverflowPolicy.coerce(
            self._policy.overflow if overflow is None else overflow
        )
        policy = BufferPolicy(
            size=self._policy.size if buffer_size is None else buffer_size,
            overflow=resolved_overflow,
        )
        queue = policy.build(name=f"eventbus:{topic}")
        subscription = Subscription(topic=topic, queue=queue, handler=handler, bus=self)
        self._subscriptions.append(subscription)
        if handler is not None:
            self._ensure_worker(subscription)
        return subscription

    def stream(
        self,
        topic: str = "*",
        *,
        buffer_size: int | None = None,
        overflow: OverflowPolicy | str | None = None,
        name: str | None = None,
    ) -> Stream[Event[Any]]:
        """Consume matching events as a single-consumer stream."""
        subscription = self._add_subscription(topic, None, buffer_size, overflow)
        stats = StreamStats(name=name or f"eventbus:{topic}")
        config = StreamConfig(
            name=name or f"eventbus:{topic}",
            buffer_size=self._policy.size if buffer_size is None else buffer_size,
            overflow=OverflowPolicy.coerce(self._policy.overflow if overflow is None else overflow),
            observe=True,
        )

        async def factory() -> AsyncIterator[Event[Any]]:
            # The stream iterator owns items_out (config.observe=True); counting
            # here as well would report every event twice.
            try:
                async for event in subscription.queue:
                    yield event
            finally:
                await self.unsubscribe(subscription)

        stream: Stream[Event[Any]] = Stream(factory, config=config, replayable=False)
        stream._stats = stats
        return stream

    async def unsubscribe(self, subscription: Subscription) -> None:
        """Remove a subscription; safe to call more than once."""
        if subscription in self._subscriptions:
            self._subscriptions.remove(subscription)
        await subscription.aclose()

    @property
    def subscriber_count(self) -> int:
        return sum(1 for subscription in self._subscriptions if subscription.active)

    @property
    def subscriptions(self) -> tuple[Subscription, ...]:
        return tuple(self._subscriptions)

    # ------------------------------------------------------------------ #
    # Publishing
    # ------------------------------------------------------------------ #
    async def publish(
        self,
        topic: str,
        data: Any = None,
        *,
        metadata: Mapping[str, Any] | None = None,
        event_id: str | None = None,
        source: str | None = None,
    ) -> int:
        """Publish ``data`` on ``topic``; returns the number of subscribers reached."""
        event: Event[Any] = Event(
            topic=validate_topic(topic),
            data=data,
            metadata=dict(metadata or {}),
            id=event_id or uuid.uuid4().hex,
            source=source,
        )
        return await self.publish_event(event)

    async def publish_event(self, event: Event[Any]) -> int:
        """Publish a pre-built event."""
        if self._closed:
            raise StreamClosedError(f"event bus {self._name!r} is closed")
        if not isinstance(event, Event):
            raise TypeError(f"publish_event() expects an Event, got {type(event).__name__}")
        self._published += 1
        delivered = 0
        for subscription in list(self._subscriptions):
            if not subscription.active or not subscription.matches(event.topic):
                continue
            if subscription.handler is not None:
                self._ensure_worker(subscription)
            try:
                stored = await subscription.queue.put(event)
            except QueueClosedError:
                subscription.active = False
                continue
            if stored:
                delivered += 1
        self._delivered += delivered
        return delivered

    def _ensure_worker(self, subscription: Subscription) -> None:
        if subscription.task is not None and not subscription.task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # pragma: no cover - publish() always runs in a loop
            return
        subscription.task = loop.create_task(
            self._dispatch(subscription),
            name=f"datacurrent-eventbus-{subscription.topic}",
        )

    async def _dispatch(self, subscription: Subscription) -> None:
        handler = subscription.handler
        if handler is None:  # pragma: no cover - stream subscriptions have no worker
            return
        try:
            async for event in subscription.queue:
                subscription.in_flight += 1
                try:
                    await maybe_await(handler(event))
                    subscription.delivered += 1
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    subscription.failed += 1
                    self._failed += 1
                    await self._report(error, subscription)
                finally:
                    subscription.in_flight -= 1
        except asyncio.CancelledError:
            raise
        except Exception as error:  # pragma: no cover - queue surfaced a failure
            await self._report(error, subscription)

    async def _report(self, error: Exception, subscription: Subscription) -> None:
        if self._on_error is not None:
            try:
                await maybe_await(self._on_error(error, subscription))
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                _log.exception("on_error handler failed")
                return
        _log.error(
            "event handler failed topic=%s subscription=%r: %r",
            subscription.topic,
            subscription,
            error,
        )

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def aclose(
        self,
        *,
        drain: bool = False,
        timeout: float = 5.0,  # noqa: ASYNC109 - drain deadline, not a single await
    ) -> None:
        """Close the bus.

        With ``drain=True`` queued events are processed before shutting down.
        """
        if self._closed:
            return
        if drain:
            await self._drain(timeout)
        self._closed = True
        subscriptions = list(self._subscriptions)
        self._subscriptions.clear()
        for subscription in subscriptions:
            await subscription.aclose()

    async def _drain(self, timeout: float) -> None:  # noqa: ASYNC109 - polling deadline
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            pending = [
                subscription
                for subscription in self._subscriptions
                if subscription.active
                and (subscription.queue.qsize() > 0 or subscription.in_flight > 0)
            ]
            if not pending:
                return
            if loop.time() >= deadline:
                _log.warning("event bus drain timed out with %d queued event(s)", len(pending))
                return
            await asyncio.sleep(0.005)

    async def __aenter__(self) -> EventBus:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose(drain=True)

    def stats(self) -> dict[str, Any]:
        dropped = sum(subscription.queue.stats().dropped for subscription in self._subscriptions)
        return {
            "name": self._name,
            "published": self._published,
            "delivered": self._delivered,
            "failed": self._failed,
            "dropped": dropped,
            "subscribers": self.subscriber_count,
            "closed": self._closed,
        }

    def __repr__(self) -> str:
        return (
            f"EventBus(name={self._name!r}, subscribers={self.subscriber_count}, "
            f"published={self._published})"
        )
