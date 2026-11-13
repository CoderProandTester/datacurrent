"""In-memory fan-out with explicit delivery semantics.

Use :class:`Channel` when several consumers must each receive every message.
A ``Stream`` is single-consumer; a channel gives every subscriber its own bounded
queue.  Delivery is *at most once* and lives only in this process: a subscriber
that is not connected when a message is published will not see it.
"""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from ..buffering.backpressure import BufferPolicy
from ..buffering.queue import BoundedQueue
from ..config import OverflowPolicy, StreamConfig
from ..core.stream import Stream
from ..exceptions import QueueClosedError, StreamClosedError
from ..observability.metrics import StreamStats

T = TypeVar("T")

__all__ = ["Channel"]


class Channel(Generic[T]):
    """A bounded broadcast channel.

    Args:
        buffer_size: Per-subscriber queue capacity.
        overflow: Policy used when a subscriber queue is full.  The default
            ``BLOCK`` applies backpressure to publishers instead of losing data.
        name: Optional name used in reprs and errors.

    Example::

        channel = Channel(buffer_size=32)
        first = channel.subscribe()
        second = channel.subscribe()

        await channel.publish({"n": 1})   # both subscribers receive it

        await channel.aclose()
    """

    def __init__(
        self,
        *,
        buffer_size: int = 256,
        overflow: OverflowPolicy | str = OverflowPolicy.BLOCK,
        name: str | None = None,
    ) -> None:
        self._policy = BufferPolicy(size=buffer_size, overflow=OverflowPolicy.coerce(overflow))
        self._name = name
        self._subscribers: set[BoundedQueue[T]] = set()
        self._closed = False
        self._published = 0
        self._delivered = 0
        self._dropped = 0

    @property
    def name(self) -> str | None:
        return self._name

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    @property
    def closed(self) -> bool:
        return self._closed

    def subscribe(
        self,
        *,
        buffer_size: int | None = None,
        overflow: OverflowPolicy | str | None = None,
        name: str | None = None,
    ) -> Stream[T]:
        """Create a new single-consumer stream of published messages."""
        if self._closed:
            raise StreamClosedError(f"channel {self._name!r} is closed")
        policy = BufferPolicy(
            size=self._policy.size if buffer_size is None else buffer_size,
            overflow=OverflowPolicy.coerce(self._policy.overflow if overflow is None else overflow),
        )

        def record_drop() -> None:
            self._dropped += 1

        queue: BoundedQueue[T] = policy.build(name=self._name or "channel", on_drop=record_drop)
        self._subscribers.add(queue)
        stats = StreamStats(name=name or self._name or "channel")
        config = StreamConfig(
            name=name or self._name,
            buffer_size=policy.size,
            overflow=policy.overflow,
            observe=True,
        )

        channel = self

        async def factory() -> Any:
            try:
                async for item in queue:
                    stats.items_out += 1
                    yield item
            finally:
                channel._subscribers.discard(queue)
                await queue.aclose()

        stream: Stream[T] = Stream(factory, config=config, replayable=False)
        stream._stats = stats
        return stream

    async def publish(self, item: T) -> int:
        """Deliver ``item`` to every active subscriber.

        Returns the number of subscriber queues that accepted the item.  A
        subscriber whose queue is closed is removed automatically.
        """
        if self._closed:
            raise StreamClosedError(f"channel {self._name!r} is closed")
        self._published += 1
        delivered = 0
        for queue in list(self._subscribers):
            try:
                if await queue.put(item):
                    delivered += 1
            except QueueClosedError:
                self._subscribers.discard(queue)
        self._delivered += delivered
        return delivered

    async def publish_many(self, items: Any) -> int:
        """Publish every item in ``items``; returns the number of deliveries."""
        total = 0
        for item in items:
            total += await self.publish(item)
        return total

    async def aclose(self) -> None:
        """Close the channel and every subscriber queue."""
        if self._closed:
            return
        self._closed = True
        subscribers = list(self._subscribers)
        self._subscribers.clear()
        for queue in subscribers:
            await queue.aclose()

    async def __aenter__(self) -> Channel[T]:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    def stats(self) -> dict[str, Any]:
        return {
            "name": self._name,
            "published": self._published,
            "delivered": self._delivered,
            "dropped": self._dropped,
            "subscribers": len(self._subscribers),
            "closed": self._closed,
        }

    def __repr__(self) -> str:
        return (
            f"Channel(name={self._name!r}, subscribers={len(self._subscribers)}, "
            f"buffer_size={self._policy.size}, overflow={self._policy.overflow.value!r})"
        )
