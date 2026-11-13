"""Subscriptions for the in-memory event bus."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ..buffering.queue import BoundedQueue
from ..exceptions import QueueClosedError
from .routing import matches

if TYPE_CHECKING:
    from .bus import EventBus

__all__ = ["Subscription"]


@dataclass(slots=True)
class Subscription:
    """A single subscriber: a topic pattern, a bounded queue and a handler.

    ``handler`` is ``None`` for stream subscriptions, where the application
    consumes the queue directly through ``EventBus.stream()``.
    """

    topic: str
    queue: BoundedQueue[Any]
    handler: Callable[[Any], Any] | None
    bus: EventBus
    task: asyncio.Task[Any] | None = None
    active: bool = True
    delivered: int = 0
    failed: int = 0
    in_flight: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def matches(self, topic: str) -> bool:
        return matches(self.topic, topic)

    async def aclose(self) -> None:
        """Stop delivery for this subscription and release its queue."""
        if not self.active and self.task is None:
            return
        self.active = False
        task, self.task = self.task, None
        if task is not None and not task.done():
            task.cancel()
            try:
                await asyncio.gather(task, return_exceptions=True)
            except asyncio.CancelledError:
                raise
        try:
            await self.queue.aclose()
        except QueueClosedError:  # pragma: no cover - already closed
            pass

    def stats(self) -> dict[str, Any]:
        buffer = self.queue.stats()
        return {
            "topic": self.topic,
            "active": self.active,
            "delivered": self.delivered,
            "failed": self.failed,
            "queued": buffer.size,
            "dropped": buffer.dropped,
            "high_watermark": buffer.high_watermark,
        }

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return (
            f"Subscription(topic={self.topic!r}, active={self.active}, "
            f"delivered={self.delivered}, failed={self.failed})"
        )
