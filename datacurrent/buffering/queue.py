"""Bounded asynchronous queues with explicit overflow policies.

The queue is the backpressure primitive used by ``Stream.buffer()``,
``Stream.merge()``, :class:`~datacurrent.Channel` and the event bus.  It never
silently drops data: the default policy blocks the producer.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from ..config import DEFAULT_BUFFER_SIZE, OverflowPolicy
from ..exceptions import (
    BufferOverflowError,
    ConfigurationError,
    QueueClosedError,
    QueueEmptyError,
)
from ..observability.logging import get_logger

T = TypeVar("T")

_EMPTY = object()
_log = get_logger("buffering")

__all__ = ["BoundedQueue", "BufferStats"]


@dataclass(frozen=True, slots=True)
class BufferStats:
    """Point-in-time snapshot of a queue."""

    size: int
    maxsize: int
    enqueued: int
    dequeued: int
    dropped: int
    overflow: int
    high_watermark: int
    closed: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "size": self.size,
            "maxsize": self.maxsize,
            "enqueued": self.enqueued,
            "dequeued": self.dequeued,
            "dropped": self.dropped,
            "overflow": self.overflow,
            "high_watermark": self.high_watermark,
            "closed": self.closed,
        }


class BoundedQueue(Generic[T]):
    """An asyncio queue with a bounded size and a configurable overflow policy.

    Args:
        maxsize: Maximum number of buffered items (must be >= 1).
        overflow: Policy applied when the buffer is full.  ``BLOCK`` applies
            backpressure, ``DROP_NEWEST`` / ``DROP_OLDEST`` discard one item per
            overflow, and ``ERROR`` raises :class:`BufferOverflowError`.
        name: Optional name used in logging and error messages.
        on_drop: Called once per dropped item (used for stream statistics).

    The queue is intended for use from a single event loop.  It is not thread
    safe and does not perform cross-loop wakeups.
    """

    __slots__ = (
        "_closed",
        "_cond",
        "_dequeued",
        "_dropped",
        "_enqueued",
        "_error",
        "_high_watermark",
        "_items",
        "_maxsize",
        "_name",
        "_on_drop",
        "_overflow",
        "_overflow_count",
    )

    def __init__(
        self,
        maxsize: int = DEFAULT_BUFFER_SIZE,
        *,
        overflow: OverflowPolicy | str = OverflowPolicy.BLOCK,
        name: str | None = None,
        on_drop: Callable[[], None] | None = None,
    ) -> None:
        if isinstance(maxsize, bool) or not isinstance(maxsize, int):
            raise ConfigurationError(f"maxsize must be an integer, got {type(maxsize).__name__}")
        if maxsize < 1:
            raise ConfigurationError(f"maxsize must be >= 1, got {maxsize}")
        self._items: deque[T] = deque()
        self._maxsize = maxsize
        self._overflow = OverflowPolicy.coerce(overflow)
        self._cond = asyncio.Condition()
        self._closed = False
        self._error: BaseException | None = None
        self._name = name
        self._on_drop = on_drop
        self._enqueued = 0
        self._dequeued = 0
        self._dropped = 0
        self._overflow_count = 0
        self._high_watermark = 0

    # ------------------------------------------------------------------ #
    # Introspection
    # ------------------------------------------------------------------ #
    @property
    def name(self) -> str | None:
        return self._name

    @property
    def maxsize(self) -> int:
        return self._maxsize

    @property
    def overflow(self) -> OverflowPolicy:
        return self._overflow

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def error(self) -> BaseException | None:
        """The failure that terminated the queue, if any."""
        return self._error

    def qsize(self) -> int:
        return len(self._items)

    def empty(self) -> bool:
        return not self._items

    def full(self) -> bool:
        return len(self._items) >= self._maxsize

    def stats(self) -> BufferStats:
        return BufferStats(
            size=len(self._items),
            maxsize=self._maxsize,
            enqueued=self._enqueued,
            dequeued=self._dequeued,
            dropped=self._dropped,
            overflow=self._overflow_count,
            high_watermark=self._high_watermark,
            closed=self._closed,
        )

    def _label(self) -> str:
        return f" {self._name!r}" if self._name else ""

    def _record_drop(self) -> None:
        self._dropped += 1
        self._overflow_count += 1
        if self._on_drop is not None:
            self._on_drop()

    # ------------------------------------------------------------------ #
    # Writing
    # ------------------------------------------------------------------ #
    async def put(self, item: T) -> bool:
        """Insert ``item``.

        Returns:
            ``True`` if the item was stored, ``False`` if it was dropped by a
            drop policy.

        Raises:
            QueueClosedError: The queue has been closed.
            BufferOverflowError: The queue is full and the policy is ``ERROR``.
        """
        async with self._cond:
            while True:
                if self._closed:
                    raise QueueClosedError(f"queue{self._label()} is closed")
                if len(self._items) < self._maxsize:
                    break
                policy = self._overflow
                if policy is OverflowPolicy.DROP_NEWEST:
                    self._record_drop()
                    self._cond.notify_all()
                    return False
                if policy is OverflowPolicy.DROP_OLDEST:
                    self._items.popleft()
                    self._record_drop()
                    break
                if policy is OverflowPolicy.ERROR:
                    self._overflow_count += 1
                    raise BufferOverflowError(self._maxsize, policy=policy, dropped=self._dropped)
                await self._cond.wait()

            self._items.append(item)
            self._enqueued += 1
            if len(self._items) > self._high_watermark:
                self._high_watermark = len(self._items)
            self._cond.notify_all()
            return True

    async def put_many(self, items: Any) -> int:
        """Insert every item from an iterable, returning the number stored."""
        stored = 0
        for item in items:
            if await self.put(item):
                stored += 1
        return stored

    # ------------------------------------------------------------------ #
    # Reading
    # ------------------------------------------------------------------ #
    async def _take(self, default: Any) -> Any:
        async with self._cond:
            while not self._items:
                if self._error is not None:
                    raise self._error
                if self._closed:
                    return default
                await self._cond.wait()
            item = self._items.popleft()
            self._dequeued += 1
            self._cond.notify_all()
            return item

    async def get(self) -> T:
        """Return the oldest item, waiting if necessary.

        Raises:
            QueueClosedError: The queue is closed and empty.
        """
        item = await self._take(_EMPTY)
        if item is _EMPTY:
            raise QueueClosedError(f"queue{self._label()} is closed and empty")
        return item

    async def get_nowait(self) -> T:
        """Return the oldest item without waiting.

        Raises:
            QueueEmptyError: No item is buffered.
        """
        async with self._cond:
            if not self._items:
                if self._error is not None:
                    raise self._error
                raise QueueEmptyError(f"queue{self._label()} is empty")
            item = self._items.popleft()
            self._dequeued += 1
            self._cond.notify_all()
            return item

    def __aiter__(self) -> AsyncIterator[T]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[T]:
        while True:
            item = await self._take(_EMPTY)
            if item is _EMPTY:
                return
            yield item

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def aclose(self) -> None:
        """Close the queue.  Buffered items can still be drained."""
        async with self._cond:
            if self._closed:
                return
            self._closed = True
            self._cond.notify_all()

    async def fail(self, error: BaseException) -> None:
        """Store ``error`` and close the queue; consumers re-raise it when drained."""
        if not isinstance(error, BaseException):
            raise TypeError("error must be an exception instance")
        async with self._cond:
            if self._error is None:
                self._error = error
            self._closed = True
            self._cond.notify_all()
        _log.debug("queue%s failed: %r", self._label(), error)

    async def __aenter__(self) -> BoundedQueue[T]:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    def __repr__(self) -> str:
        return (
            f"BoundedQueue(size={len(self._items)}, maxsize={self._maxsize}, "
            f"overflow={self._overflow.value!r}, closed={self._closed})"
        )
