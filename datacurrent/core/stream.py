"""The core :class:`Stream` type: one async pipeline, one clear contract.

A ``Stream`` is a lazy, single-consumer async iterable.  Nothing runs until you
iterate it, and every iteration opens the pipeline exactly once.  For multiple
independent consumers use :class:`datacurrent.Channel` or
:class:`datacurrent.EventBus`, which implement explicit fan-out.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable
from dataclasses import replace
from typing import Any, Generic, TypeVar

from ..buffering.queue import BoundedQueue
from ..config import OverflowPolicy, StreamConfig
from ..exceptions import (
    ConfigurationError,
    EmptyStreamError,
    NonReplayableStreamError,
    StreamConsumedError,
)
from ..observability.metrics import StreamStats, snapshot_stats
from ..reliability.retry import RetryPolicy
from . import operators as ops
from ._util import as_aiterator, maybe_await

T = TypeVar("T")
U = TypeVar("U")
_UNSET = object()

__all__ = ["Stream", "StreamIterator"]


class StreamIterator(Generic[T]):
    """Wrap an async generator with statistics and deterministic ``aclose``."""

    __slots__ = ("_closed", "_exhausted", "_generator", "_stats")

    def __init__(
        self,
        generator: AsyncIterator[T],
        *,
        stats: StreamStats | None = None,
    ) -> None:
        self._generator = generator
        self._stats = stats
        self._closed = False
        self._exhausted = False

    def __aiter__(self) -> StreamIterator[T]:
        return self

    async def __anext__(self) -> T:
        if self._closed:
            raise StopAsyncIteration
        try:
            item = await self._generator.__anext__()
        except StopAsyncIteration:
            self._closed = True
            self._exhausted = True
            raise
        except asyncio.CancelledError:
            raise
        except Exception:
            if self._stats is not None:
                self._stats.errors += 1
            self._closed = True
            raise
        if self._stats is not None:
            self._stats.items_out += 1
        return item

    async def aclose(self) -> None:
        """Close the underlying generator; idempotent and cancellation-safe."""
        if self._exhausted:
            return
        self._exhausted = True
        self._closed = True
        closer = getattr(self._generator, "aclose", None)
        if closer is None:
            return
        try:
            await closer()
        except (RuntimeError, StopAsyncIteration):  # pragma: no cover - defensive
            return

    async def __aenter__(self) -> StreamIterator[T]:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def exhausted(self) -> bool:
        return self._exhausted

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return f"StreamIterator(closed={self._closed}, exhausted={self._exhausted})"


class Stream(Generic[T]):
    """A lazy asynchronous sequence with composable operators.

    Args:
        source: An iterable, an async iterable, a zero-argument callable that
            returns one of those, or another :class:`Stream`.
        config: Optional :class:`~datacurrent.StreamConfig`.
        replayable: Override replay detection.  Lists and tuples are replayable
            automatically; generators and async generators are not.

    Example::

        from datacurrent import Stream

        stream = Stream.from_iterable([1, 2, 3, 4, 5])
        doubled = stream.filter(lambda n: n % 2).map(lambda n: n * 2)
        async for value in doubled:
            print(value)

    A ``Stream`` is single-consumer: iterating it twice raises
    :class:`~datacurrent.StreamConsumedError`.  Use ``Channel`` or ``EventBus``
    when several consumers must each receive every item.
    """

    __slots__ = ("_config", "_consumed", "_current", "_factory", "_replayable", "_stats")

    def __init__(
        self,
        source: Any,
        *,
        config: StreamConfig | None = None,
        replayable: bool | None = None,
    ) -> None:
        if config is not None and not isinstance(config, StreamConfig):
            raise ConfigurationError(
                f"config must be a StreamConfig instance or None, got {type(config).__name__}"
            )
        self._config: StreamConfig = config or StreamConfig()
        self._consumed = False
        self._current: StreamIterator[T] | None = None
        self._stats = StreamStats(name=self._config.name)

        if isinstance(source, Stream):
            upstream: Stream[Any] = source

            async def _drain_upstream() -> AsyncIterator[Any]:
                async for item in upstream:
                    yield item

            self._factory: Callable[[], AsyncIterator[T]] = _drain_upstream
            self._replayable = False
            return

        if _is_async_iterable(source) or hasattr(source, "__iter__"):
            replayable_flag = (
                bool(replayable)
                if replayable is not None
                else hasattr(source, "__len__") and not _is_async_iterable(source)
            )
            if replayable_flag:
                self._factory = lambda: as_aiterator(source)
            else:
                cached: list[AsyncIterator[Any]] = []

                def _one_shot() -> AsyncIterator[Any]:
                    if not cached:
                        cached.append(as_aiterator(source))
                    return cached[0]

                self._factory = _one_shot
            self._replayable = replayable_flag
            return

        if callable(source):
            factory: Callable[[], Any] = source
            self._factory = lambda: as_aiterator(factory())
            self._replayable = True if replayable is None else bool(replayable)
            return

        raise TypeError(
            "Stream() expects an iterable, an async iterable, a zero-argument "
            f"callable or another Stream, got {type(source).__name__}"
        )

    # ------------------------------------------------------------------ #
    # Construction
    # ------------------------------------------------------------------ #
    @classmethod
    def from_iterable(cls, items: Iterable[T], *, config: StreamConfig | None = None) -> Stream[T]:
        """Stream from a synchronous iterable.

        A list or tuple is replayable (and therefore retryable); a plain
        generator is single-use.
        """
        if not hasattr(items, "__iter__"):
            raise TypeError(f"from_iterable() expects an iterable, got {type(items).__name__}")
        return cls(items, config=config)

    @classmethod
    def from_async_iterable(
        cls, items: AsyncIterable[T], *, config: StreamConfig | None = None
    ) -> Stream[T]:
        """Stream from an async iterable or async generator (single-use)."""
        if not _is_async_iterable(items):
            raise TypeError(
                f"from_async_iterable() expects an async iterable, got {type(items).__name__}"
            )
        return cls(items, config=config)

    @classmethod
    def from_factory(
        cls,
        factory: Callable[[], Any],
        *,
        config: StreamConfig | None = None,
    ) -> Stream[T]:
        """Stream from a zero-argument callable, re-invoked on each retry."""
        if not callable(factory):
            raise TypeError("from_factory() expects a zero-argument callable")
        return cls(factory, config=config, replayable=True)

    @classmethod
    def from_value(cls, value: T, *, config: StreamConfig | None = None) -> Stream[T]:
        """Stream containing exactly one item."""
        return cls([value], config=config)

    @classmethod
    def empty(cls, *, config: StreamConfig | None = None) -> Stream[Any]:
        """An empty stream."""
        return cls([], config=config)

    @classmethod
    def from_queue(
        cls,
        queue: BoundedQueue[T],
        *,
        config: StreamConfig | None = None,
    ) -> Stream[T]:
        """Stream the items of a :class:`BoundedQueue` until it is closed.

        The stream is single-consumer; the queue lifecycle stays with the caller.
        """
        if not isinstance(queue, BoundedQueue):
            raise TypeError(f"from_queue() expects a BoundedQueue, got {type(queue).__name__}")

        async def _drain_queue() -> AsyncIterator[T]:
            async for item in queue:
                yield item

        return cls(_drain_queue, config=config, replayable=False)

    # ------------------------------------------------------------------ #
    # Introspection
    # ------------------------------------------------------------------ #
    @property
    def config(self) -> StreamConfig:
        return self._config

    @property
    def replayable(self) -> bool:
        """Whether the source can be iterated again (needed by ``retry()``)."""
        return self._replayable

    @property
    def consumed(self) -> bool:
        return self._consumed

    @property
    def stats(self) -> dict[str, Any]:
        """Counters for this stream (``observe=True`` collects them)."""
        snapshot = snapshot_stats(self._stats)
        assert snapshot is not None
        return snapshot

    def _label(self) -> str:
        return f" {self._config.name!r}" if self._config.name else ""

    def _derive(
        self,
        factory: Callable[[], AsyncIterator[Any]],
        *,
        name: str | None = None,
        replayable: bool | None = None,
        stats: StreamStats | None = None,
    ) -> Stream[Any]:
        config = self._config if name is None else replace(self._config, name=name)
        stream: Stream[Any] = Stream(
            factory,
            config=config,
            replayable=self._replayable if replayable is None else replayable,
        )
        if stats is not None:
            stream._stats = stats
        return stream

    def _open(self) -> StreamIterator[T]:
        if self._consumed:
            raise StreamConsumedError(
                f"stream{self._label()} has already been consumed; streams are "
                "single-consumer. Build a new stream for a second pass, or use "
                "Channel/EventBus for fan-out."
            )
        self._consumed = True
        generator = self._factory()
        iterator: StreamIterator[T] = StreamIterator(
            generator, stats=self._stats if self._config.observe else None
        )
        self._current = iterator
        return iterator

    def _open_raw(self) -> AsyncIterator[T]:
        """Internal open used by operators; allows replay for retryable sources."""
        if self._consumed and not self._replayable:
            raise NonReplayableStreamError(
                f"stream{self._label()} is single-use and has already been "
                "consumed; wrap the source with Stream.from_factory() to make it "
                "replayable"
            )
        self._consumed = True
        return self._factory()

    # ------------------------------------------------------------------ #
    # Iteration
    # ------------------------------------------------------------------ #
    def __aiter__(self) -> StreamIterator[T]:
        return self._open()

    async def __aenter__(self) -> StreamIterator[T]:
        iterator = self._open()
        self._current = iterator
        return iterator

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close the active iterator and everything upstream of it."""
        current = self._current
        self._current = None
        if current is not None:
            await current.aclose()

    def __repr__(self) -> str:
        state = "consumed" if self._consumed else "fresh"
        kind = "replayable" if self._replayable else "one-shot"
        return f"<Stream{self._label()} {kind}, {state}>"

    # ------------------------------------------------------------------ #
    # Terminal operations
    # ------------------------------------------------------------------ #
    async def collect(self) -> list[T]:
        """Consume the stream and return every item as a list."""
        iterator = self._open()
        items: list[T] = []
        try:
            async for item in iterator:
                items.append(item)
        finally:
            await iterator.aclose()
        return items

    async def count(self) -> int:
        """Consume the stream and return the number of items."""
        iterator = self._open()
        total = 0
        try:
            async for _ in iterator:
                total += 1
        finally:
            await iterator.aclose()
        return total

    async def drain(self) -> int:
        """Consume and discard every item, returning the count."""
        return await self.count()

    async def first(self, default: Any = _UNSET) -> Any:
        """Return the first item.

        Raises:
            EmptyStreamError: The stream is empty and no default was given.
        """
        iterator = self._open()
        try:
            try:
                return await iterator.__anext__()
            except StopAsyncIteration:
                if default is _UNSET:
                    raise EmptyStreamError("stream is empty") from None
                return default
        finally:
            await iterator.aclose()

    async def last(self, default: Any = _UNSET) -> Any:
        """Return the last item, consuming the stream."""
        iterator = self._open()
        found: Any = _UNSET
        try:
            async for item in iterator:
                found = item
        finally:
            await iterator.aclose()
        if found is _UNSET:
            if default is _UNSET:
                raise EmptyStreamError("stream is empty")
            return default
        return found

    async def reduce(self, func: Callable[[Any, T], Any], initial: Any = _UNSET) -> Any:
        """Fold the stream with ``func(accumulator, item)``.

        ``func`` may be a normal function or a coroutine function.  Without an
        initial value the first item seeds the accumulator; an empty stream then
        raises :class:`EmptyStreamError`.
        """
        if not callable(func):
            raise TypeError(f"reduce() requires a callable, got {type(func).__name__}")
        iterator = self._open()
        accumulator: Any = initial
        try:
            async for item in iterator:
                if accumulator is _UNSET:
                    accumulator = item
                else:
                    accumulator = await maybe_await(func(accumulator, item))
        finally:
            await iterator.aclose()
        if accumulator is _UNSET:
            raise EmptyStreamError("cannot reduce an empty stream without an initial value")
        return accumulator

    # ------------------------------------------------------------------ #
    # Operators
    # ------------------------------------------------------------------ #
    def map(
        self,
        func: Callable[[T], Any],
        *,
        concurrency: int | None = None,
        ordered: bool = True,
        name: str | None = None,
    ) -> Stream[Any]:
        """Transform every item; callbacks may be async.

        With ``concurrency > 1`` up to that many callbacks run at once while
        order is preserved (``ordered=False`` yields completion order).
        """
        return ops.op_map(self, func, concurrency=concurrency, ordered=ordered, name=name)

    def filter(self, predicate: Callable[[T], Any], *, name: str | None = None) -> Stream[T]:
        """Keep items for which ``predicate`` is truthy."""
        return ops.op_filter(self, predicate, name=name)

    def flat_map(self, func: Callable[[T], Any], *, name: str | None = None) -> Stream[Any]:
        """Map each item to an iterable and flatten the results in order."""
        return ops.op_flat_map(self, func, name=name)

    def take(self, count: int, *, name: str | None = None) -> Stream[T]:
        """Yield at most ``count`` items, then close upstream."""
        return ops.op_take(self, count, name=name)

    def skip(self, count: int, *, name: str | None = None) -> Stream[T]:
        """Discard the first ``count`` items."""
        return ops.op_skip(self, count, name=name)

    def batch(
        self,
        size: int,
        *,
        partial: bool = True,
        name: str | None = None,
    ) -> Stream[list[T]]:
        """Group items into lists of ``size`` (the last one may be partial)."""
        return ops.op_batch(self, size, partial=partial, name=name)

    def buffer(
        self,
        *,
        size: int | None = None,
        overflow: OverflowPolicy | str | None = None,
        name: str | None = None,
    ) -> Stream[T]:
        """Insert a bounded, backpressured buffer between producer and consumer."""
        return ops.op_buffer(self, size=size, overflow=overflow, name=name)

    def distinct(
        self,
        key: Callable[[T], Any] | None = None,
        *,
        capacity: int | None = None,
        name: str | None = None,
    ) -> Stream[T]:
        """Drop duplicates, optionally remembering only the last ``capacity`` keys."""
        return ops.op_distinct(self, key, capacity=capacity, name=name)

    def enumerate(self, start: int = 0, *, name: str | None = None) -> Stream[tuple[int, T]]:
        """Pair every item with its index."""
        return ops.op_enumerate(self, start, name=name)

    def window(self, size: int, *, step: int = 1, name: str | None = None) -> Stream[list[T]]:
        """Yield sliding windows of ``size`` items, advancing by ``step``."""
        return ops.op_window(self, size, step=step, name=name)

    def debounce(self, seconds: float, *, name: str | None = None) -> Stream[T]:
        """Emit an item after ``seconds`` of silence."""
        return ops.op_debounce(self, seconds, name=name)

    def throttle(self, interval: float, *, name: str | None = None) -> Stream[T]:
        """Emit at most one item per ``interval`` seconds."""
        return ops.op_throttle(self, interval, name=name)

    def timeout(self, seconds: float, *, name: str | None = None) -> Stream[T]:
        """Fail with :class:`StreamTimeoutError` if no item arrives in time."""
        return ops.op_timeout(self, seconds, name=name)

    def retry(
        self,
        policy: RetryPolicy | int | dict[str, Any] | None = None,
        *,
        on: Any = None,
        name: str | None = None,
    ) -> Stream[T]:
        """Restart a replayable upstream when it fails."""
        return ops.op_retry(self, policy, on=on, name=name)

    def recover(self, handler: Callable[[Exception], Any], *, name: str | None = None) -> Stream[T]:
        """Replace a failed upstream with items returned by ``handler``."""
        return ops.op_recover(self, handler, name=name)

    def tap(self, func: Callable[[T], Any], *, name: str | None = None) -> Stream[T]:
        """Run a side effect for every item without changing the items."""
        return ops.op_tap(self, func, name=name)

    def zip(self, *others: Any, name: str | None = None) -> Stream[tuple[Any, ...]]:
        """Combine streams positionally; stops when the first one ends."""
        return ops.op_zip(self, *others, name=name)

    def merge(
        self,
        *others: Any,
        buffer_size: int | None = None,
        overflow: OverflowPolicy | str | None = None,
        name: str | None = None,
    ) -> Stream[Any]:
        """Interleave streams concurrently and yield items as they arrive."""
        return ops.op_merge(
            self,
            *others,
            buffer_size=buffer_size,
            overflow=overflow,
            name=name,
        )

    def concat(self, *others: Any, name: str | None = None) -> Stream[Any]:
        """Consume streams one after another."""
        return ops.op_concat(self, *others, name=name)


def _is_async_iterable(value: Any) -> bool:
    return hasattr(value, "__aiter__")
