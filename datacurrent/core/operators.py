"""Lazy, cancellation-safe stream operators.

Every operator returns a new :class:`~datacurrent.Stream` backed by an async
generator factory.  Opening the derived stream pulls from the upstream exactly
once, and closing it closes every upstream iterator, so ``break``, exceptions
and task cancellation never leak generators or background tasks.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Callable, Iterable
from typing import TYPE_CHECKING, Any, TypeVar

from ..buffering.backpressure import BufferPolicy
from ..buffering.queue import BoundedQueue
from ..config import OverflowPolicy
from ..exceptions import (
    ConfigurationError,
    NonReplayableStreamError,
    QueueClosedError,
    StreamTimeoutError,
)
from ..observability.metrics import StreamStats
from ..reliability.retry import RetryPolicy, is_retryable
from ..reliability.timeout import validate_timeout
from ._util import aclose, as_aiterator, maybe_await

if TYPE_CHECKING:
    from .stream import Stream

T = TypeVar("T")
U = TypeVar("U")
_UNSET = object()

__all__ = [
    "op_batch",
    "op_buffer",
    "op_concat",
    "op_debounce",
    "op_distinct",
    "op_enumerate",
    "op_filter",
    "op_flat_map",
    "op_map",
    "op_merge",
    "op_recover",
    "op_retry",
    "op_skip",
    "op_take",
    "op_tap",
    "op_throttle",
    "op_timeout",
    "op_window",
    "op_zip",
]


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _require_callable(func: Any, operator: str) -> Callable[..., Any]:
    if not callable(func):
        raise TypeError(f"{operator}() requires a callable, got {type(func).__name__}")
    return func


def _require_int(value: Any, name: str, *, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigurationError(f"{name} must be an integer, got {type(value).__name__}")
    if value < minimum:
        raise ConfigurationError(f"{name} must be >= {minimum}, got {value}")
    return value


async def _next_item(iterator: AsyncIterator[Any]) -> Any:
    """Await the next item from an async iterator (typed for create_task)."""
    return await iterator.__anext__()


def _as_stream(value: Any, config: Any) -> Stream[Any]:
    """Coerce a Stream, iterable or async iterable into a Stream."""
    from .stream import Stream

    if isinstance(value, Stream):
        return value
    return Stream(value, config=config)


# --------------------------------------------------------------------------- #
# map / filter / flat_map / tap
# --------------------------------------------------------------------------- #
def op_map(
    upstream: Stream[T],
    func: Callable[[T], Any],
    *,
    concurrency: int | None = None,
    ordered: bool = True,
    name: str | None = None,
) -> Stream[Any]:
    _require_callable(func, "map")
    limit = (
        upstream.config.concurrency
        if concurrency is None
        else _require_int(concurrency, "concurrency", minimum=1)
    )
    if not isinstance(ordered, bool):
        raise ConfigurationError("ordered must be a bool")

    def factory() -> AsyncIterator[Any]:
        return _map_gen(upstream, func, limit, ordered)

    return upstream._derive(factory, name=name)


async def _map_gen(
    upstream: Stream[Any],
    func: Callable[[Any], Any],
    limit: int,
    ordered: bool,
) -> AsyncIterator[Any]:
    source = upstream._open_raw()
    try:
        if limit == 1:
            async for item in source:
                yield await maybe_await(func(item))
        else:
            async for result in _concurrent_map(source, func, limit, ordered):
                yield result
    finally:
        await aclose(source)


async def _invoke(func: Callable[[Any], Any], item: Any) -> Any:
    """Run a callback inside a task.

    The inner awaitable is created only when the task actually starts, so
    cancelling a task before its first step can never leave an un-awaited
    coroutine behind.
    """
    return await maybe_await(func(item))


async def _concurrent_map(
    source: AsyncIterator[Any],
    func: Callable[[Any], Any],
    limit: int,
    ordered: bool,
) -> AsyncIterator[Any]:
    tasks: deque[asyncio.Task[Any]] = deque()
    exhausted = False
    try:
        while len(tasks) < limit and not exhausted:
            try:
                item = await source.__anext__()
            except StopAsyncIteration:
                exhausted = True
                break
            tasks.append(asyncio.create_task(_invoke(func, item)))
        while tasks:
            if ordered:
                yield await tasks.popleft()
            else:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    tasks.remove(task)
                    yield task.result()
            if not exhausted:
                try:
                    item = await source.__anext__()
                except StopAsyncIteration:
                    exhausted = True
                else:
                    tasks.append(asyncio.create_task(_invoke(func, item)))
    finally:
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


def op_filter(
    upstream: Stream[T],
    predicate: Callable[[T], Any],
    *,
    name: str | None = None,
) -> Stream[T]:
    _require_callable(predicate, "filter")

    def factory() -> AsyncIterator[T]:
        return _filter_gen(upstream, predicate)

    return upstream._derive(factory, name=name)


async def _filter_gen(upstream: Stream[T], predicate: Callable[[T], Any]) -> AsyncIterator[T]:
    source = upstream._open_raw()
    try:
        async for item in source:
            if await maybe_await(predicate(item)):
                yield item
    finally:
        await aclose(source)


def op_flat_map(
    upstream: Stream[T],
    func: Callable[[T], Any],
    *,
    name: str | None = None,
) -> Stream[Any]:
    _require_callable(func, "flat_map")

    def factory() -> AsyncIterator[Any]:
        return _flat_map_gen(upstream, func)

    return upstream._derive(factory, name=name)


async def _flat_map_gen(upstream: Stream[T], func: Callable[[T], Any]) -> AsyncIterator[Any]:
    source = upstream._open_raw()
    try:
        async for item in source:
            produced = await maybe_await(func(item))
            if produced is None:
                continue
            if isinstance(produced, (str, bytes, bytearray)):
                raise TypeError(
                    "flat_map() callback must return an iterable of items, not "
                    f"{type(produced).__name__}; wrap single values in a list"
                )
            inner = as_aiterator(produced)
            try:
                async for value in inner:
                    yield value
            finally:
                await aclose(inner)
    finally:
        await aclose(source)


def op_tap(
    upstream: Stream[T],
    func: Callable[[T], Any],
    *,
    name: str | None = None,
) -> Stream[T]:
    """Observe items without changing them; side effects may be async."""
    _require_callable(func, "tap")

    def factory() -> AsyncIterator[T]:
        return _tap_gen(upstream, func)

    return upstream._derive(factory, name=name)


async def _tap_gen(upstream: Stream[T], func: Callable[[T], Any]) -> AsyncIterator[T]:
    source = upstream._open_raw()
    try:
        async for item in source:
            await maybe_await(func(item))
            yield item
    finally:
        await aclose(source)


# --------------------------------------------------------------------------- #
# take / skip / batch / buffer
# --------------------------------------------------------------------------- #
def op_take(upstream: Stream[T], count: int, *, name: str | None = None) -> Stream[T]:
    limit = _require_int(count, "count", minimum=0)

    def factory() -> AsyncIterator[T]:
        return _take_gen(upstream, limit)

    return upstream._derive(factory, name=name)


async def _take_gen(upstream: Stream[T], limit: int) -> AsyncIterator[T]:
    if limit == 0:
        return
    source = upstream._open_raw()
    try:
        seen = 0
        while True:
            try:
                item = await source.__anext__()
            except StopAsyncIteration:
                return
            yield item
            seen += 1
            if seen >= limit:
                return
    finally:
        await aclose(source)


def op_skip(upstream: Stream[T], count: int, *, name: str | None = None) -> Stream[T]:
    to_skip = _require_int(count, "count", minimum=0)

    def factory() -> AsyncIterator[T]:
        return _skip_gen(upstream, to_skip)

    return upstream._derive(factory, name=name)


async def _skip_gen(upstream: Stream[T], count: int) -> AsyncIterator[T]:
    source = upstream._open_raw()
    try:
        seen = 0
        async for item in source:
            if seen < count:
                seen += 1
                continue
            yield item
    finally:
        await aclose(source)


def op_batch(
    upstream: Stream[T],
    size: int,
    *,
    partial: bool = True,
    name: str | None = None,
) -> Stream[list[T]]:
    batch_size = _require_int(size, "size", minimum=1)
    if not isinstance(partial, bool):
        raise ConfigurationError("partial must be a bool")

    def factory() -> AsyncIterator[list[T]]:
        return _batch_gen(upstream, batch_size, partial)

    return upstream._derive(factory, name=name)


async def _batch_gen(upstream: Stream[T], size: int, partial: bool) -> AsyncIterator[list[T]]:
    source = upstream._open_raw()
    chunk: list[T] = []
    try:
        async for item in source:
            chunk.append(item)
            if len(chunk) >= size:
                yield chunk
                chunk = []
        if chunk and partial:
            yield chunk
    finally:
        await aclose(source)


def op_buffer(
    upstream: Stream[T],
    *,
    size: int | None = None,
    overflow: OverflowPolicy | str | None = None,
    name: str | None = None,
) -> Stream[T]:
    """Decouple producer and consumer through a bounded queue.

    The producer runs in one background task and suspends when the buffer is
    full (``BLOCK``), which is how a slow consumer applies real backpressure
    instead of growing memory without limit.
    """
    policy = BufferPolicy.from_config(upstream.config, size=size, overflow=overflow)
    stats = StreamStats(name=name or upstream.config.name)

    def factory() -> AsyncIterator[T]:
        return _buffer_gen(upstream, policy, stats)

    return upstream._derive(factory, name=name, stats=stats)


async def _buffer_gen(
    upstream: Stream[T],
    policy: BufferPolicy,
    stats: StreamStats,
) -> AsyncIterator[T]:
    def record_drop() -> None:
        stats.dropped += 1

    queue: BoundedQueue[T] = policy.build(name=upstream.config.name, on_drop=record_drop)
    source = upstream._open_raw()

    async def produce() -> None:
        try:
            async for item in source:
                await queue.put(item)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await queue.fail(error)
        finally:
            await queue.aclose()

    producer = asyncio.create_task(produce(), name="datacurrent-buffer")
    try:
        async for item in queue:
            yield item
    finally:
        producer.cancel()
        await asyncio.gather(producer, return_exceptions=True)
        await queue.aclose()
        await aclose(source)


# --------------------------------------------------------------------------- #
# distinct / enumerate / window / debounce / throttle
# --------------------------------------------------------------------------- #
def op_distinct(
    upstream: Stream[T],
    key: Callable[[T], Any] | None = None,
    *,
    capacity: int | None = None,
    name: str | None = None,
) -> Stream[T]:
    if key is not None:
        _require_callable(key, "distinct")
    if capacity is not None:
        _require_int(capacity, "capacity", minimum=1)

    def factory() -> AsyncIterator[T]:
        return _distinct_gen(upstream, key, capacity)

    return upstream._derive(factory, name=name)


async def _distinct_gen(
    upstream: Stream[T],
    key: Callable[[T], Any] | None,
    capacity: int | None,
) -> AsyncIterator[T]:
    seen: set[Any] = set()
    order: deque[Any] = deque()
    source = upstream._open_raw()
    try:
        async for item in source:
            marker = await maybe_await(key(item)) if key is not None else item
            if marker in seen:
                continue
            seen.add(marker)
            order.append(marker)
            if capacity is not None and len(order) > capacity:
                seen.discard(order.popleft())
            yield item
    finally:
        await aclose(source)


def op_enumerate(
    upstream: Stream[T], start: int = 0, *, name: str | None = None
) -> Stream[tuple[int, T]]:
    _require_int(start, "start", minimum=-(2**63))

    def factory() -> AsyncIterator[tuple[int, T]]:
        return _enumerate_gen(upstream, start)

    return upstream._derive(factory, name=name)


async def _enumerate_gen(upstream: Stream[T], start: int) -> AsyncIterator[tuple[int, T]]:
    source = upstream._open_raw()
    try:
        index = start
        async for item in source:
            yield index, item
            index += 1
    finally:
        await aclose(source)


def op_window(
    upstream: Stream[T],
    size: int,
    *,
    step: int = 1,
    name: str | None = None,
) -> Stream[list[T]]:
    window_size = _require_int(size, "size", minimum=1)
    step_size = _require_int(step, "step", minimum=1)

    def factory() -> AsyncIterator[list[T]]:
        return _window_gen(upstream, window_size, step_size)

    return upstream._derive(factory, name=name)


async def _window_gen(upstream: Stream[T], size: int, step: int) -> AsyncIterator[list[T]]:
    buffer: deque[T] = deque(maxlen=size)
    source = upstream._open_raw()
    try:
        async for item in source:
            buffer.append(item)
            if len(buffer) == size:
                yield list(buffer)
                for _ in range(min(step, len(buffer))):
                    buffer.popleft()
    finally:
        await aclose(source)


def op_debounce(
    upstream: Stream[T],
    seconds: float,
    *,
    name: str | None = None,
) -> Stream[T]:
    """Emit an item once no newer item arrived for ``seconds``."""
    interval = validate_timeout(seconds, name="debounce interval")

    def factory() -> AsyncIterator[T]:
        return _debounce_gen(upstream, interval)

    return upstream._derive(factory, name=name)


async def _debounce_gen(upstream: Stream[T], seconds: float) -> AsyncIterator[T]:
    source = upstream._open_raw()
    iterator = source.__aiter__()
    pending: Any = _UNSET
    pull = asyncio.create_task(_next_item(iterator), name="datacurrent-debounce")
    try:
        while True:
            timeout = None if pending is _UNSET else seconds
            done, _ = await asyncio.wait({pull}, timeout=timeout)
            if not done:
                yield pending
                pending = _UNSET
                continue
            try:
                item = pull.result()
            except StopAsyncIteration:
                if pending is not _UNSET:
                    yield pending
                return
            pending = item
            pull = asyncio.create_task(_next_item(iterator), name="datacurrent-debounce")
    finally:
        if not pull.done():
            pull.cancel()
        await asyncio.gather(pull, return_exceptions=True)
        await aclose(source)


def op_throttle(
    upstream: Stream[T],
    interval: float,
    *,
    name: str | None = None,
) -> Stream[T]:
    """Emit at most one item per ``interval`` seconds (leading edge).

    Items that arrive too soon are consumed and dropped; use ``buffer()`` if you
    need to keep them.
    """
    spacing = validate_timeout(interval, name="throttle interval")

    def factory() -> AsyncIterator[T]:
        return _throttle_gen(upstream, spacing)

    return upstream._derive(factory, name=name)


async def _throttle_gen(upstream: Stream[T], interval: float) -> AsyncIterator[T]:
    source = upstream._open_raw()
    loop = asyncio.get_running_loop()
    last_emit: float | None = None
    try:
        async for item in source:
            now = loop.time()
            if last_emit is None or (now - last_emit) >= interval:
                last_emit = now
                yield item
    finally:
        await aclose(source)


# --------------------------------------------------------------------------- #
# timeout / retry / recover
# --------------------------------------------------------------------------- #
def op_timeout(
    upstream: Stream[T],
    seconds: float,
    *,
    name: str | None = None,
) -> Stream[T]:
    timeout = validate_timeout(seconds)

    def factory() -> AsyncIterator[T]:
        return _timeout_gen(upstream, timeout)

    return upstream._derive(factory, name=name)


async def _timeout_gen(upstream: Stream[T], deadline: float) -> AsyncIterator[T]:
    source = upstream._open_raw()
    iterator = source.__aiter__()
    try:
        while True:
            try:
                item = await asyncio.wait_for(iterator.__anext__(), timeout=deadline)
            except StopAsyncIteration:
                return
            except TimeoutError:
                raise StreamTimeoutError(f"stream produced no item within {deadline:g}s") from None
            yield item
    finally:
        await aclose(source)


def op_retry(
    upstream: Stream[T],
    policy: RetryPolicy | int | dict[str, Any] | None = None,
    *,
    on: Any = None,
    name: str | None = None,
) -> Stream[T]:
    """Restart the upstream stream when it fails.

    Only replayable sources can be retried.  ``Stream.from_iterable()`` on a
    list, ``Stream.from_factory()`` and ``Stream.from_async_iterable()`` on a
    list are replayable; a bare generator or async generator is not, because it
    cannot be restarted.
    """
    resolved = RetryPolicy.coerce(policy)
    predicate = resolved.retry_on if on is None else on
    stats = StreamStats(name=name or upstream.config.name)

    def factory() -> AsyncIterator[T]:
        return _retry_gen(upstream, resolved, predicate, stats)

    return upstream._derive(
        factory,
        name=name,
        replayable=upstream.replayable,
        stats=stats,
    )


async def _retry_gen(
    upstream: Stream[T],
    policy: RetryPolicy,
    predicate: Any,
    stats: StreamStats,
) -> AsyncIterator[T]:
    attempt = 1
    while True:
        source = upstream._open_raw()
        try:
            async for item in source:
                yield item
            return
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if attempt >= policy.max_attempts or not is_retryable(predicate, error):
                raise
            if not upstream.replayable:
                raise NonReplayableStreamError(
                    "retry() needs a replayable stream. Use Stream.from_factory(), "
                    "Stream.from_iterable(list) or Stream.from_async_iterable(list); "
                    "a bare generator or async generator is consumed once."
                ) from error
            attempt += 1
            stats.retries += 1
            delay = policy.delay_for(attempt - 1)
            if delay:
                await asyncio.sleep(delay)
        finally:
            await aclose(source)


def op_recover(
    upstream: Stream[T],
    handler: Callable[[Exception], Any],
    *,
    name: str | None = None,
) -> Stream[T]:
    """Recover from an upstream failure with a replacement iterable.

    ``handler`` receives the exception and may return an iterable, async
    iterable, ``Stream`` or ``None`` (recover with no further items).  Exceptions
    raised by the handler propagate.
    """
    _require_callable(handler, "recover")

    def factory() -> AsyncIterator[T]:
        return _recover_gen(upstream, handler)

    return upstream._derive(factory, name=name)


async def _recover_gen(
    upstream: Stream[T], handler: Callable[[Exception], Any]
) -> AsyncIterator[T]:
    source = upstream._open_raw()
    replacement: Any = _UNSET
    try:
        async for item in source:
            yield item
        return
    except asyncio.CancelledError:
        raise
    except Exception as error:
        replacement = await maybe_await(handler(error))
    finally:
        await aclose(source)

    if replacement is _UNSET or replacement is None:
        return
    inner = as_aiterator(replacement)
    try:
        async for item in inner:
            yield item
    finally:
        await aclose(inner)


# --------------------------------------------------------------------------- #
# zip / merge / concat
# --------------------------------------------------------------------------- #
def op_zip(
    upstream: Stream[T],
    *others: Any,
    name: str | None = None,
) -> Stream[tuple[Any, ...]]:
    if not others:
        raise ConfigurationError("zip() requires at least one other stream")
    sources = [_as_stream(other, upstream.config) for other in others]
    replayable = upstream.replayable and all(source.replayable for source in sources)
    all_sources = [upstream, *sources]

    def factory() -> AsyncIterator[tuple[Any, ...]]:
        return _zip_gen(all_sources)

    return upstream._derive(factory, name=name, replayable=replayable)


async def _zip_gen(sources: Iterable[Stream[Any]]) -> AsyncIterator[tuple[Any, ...]]:
    iterators: list[AsyncIterator[Any]] = []
    try:
        for source in sources:
            iterators.append(source._open_raw())
        while True:
            results = await asyncio.gather(
                *(iterator.__anext__() for iterator in iterators),
                return_exceptions=True,
            )
            items: list[Any] = []
            exhausted = False
            for result in results:
                if isinstance(result, StopAsyncIteration):
                    exhausted = True
                elif isinstance(result, BaseException):
                    raise result
                else:
                    items.append(result)
            if exhausted:
                return
            yield tuple(items)
    finally:
        for iterator in iterators:
            await aclose(iterator)


def op_merge(
    upstream: Stream[T],
    *others: Any,
    buffer_size: int | None = None,
    overflow: OverflowPolicy | str | None = None,
    name: str | None = None,
) -> Stream[T]:
    """Interleave several streams concurrently, yielding items as they arrive."""
    if not others:
        raise ConfigurationError("merge() requires at least one other stream")
    sources = [upstream, *(_as_stream(other, upstream.config) for other in others)]
    policy = BufferPolicy.from_config(upstream.config, size=buffer_size, overflow=overflow)
    replayable = all(source.replayable for source in sources)

    def factory() -> AsyncIterator[T]:
        return _merge_gen(sources, policy)

    return upstream._derive(factory, name=name, replayable=replayable)


async def _merge_gen(sources: Iterable[Stream[Any]], policy: BufferPolicy) -> AsyncIterator[Any]:
    queue = policy.build(name="merge")
    remaining = len(list(sources)) if not isinstance(sources, list) else len(sources)
    counter = [remaining]

    async def pump(source: Stream[Any]) -> None:
        iterator = source._open_raw()
        try:
            async for item in iterator:
                try:
                    await queue.put(item)
                except QueueClosedError:
                    return
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await queue.fail(error)
        finally:
            await aclose(iterator)
            counter[0] -= 1
            if counter[0] == 0:
                await queue.aclose()

    tasks = [
        asyncio.create_task(pump(source), name=f"datacurrent-merge-{index}")
        for index, source in enumerate(sources)
    ]
    try:
        async for item in queue:
            yield item
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await queue.aclose()


def op_concat(
    upstream: Stream[T],
    *others: Any,
    name: str | None = None,
) -> Stream[T]:
    """Consume streams one after another, preserving order."""
    if not others:
        raise ConfigurationError("concat() requires at least one other stream")
    sources = [upstream, *(_as_stream(other, upstream.config) for other in others)]
    replayable = all(source.replayable for source in sources)

    def factory() -> AsyncIterator[T]:
        return _concat_gen(sources)

    return upstream._derive(factory, name=name, replayable=replayable)


async def _concat_gen(sources: Iterable[Stream[Any]]) -> AsyncIterator[Any]:
    for source in sources:
        iterator = source._open_raw()
        try:
            async for item in iterator:
                yield item
        finally:
            await aclose(iterator)
