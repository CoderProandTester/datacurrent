"""Helpers to drive a stream into application code."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from ..config import DEFAULT_CONCURRENCY
from ..exceptions import ConfigurationError
from .stream import Stream

T = TypeVar("T")

__all__ = ["consume", "drain_into"]


async def consume(
    stream: Stream[T],
    handler: Callable[[T], Any],
    *,
    concurrency: int = DEFAULT_CONCURRENCY,
    ordered: bool = True,
) -> int:
    """Await ``handler(item)`` for every item and return the item count.

    ``handler`` may be synchronous or async.  With ``concurrency > 1`` the
    handler runs in a bounded number of tasks; exceptions cancel the remaining
    work and propagate.

    Example::

        count = await consume(stream, send_email, concurrency=8)
    """
    if not isinstance(stream, Stream):
        raise ConfigurationError("consume() expects a Stream")
    if not callable(handler):
        raise TypeError(f"handler must be callable, got {type(handler).__name__}")
    if isinstance(concurrency, bool) or not isinstance(concurrency, int):
        raise ConfigurationError("concurrency must be an integer")
    if concurrency < 1:
        raise ConfigurationError(f"concurrency must be >= 1, got {concurrency}")

    if concurrency == 1:
        total = 0
        async for item in stream:
            await _maybe(handler(item))
            total += 1
        return total

    mapped = stream.map(handler, concurrency=concurrency, ordered=ordered)
    return await mapped.count()


async def drain_into(
    stream: Stream[T],
    sink: Any,
    *,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> int:
    """Push every item into ``sink.put()`` (awaiting it when necessary)."""
    put = getattr(sink, "put", None)
    if put is None:
        raise ConfigurationError("drain_into() expects a sink exposing put(item)")
    return await consume(stream, put, concurrency=concurrency)


async def _maybe(value: Any) -> Any:
    from ._util import maybe_await

    return await maybe_await(value)
