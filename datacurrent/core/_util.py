"""Internal async helpers shared by the stream engine and operators."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Iterable
from typing import Any, TypeVar

from .lifecycle import aclose

T = TypeVar("T")

__all__ = ["aclose", "as_aiterator", "from_sync_iterable", "maybe_await"]


async def maybe_await(value: Any) -> Any:
    """Await ``value`` when it is awaitable, otherwise return it unchanged.

    This is what allows every operator callback to be either a plain function
    or a coroutine function.
    """
    if inspect.isawaitable(value):
        return await value
    return value


async def from_sync_iterable(iterable: Iterable[T]) -> AsyncIterator[T]:
    """Adapt a synchronous iterable to an async iterator without a thread."""
    for item in iterable:
        yield item


def as_aiterator(source: Any) -> AsyncIterator[Any]:
    """Return an async iterator for an async iterable, iterable or stream."""
    aiter = getattr(source, "__aiter__", None)
    if aiter is not None:
        return aiter()
    if hasattr(source, "__iter__"):
        return from_sync_iterable(source)
    raise TypeError(
        f"expected an async iterable, an iterable, or a Stream, got {type(source).__name__}"
    )
