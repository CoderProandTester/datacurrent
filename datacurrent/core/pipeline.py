"""Pipeline construction helpers.

``Stream`` methods are the primary API; these helpers cover the two cases where
a free function reads better than a method: building the first stream and
combining several sources.
"""

from __future__ import annotations

from collections.abc import AsyncIterable, Iterable
from typing import Any, TypeVar

from ..config import StreamConfig
from .stream import Stream

T = TypeVar("T")

__all__ = ["concat_sources", "merge_sources", "pipeline"]


def pipeline(
    source: Iterable[T] | AsyncIterable[T] | Any = None,
    *,
    config: StreamConfig | None = None,
) -> Stream[T]:
    """Create the first stream of a pipeline.

    ``pipeline()`` with no argument returns an empty stream, which is handy when
    the real source is decided later.
    """
    if source is None:
        return Stream.empty(config=config)
    if isinstance(source, Stream):
        return source
    return Stream(source, config=config)


def merge_sources(
    *sources: Iterable[Any] | AsyncIterable[Any] | Stream[Any],
    buffer_size: int | None = None,
    config: StreamConfig | None = None,
) -> Stream[Any]:
    """Merge several sources into one concurrent stream."""
    if not sources:
        return Stream.empty(config=config)
    first, *rest = sources
    head: Stream[Any] = pipeline(first, config=config)
    if not rest:
        return head
    return head.merge(*rest, buffer_size=buffer_size)


def concat_sources(
    *sources: Iterable[Any] | AsyncIterable[Any] | Stream[Any],
    config: StreamConfig | None = None,
) -> Stream[Any]:
    """Concatenate several sources into one ordered stream."""
    if not sources:
        return Stream.empty(config=config)
    first, *rest = sources
    head: Stream[Any] = pipeline(first, config=config)
    if not rest:
        return head
    return head.concat(*rest)
