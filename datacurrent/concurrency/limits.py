"""Limits and helpers for bounded concurrency."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from typing import Any, TypeVar, cast

from ..core._util import maybe_await
from ..exceptions import ConfigurationError

T = TypeVar("T")

__all__ = ["ConcurrencyLimiter", "gather_limited"]


class ConcurrencyLimiter:
    """A semaphore with introspection.

    ``limit`` workers may run at once; additional callers wait instead of
    spawning unbounded tasks.
    """

    __slots__ = ("_active", "_limit", "_peak", "_semaphore", "_waiting")

    def __init__(self, limit: int) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ConfigurationError(
                f"concurrency limit must be an integer, got {type(limit).__name__}"
            )
        if limit < 1:
            raise ConfigurationError(f"concurrency limit must be >= 1, got {limit}")
        self._limit = limit
        self._semaphore = asyncio.Semaphore(limit)
        self._active = 0
        self._peak = 0
        self._waiting = 0

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def active(self) -> int:
        return self._active

    @property
    def waiting(self) -> int:
        return self._waiting

    @property
    def peak(self) -> int:
        return self._peak

    async def acquire(self) -> None:
        self._waiting += 1
        try:
            await self._semaphore.acquire()
        finally:
            self._waiting -= 1
        self._active += 1
        self._peak = max(self._peak, self._active)

    def release(self) -> None:
        if self._active > 0:
            self._active -= 1
        self._semaphore.release()

    async def __aenter__(self) -> ConcurrencyLimiter:
        await self.acquire()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.release()

    async def run(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        async with self:
            return await maybe_await(func(*args, **kwargs))

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return (
            f"ConcurrencyLimiter(limit={self._limit}, active={self._active}, "
            f"waiting={self._waiting}, peak={self._peak})"
        )


async def gather_limited(
    limit: int,
    awaitables: Iterable[Awaitable[T]],
    *,
    return_exceptions: bool = False,
) -> list[T]:
    """Await ``awaitables`` with at most ``limit`` running at the same time."""
    limiter = ConcurrencyLimiter(limit)

    async def run(awaitable: Awaitable[T]) -> T:
        async with limiter:
            return await awaitable

    return cast(
        "list[T]",
        await asyncio.gather(
            *(run(awaitable) for awaitable in awaitables),
            return_exceptions=return_exceptions,
        ),
    )
