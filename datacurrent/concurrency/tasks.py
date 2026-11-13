"""Structured task management: no unbounded background tasks, no leaks."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Coroutine
from typing import Any, TypeVar

from ..core.lifecycle import cancel_tasks
from ..exceptions import StreamClosedError
from ..observability.logging import get_logger

T = TypeVar("T")

__all__ = ["TaskTracker"]

_log = get_logger("concurrency")


class TaskTracker:
    """Track background tasks so they can always be cancelled deterministically.

    DataCurrent uses this for buffering producers, ``merge()`` pumps and event
    bus dispatchers.  Closing the tracker cancels every task and waits for it,
    which is what keeps cancellation from leaking work into a stopped loop.
    """

    __slots__ = ("_closed", "_name", "_tasks")

    def __init__(self, *, name: str = "datacurrent") -> None:
        self._tasks: set[asyncio.Task[Any]] = set()
        self._name = name
        self._closed = False

    @property
    def active(self) -> int:
        return len(self._tasks)

    @property
    def tasks(self) -> tuple[asyncio.Task[Any], ...]:
        return tuple(self._tasks)

    @property
    def closed(self) -> bool:
        return self._closed

    def spawn(
        self,
        coro: Awaitable[T] | Coroutine[Any, Any, T],
        *,
        name: str | None = None,
    ) -> asyncio.Task[T]:
        """Schedule ``coro`` and keep a strong reference until it finishes."""
        if self._closed:
            if asyncio.iscoroutine(coro):
                coro.close()
            raise StreamClosedError(f"task tracker {self._name!r} is closed")
        task = asyncio.ensure_future(coro)
        task.set_name(name or f"{self._name}-{len(self._tasks) + 1}")
        self._tasks.add(task)
        task.add_done_callback(self._on_done)
        return task

    def _on_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            _log.warning("%s: background task failed: %r", self._name, error)

    async def aclose(self, *, timeout: float | None = 5.0) -> None:  # noqa: ASYNC109
        """Cancel and await every tracked task."""
        self._closed = True
        tasks = list(self._tasks)
        self._tasks.clear()
        await cancel_tasks(tasks, timeout=timeout)

    async def __aenter__(self) -> TaskTracker:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return f"TaskTracker(name={self._name!r}, active={len(self._tasks)}, closed={self._closed})"
