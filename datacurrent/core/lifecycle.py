"""Deterministic cleanup helpers for async resources and tasks."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Iterable
from typing import Any

__all__ = ["aclose", "aclose_many", "cancel_task", "cancel_tasks"]


async def aclose(obj: Any, *, suppress: bool = True) -> None:
    """Close an object that may expose ``aclose()`` or ``close()``.

    Used in ``finally`` blocks of every operator so early exits (``break``,
    cancellation, exceptions) never leak generators, responses or connections.
    """
    if obj is None:
        return
    close = getattr(obj, "aclose", None)
    if close is None:
        close = getattr(obj, "close", None)
    if close is None:
        return
    try:
        result = close()
        if inspect.isawaitable(result):
            await result
    except asyncio.CancelledError:
        raise
    except Exception:
        if not suppress:
            raise


async def aclose_many(objs: Iterable[Any], *, suppress: bool = True) -> None:
    for obj in objs:
        await aclose(obj, suppress=suppress)


async def cancel_task(
    task: asyncio.Task[Any] | None,
    *,
    timeout: float | None = None,  # noqa: ASYNC109 - cleanup deadline
) -> None:
    """Cancel a single task and wait for it to finish."""
    if task is None:
        return
    if not task.done():
        task.cancel()
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
    except (asyncio.CancelledError, TimeoutError):
        if not task.done():
            # Give the loop a chance to run the cancellation callbacks.
            await asyncio.sleep(0)
    except Exception:
        # Exceptions are the caller's concern; cleanup must still succeed.
        pass


async def cancel_tasks(
    tasks: Iterable[asyncio.Task[Any]],
    *,
    timeout: float | None = None,  # noqa: ASYNC109 - cleanup deadline
) -> None:
    """Cancel every task in ``tasks`` and wait for their completion."""
    pending = [task for task in tasks if not task.done()]
    for task in pending:
        task.cancel()
    if not pending:
        return
    done, still_pending = await asyncio.wait(pending, timeout=timeout)
    for task in done:
        if task.cancelled():
            continue
        # Retrieve exceptions so asyncio does not emit "never retrieved" warnings.
        task.exception()
    for task in still_pending:
        task.cancel()
