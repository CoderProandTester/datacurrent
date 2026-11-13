"""Timeout helpers that raise DataCurrent's timeout error type."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import Any, TypeVar

from ..exceptions import ConfigurationError, StreamTimeoutError

T = TypeVar("T")

__all__ = ["validate_timeout", "with_timeout"]


def validate_timeout(seconds: Any, *, name: str = "timeout") -> float:
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
        raise ConfigurationError(f"{name} must be a number, got {type(seconds).__name__}")
    if seconds <= 0:
        raise ConfigurationError(f"{name} must be > 0, got {seconds}")
    return float(seconds)


async def with_timeout(
    awaitable: Awaitable[T],
    seconds: float,
    *,
    what: str = "operation",
) -> T:
    """Await ``awaitable`` for at most ``seconds``.

    Raises:
        StreamTimeoutError: The deadline elapsed.
    """
    timeout = validate_timeout(seconds)
    try:
        return await asyncio.wait_for(awaitable, timeout=timeout)
    except TimeoutError:
        raise StreamTimeoutError(f"{what} exceeded {timeout:g}s") from None
