"""Retry policies with exponential backoff and jitter."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, TypeVar

from ..exceptions import ConfigurationError

T = TypeVar("T")

__all__ = ["RetryPolicy", "is_retryable", "retry_async"]


def is_retryable(predicate: Any, error: BaseException) -> bool:
    """Return whether ``error`` matches a retry predicate.

    ``predicate`` may be an exception class, a tuple of classes, or a callable
    taking the exception and returning a bool.
    """
    if predicate is None:
        return True
    if isinstance(predicate, tuple):
        return isinstance(error, predicate)
    if isinstance(predicate, type) and issubclass(predicate, BaseException):
        return isinstance(error, predicate)
    if callable(predicate):
        return bool(predicate(error))
    raise ConfigurationError(
        f"retry predicate must be an exception class, tuple or callable, got {predicate!r}"
    )


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Declarative retry configuration.

    Args:
        max_attempts: Total attempts including the initial one (>= 1).
        base_delay: Delay before the first retry, in seconds.
        max_delay: Upper bound for a single delay, in seconds.
        multiplier: Exponential growth factor (>= 1).
        jitter: Randomization fraction in ``[0, 1]``; 0 disables jitter.
        retry_on: Exception class, tuple of classes or predicate.
    """

    max_attempts: int = 3
    base_delay: float = 0.1
    max_delay: float = 10.0
    multiplier: float = 2.0
    jitter: float = 0.1
    retry_on: Any = Exception

    def __post_init__(self) -> None:
        if isinstance(self.max_attempts, bool) or not isinstance(self.max_attempts, int):
            raise ConfigurationError("max_attempts must be an integer")
        if self.max_attempts < 1:
            raise ConfigurationError("max_attempts must be >= 1")
        for name in ("base_delay", "max_delay", "multiplier", "jitter"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ConfigurationError(f"{name} must be a number")
            if value < 0:
                raise ConfigurationError(f"{name} must be >= 0")
        if self.multiplier < 1:
            raise ConfigurationError("multiplier must be >= 1")
        if self.jitter > 1:
            raise ConfigurationError("jitter must be between 0 and 1")

    def should_retry(self, error: BaseException) -> bool:
        return is_retryable(self.retry_on, error)

    def delay_for(self, attempt: int, *, rng: random.Random | None = None) -> float:
        """Delay before retry number ``attempt`` (1 = first retry)."""
        if attempt < 1:
            raise ConfigurationError("attempt must be >= 1")
        if self.base_delay == 0:
            return 0.0
        exponent = attempt - 1
        delay = min(float(self.max_delay), float(self.base_delay) * (self.multiplier**exponent))
        if self.jitter:
            source = rng.random() if rng is not None else random.random()
            spread = delay * self.jitter
            delay = delay - spread + (2 * spread * source)
        return max(0.0, delay)

    @classmethod
    def coerce(cls, value: RetryPolicy | int | Mapping[str, Any] | None) -> RetryPolicy:
        """Build a policy from a policy, an attempt count or a mapping."""
        if isinstance(value, cls):
            return value
        if value is None:
            return cls()
        if isinstance(value, bool):
            raise ConfigurationError("retry policy must not be a bool")
        if isinstance(value, int):
            return cls(max_attempts=value)
        if isinstance(value, Mapping):
            return cls(**dict(value))
        raise ConfigurationError(
            f"cannot interpret {value!r} as a RetryPolicy; pass RetryPolicy(...), "
            "an integer attempt count, or a mapping of policy fields"
        )


async def retry_async(
    func: Callable[[], Awaitable[T]],
    policy: RetryPolicy | int | Mapping[str, Any] | None = None,
    *,
    on: Any = None,
) -> T:
    """Call ``func`` until it succeeds or the policy is exhausted.

    The final error is re-raised unchanged, so callers keep the original
    traceback and exception type.
    """
    resolved = RetryPolicy.coerce(policy)
    predicate = resolved.retry_on if on is None else on
    attempt = 1
    while True:
        try:
            return await func()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if attempt >= resolved.max_attempts or not is_retryable(predicate, error):
                raise
            delay = resolved.delay_for(attempt)
            attempt += 1
            if delay:
                await asyncio.sleep(delay)
