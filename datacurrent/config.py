"""Configuration objects shared by streams, buffers, channels and the event bus."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final

from .exceptions import ConfigurationError

DEFAULT_BUFFER_SIZE: Final[int] = 1024
DEFAULT_CONCURRENCY: Final[int] = 1
DEFAULT_MAX_MESSAGE_SIZE: Final[int] = 8 * 1024 * 1024
DEFAULT_TIMEOUT: Final[float] = 30.0

__all__ = [
    "DEFAULT_BUFFER_SIZE",
    "DEFAULT_CONCURRENCY",
    "DEFAULT_MAX_MESSAGE_SIZE",
    "DEFAULT_TIMEOUT",
    "OverflowPolicy",
    "StreamConfig",
]


class OverflowPolicy(StrEnum):
    """What a bounded buffer does when it is full.

    ``BLOCK`` (the default) applies real backpressure: the producer waits.
    Drops are only ever performed when explicitly configured.
    """

    BLOCK = "block"
    DROP_NEWEST = "drop_newest"
    DROP_OLDEST = "drop_oldest"
    ERROR = "error"

    @classmethod
    def coerce(cls, value: Any) -> OverflowPolicy:
        """Normalize ``value`` (enum or string) into an :class:`OverflowPolicy`."""
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower().replace("-", "_")
            try:
                return cls(normalized)
            except ValueError:
                pass
        allowed = ", ".join(policy.value for policy in cls)
        raise ConfigurationError(f"unknown overflow policy {value!r}; expected one of: {allowed}")


def _require_int(name: str, value: Any, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigurationError(f"{name} must be an integer, got {type(value).__name__}")
    if value < minimum:
        raise ConfigurationError(f"{name} must be >= {minimum}, got {value}")
    return value


@dataclass(frozen=True, slots=True)
class StreamConfig:
    """Immutable configuration attached to a stream.

    Attributes:
        name: Optional human readable name used in logs and reprs.
        buffer_size: Capacity used by buffering operators such as ``buffer()``.
        overflow: Policy applied when a buffer is full.
        concurrency: Default worker count for operators that support it.
        observe: Collect per-stream counters (items out, errors, drops, retries).
    """

    name: str | None = None
    buffer_size: int = DEFAULT_BUFFER_SIZE
    overflow: OverflowPolicy = OverflowPolicy.BLOCK
    concurrency: int = DEFAULT_CONCURRENCY
    observe: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "overflow", OverflowPolicy.coerce(self.overflow))
        _require_int("buffer_size", self.buffer_size, 1)
        _require_int("concurrency", self.concurrency, 1)
        if self.name is not None and not isinstance(self.name, str):
            raise ConfigurationError(
                f"name must be a string or None, got {type(self.name).__name__}"
            )
