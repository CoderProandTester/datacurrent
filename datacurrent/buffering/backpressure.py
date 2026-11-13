"""Normalized buffer settings shared by streams, channels and the event bus."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..config import DEFAULT_BUFFER_SIZE, OverflowPolicy, StreamConfig
from ..exceptions import ConfigurationError
from .queue import BoundedQueue

__all__ = ["BufferPolicy"]


@dataclass(frozen=True, slots=True)
class BufferPolicy:
    """A size + overflow policy pair that can build :class:`BoundedQueue` objects."""

    size: int = DEFAULT_BUFFER_SIZE
    overflow: OverflowPolicy = OverflowPolicy.BLOCK

    def __post_init__(self) -> None:
        object.__setattr__(self, "overflow", OverflowPolicy.coerce(self.overflow))
        if isinstance(self.size, bool) or not isinstance(self.size, int):
            raise ConfigurationError(
                f"buffer size must be an integer, got {type(self.size).__name__}"
            )
        if self.size < 1:
            raise ConfigurationError(f"buffer size must be >= 1, got {self.size}")

    def build(self, *, name: str | None = None, on_drop: Any = None) -> BoundedQueue[Any]:
        """Create a queue configured by this policy."""
        return BoundedQueue(self.size, overflow=self.overflow, name=name, on_drop=on_drop)

    @classmethod
    def from_config(
        cls,
        config: StreamConfig,
        *,
        size: int | None = None,
        overflow: OverflowPolicy | str | None = None,
    ) -> BufferPolicy:
        """Derive a policy from a stream config, allowing per-call overrides."""
        return cls(
            size=config.buffer_size if size is None else size,
            overflow=OverflowPolicy.coerce(config.overflow if overflow is None else overflow),
        )
