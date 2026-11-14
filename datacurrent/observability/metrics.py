"""Small, dependency-free metrics primitives.

DataCurrent never ships metrics to a hosted service on its own.  You expose the
snapshot to whatever system you already use (Prometheus, OpenTelemetry, logs).
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

__all__ = ["Counter", "Metrics", "StreamStats", "snapshot_stats"]


@dataclass(slots=True)
class StreamStats:
    """Mutable counters for a single stream instance."""

    name: str | None = None
    items_out: int = 0
    errors: int = 0
    retries: int = 0
    dropped: int = 0

    def snapshot(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "items_out": self.items_out,
            "errors": self.errors,
            "retries": self.retries,
            "dropped": self.dropped,
        }


def snapshot_stats(stats: StreamStats | None) -> dict[str, Any] | None:
    return None if stats is None else stats.snapshot()


class Counter:
    """A monotonically increasing counter."""

    __slots__ = ("_value",)

    def __init__(self) -> None:
        self._value = 0

    def inc(self, amount: int = 1) -> int:
        self._value += amount
        return self._value

    @property
    def value(self) -> int:
        return self._value

    def reset(self) -> None:
        self._value = 0

    def __int__(self) -> int:
        return self._value

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return f"Counter(value={self._value})"


@dataclass(slots=True)
class Metrics:
    """A tiny registry of counters and gauges.

    Keys are plain strings; tags are folded into the key so the output can be
    logged as-is or exported to a metrics backend.

    Example::

        metrics = Metrics()
        metrics.inc("stream.items", stream="orders")
        metrics.snapshot()["stream.items{stream=orders}"] == 1
    """

    prefix: str = "datacurrent"
    counters: dict[str, int] = field(default_factory=dict)
    gauges: dict[str, float] = field(default_factory=dict)

    @staticmethod
    def _key(name: str, tags: dict[str, Any] | None) -> str:
        if not tags:
            return name
        rendered = ",".join(f"{key}={value}" for key, value in sorted(tags.items()))
        return f"{name}{{{rendered}}}"

    def inc(self, name: str, amount: int = 1, **tags: Any) -> int:
        key = self._key(name, tags or None)
        self.counters[key] = self.counters.get(key, 0) + amount
        return self.counters[key]

    def set(self, name: str, value: float, **tags: Any) -> float:
        key = self._key(name, tags or None)
        self.gauges[key] = float(value)
        return self.gauges[key]

    def get(self, name: str, default: int = 0, **tags: Any) -> int:
        return self.counters.get(self._key(name, tags or None), default)

    @contextmanager
    def timer(self, name: str, **tags: Any) -> Iterator[None]:
        started = time.perf_counter()
        try:
            yield
        finally:
            self.observe_duration(name, time.perf_counter() - started, **tags)

    def observe_duration(self, name: str, seconds: float, **tags: Any) -> None:
        key = self._key(f"{name}.milliseconds", tags or None)
        self.gauges[key] = seconds * 1000.0

    def snapshot(self) -> dict[str, Any]:
        return {
            "prefix": self.prefix,
            "counters": dict(self.counters),
            "gauges": dict(self.gauges),
        }

    def reset(self) -> None:
        self.counters.clear()
        self.gauges.clear()
