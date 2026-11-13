"""Buffering: bounded queues, overflow policies and backpressure."""

from __future__ import annotations

from .backpressure import BufferPolicy
from .queue import BoundedQueue, BufferStats

__all__ = ["BoundedQueue", "BufferPolicy", "BufferStats"]
