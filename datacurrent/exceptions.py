"""Exception hierarchy used across DataCurrent.

Every exception raised deliberately by the library derives from
:class:`DataCurrentError`.  Applications can therefore catch one base class
without accidentally swallowing programming errors such as ``TypeError`` or
control-flow exceptions such as ``asyncio.CancelledError``.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "BufferOverflowError",
    "ConfigurationError",
    "ConnectionFailedError",
    "DataCurrentError",
    "EmptyStreamError",
    "EventBusError",
    "MessageTooLargeError",
    "NonReplayableStreamError",
    "OptionalDependencyError",
    "QueueClosedError",
    "QueueEmptyError",
    "RetryError",
    "SSEProtocolError",
    "StreamClosedError",
    "StreamConsumedError",
    "StreamError",
    "StreamProtocolError",
    "StreamTimeoutError",
    "SubscriptionClosedError",
    "TransportError",
    "WebSocketClosedError",
]


class DataCurrentError(Exception):
    """Base class for every error raised by DataCurrent."""


class ConfigurationError(DataCurrentError, ValueError):
    """The caller supplied an invalid configuration value."""


class OptionalDependencyError(DataCurrentError, ImportError):
    """An optional integration was used without its dependency installed."""

    def __init__(
        self,
        package: str,
        *,
        extra: str | None = None,
        purpose: str = "This feature",
    ) -> None:
        self.package = package
        self.extra = extra
        hint = f'pip install "datacurrent[{extra}]"' if extra else f"pip install {package}"
        super().__init__(
            f"{purpose} requires the optional dependency {package!r}. Install it with: {hint}"
        )


# --------------------------------------------------------------------------- #
# Streams
# --------------------------------------------------------------------------- #
class StreamError(DataCurrentError):
    """Base class for errors related to stream consumption."""


class StreamConsumedError(StreamError):
    """A single-consumer stream was iterated more than once."""


class NonReplayableStreamError(StreamError):
    """An operation required restarting a stream that cannot be replayed."""


class EmptyStreamError(StreamError):
    """A terminal operation required at least one item but the stream was empty."""


class StreamTimeoutError(StreamError, TimeoutError):
    """No item arrived within the configured timeout."""


class StreamClosedError(StreamError):
    """The stream or its underlying queue has been closed."""


# --------------------------------------------------------------------------- #
# Buffering
# --------------------------------------------------------------------------- #
class BufferOverflowError(StreamError, BufferError):
    """A bounded buffer was full and the configured policy is ``error``.

    Attributes:
        maxsize: Capacity of the buffer that overflowed.
        policy: Overflow policy that produced the error.
        dropped: Number of items dropped in total by that buffer.
    """

    def __init__(
        self,
        maxsize: int,
        *,
        policy: Any = "error",
        dropped: int = 0,
    ) -> None:
        self.maxsize = maxsize
        self.policy = getattr(policy, "value", policy)
        self.dropped = dropped
        super().__init__(
            f"buffer is full (maxsize={maxsize}, policy={self.policy!r}, "
            f"dropped={dropped}); slow down the producer or enlarge the buffer"
        )


class QueueClosedError(DataCurrentError):
    """An operation was attempted on a closed queue."""


class QueueEmptyError(DataCurrentError):
    """A non-blocking read was attempted on an empty queue."""


# --------------------------------------------------------------------------- #
# Transports
# --------------------------------------------------------------------------- #
class TransportError(DataCurrentError):
    """Base class for networking and transport failures."""


class ConnectionFailedError(TransportError):
    """A connection could not be established or was lost."""


class StreamProtocolError(TransportError):
    """The peer sent data that violates the expected protocol."""


class SSEProtocolError(StreamProtocolError):
    """A Server-Sent Events response was malformed or had the wrong content type."""


class MessageTooLargeError(TransportError):
    """A received message exceeded the configured limit."""

    def __init__(self, size: int, limit: int) -> None:
        self.size = size
        self.limit = limit
        super().__init__(f"message of {size} bytes exceeds the limit of {limit} bytes")


class WebSocketClosedError(TransportError):
    """The WebSocket connection was closed before reading finished."""

    def __init__(self, code: int | None = None, reason: str | None = None) -> None:
        self.code = code
        self.reason = reason
        detail = []
        if code is not None:
            detail.append(f"code={code}")
        if reason:
            detail.append(f"reason={reason}")
        suffix = f" ({', '.join(detail)})" if detail else ""
        super().__init__(f"websocket connection closed{suffix}")


# --------------------------------------------------------------------------- #
# Reliability
# --------------------------------------------------------------------------- #
class RetryError(DataCurrentError):
    """All retry attempts were exhausted."""

    def __init__(self, attempts: int, last_error: BaseException | None = None) -> None:
        self.attempts = attempts
        self.last_error = last_error
        suffix = f": {last_error!r}" if last_error is not None else ""
        super().__init__(f"giving up after {attempts} attempt(s){suffix}")


# --------------------------------------------------------------------------- #
# Events
# --------------------------------------------------------------------------- #
class EventBusError(DataCurrentError):
    """Base class for event bus failures."""


class SubscriptionClosedError(EventBusError):
    """The subscription is no longer able to receive events."""
