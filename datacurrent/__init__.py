"""DataCurrent — Python streaming, made simple.

Async streams, composable operators, real backpressure and modular real-time
transports for Python applications.

    from datacurrent import Stream

    stream = Stream.from_iterable([1, 2, 3]).map(lambda n: n * 2)
    async for value in stream:
        print(value)
"""

from __future__ import annotations

from ._version import VERSION, __version__
from .buffering import BoundedQueue, BufferPolicy, BufferStats
from .concurrency import ConcurrencyLimiter, TaskTracker, gather_limited
from .config import (
    DEFAULT_BUFFER_SIZE,
    DEFAULT_CONCURRENCY,
    DEFAULT_MAX_MESSAGE_SIZE,
    DEFAULT_TIMEOUT,
    OverflowPolicy,
    StreamConfig,
)
from .core import (
    Stream,
    StreamIterator,
    concat_sources,
    consume,
    drain_into,
    merge_sources,
    pipeline,
)
from .events import Event, EventBus, Subscription, matches
from .exceptions import (
    BufferOverflowError,
    ConfigurationError,
    ConnectionFailedError,
    DataCurrentError,
    EmptyStreamError,
    EventBusError,
    MessageTooLargeError,
    NonReplayableStreamError,
    OptionalDependencyError,
    QueueClosedError,
    QueueEmptyError,
    RetryError,
    SSEProtocolError,
    StreamClosedError,
    StreamConsumedError,
    StreamError,
    StreamProtocolError,
    StreamTimeoutError,
    SubscriptionClosedError,
    TransportError,
    WebSocketClosedError,
)
from .observability import (
    Counter,
    Metrics,
    StreamStats,
    get_logger,
    redact_headers,
    redact_secret,
    redact_url,
)
from .reliability import RetryPolicy, retry_async, with_timeout
from .transports import (
    Channel,
    HTTPStream,
    SSEEvent,
    SSEStream,
    StreamTransport,
    WebSocketStream,
    format_heartbeat,
    format_sse,
    parse_sse,
)

__all__ = [
    # Constants
    "DEFAULT_BUFFER_SIZE",
    "DEFAULT_CONCURRENCY",
    "DEFAULT_MAX_MESSAGE_SIZE",
    "DEFAULT_TIMEOUT",
    "VERSION",
    # Buffering
    "BoundedQueue",
    "BufferOverflowError",
    "BufferPolicy",
    "BufferStats",
    # Transports
    "Channel",
    # Concurrency
    "ConcurrencyLimiter",
    "ConfigurationError",
    "ConnectionFailedError",
    "Counter",
    # Exceptions
    "DataCurrentError",
    "EmptyStreamError",
    # Events
    "Event",
    "EventBus",
    "EventBusError",
    "HTTPStream",
    "MessageTooLargeError",
    # Observability
    "Metrics",
    "NonReplayableStreamError",
    "OptionalDependencyError",
    "OverflowPolicy",
    "QueueClosedError",
    "QueueEmptyError",
    "RetryError",
    # Reliability
    "RetryPolicy",
    "SSEEvent",
    "SSEProtocolError",
    "SSEStream",
    # Core engine
    "Stream",
    "StreamClosedError",
    "StreamConfig",
    "StreamConsumedError",
    "StreamError",
    "StreamIterator",
    "StreamProtocolError",
    "StreamStats",
    "StreamTimeoutError",
    "StreamTransport",
    "Subscription",
    "SubscriptionClosedError",
    "TaskTracker",
    "TransportError",
    "WebSocketClosedError",
    "WebSocketStream",
    # Version
    "__version__",
    "concat_sources",
    "consume",
    "drain_into",
    "format_heartbeat",
    "format_sse",
    "gather_limited",
    "get_logger",
    "matches",
    "merge_sources",
    "parse_sse",
    "pipeline",
    "redact_headers",
    "redact_secret",
    "redact_url",
    "retry_async",
    "with_timeout",
]
