"""Extension point for custom transports.

A transport is just an object that produces messages and can be closed.  Build
one when your protocol is not covered by the SSE, HTTP or WebSocket adapters::

    class MyTransport(StreamTransport[dict]):
        async def open(self): ...
        async def close(self): ...
        async def messages(self): yield {"...": "..."}

    stream = MyTransport().as_stream()
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any, Generic, TypeVar

from ..core.stream import Stream
from ..exceptions import StreamClosedError

T = TypeVar("T")

__all__ = ["StreamTransport"]


class StreamTransport(ABC, Generic[T]):
    """Base class for transports that expose an async stream of messages."""

    def __init__(self, *, name: str | None = None) -> None:
        self.name = name or type(self).__name__
        self._opened = False
        self._closed = False

    @property
    def is_open(self) -> bool:
        return self._opened and not self._closed

    @abstractmethod
    async def open(self) -> None:
        """Establish the connection / start producing."""

    @abstractmethod
    async def close(self) -> None:
        """Release every resource.  Must be idempotent."""

    @abstractmethod
    def messages(self) -> AsyncIterator[T]:
        """Yield messages until the transport is closed."""

    async def __aenter__(self) -> StreamTransport[T]:
        await self.open()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    def as_stream(self, **stream_kwargs: Any) -> Stream[T]:
        """Expose the transport as a single-consumer stream.

        Closing the stream closes the transport, so ``break`` cannot leak a
        connection.
        """
        transport = self

        async def factory() -> AsyncIterator[T]:
            if not transport.is_open:
                await transport.open()
            try:
                async for message in transport.messages():
                    yield message
            finally:
                await transport.close()

        return Stream(factory, replayable=False, **stream_kwargs)

    async def _ensure_open(self) -> None:
        if self._closed:
            raise StreamClosedError(f"transport {self.name!r} is closed")
