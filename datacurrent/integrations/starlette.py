"""Starlette helpers (also used by the FastAPI integration).

Starlette is optional: importing this module never imports Starlette itself.
"""

from __future__ import annotations

from collections.abc import AsyncIterable, Mapping
from typing import Any

from ..core._util import aclose
from ..core.stream import Stream

__all__ = ["StreamLifespan", "streaming_response"]


def streaming_response(
    stream: Stream[Any] | AsyncIterable[Any],
    *,
    media_type: str = "text/plain; charset=utf-8",
    headers: Mapping[str, str] | None = None,
    status_code: int = 200,
) -> Any:
    """Return a Starlette ``StreamingResponse`` backed by a stream.

    Bytes are forwarded untouched and strings are encoded as UTF-8.  The stream
    is closed when the response finishes or the client disconnects.
    """
    from starlette.responses import StreamingResponse

    source = stream.__aiter__() if isinstance(stream, Stream) else stream

    async def body() -> Any:
        iterator = source.__aiter__()
        try:
            async for chunk in iterator:
                yield chunk.encode("utf-8") if isinstance(chunk, str) else chunk
        finally:
            await aclose(iterator)

    return StreamingResponse(
        body(),
        media_type=media_type,
        headers=dict(headers or {}),
        status_code=status_code,
    )


class StreamLifespan:
    """Run async closables during application startup/shutdown.

    Example::

        lifespan = StreamLifespan(bus, channel)

        app = FastAPI(lifespan=lifespan.as_context_manager())
    """

    def __init__(self, *resources: Any) -> None:
        self._resources = list(resources)

    async def __aenter__(self) -> StreamLifespan:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        resources, self._resources = self._resources, []
        for resource in reversed(resources):
            await aclose(resource)

    def as_context_manager(self) -> Any:
        """Return the lifespan as an async context manager for ASGI frameworks."""
        return self
