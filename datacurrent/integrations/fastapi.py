"""FastAPI helpers: SSE responses and WebSocket message streams.

FastAPI is optional and imported lazily inside the functions.
"""

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Mapping
from typing import Any

from ..core._util import aclose
from ..core.stream import Stream
from ..transports.sse import SSEEvent, format_sse

__all__ = ["sse_response", "websocket_messages", "websocket_stream"]


def sse_response(
    events: AsyncIterable[SSEEvent] | Stream[SSEEvent],
    *,
    headers: Mapping[str, str] | None = None,
    status_code: int = 200,
) -> Any:
    """Return a ``StreamingResponse`` that encodes events as ``text/event-stream``."""
    from starlette.responses import StreamingResponse

    default_headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    merged = {**default_headers, **dict(headers or {})}

    async def body() -> AsyncIterator[bytes]:
        source: AsyncIterable[SSEEvent] = events
        iterator = source.__aiter__()
        try:
            async for event in iterator:
                yield format_sse(event).encode("utf-8")
        finally:
            await aclose(iterator)

    return StreamingResponse(
        body(),
        media_type="text/event-stream",
        headers=merged,
        status_code=status_code,
    )


async def websocket_messages(websocket: Any) -> AsyncIterator[Any]:
    """Yield text and binary messages from a Starlette/FastAPI WebSocket."""
    while True:
        message = await websocket.receive()
        message_type = message.get("type")
        if message_type == "websocket.disconnect":
            return
        if "text" in message and message["text"] is not None:
            yield message["text"]
        elif "bytes" in message and message["bytes"] is not None:
            yield message["bytes"]


def websocket_stream(websocket: Any, *, config: Any = None) -> Stream[Any]:
    """Expose an incoming WebSocket as a single-consumer stream."""

    async def factory() -> AsyncIterator[Any]:
        try:
            async for message in websocket_messages(websocket):
                yield message
        finally:
            closer = getattr(websocket, "close", None)
            if closer is not None:
                await aclose(websocket)

    return Stream(factory, config=config, replayable=False)
