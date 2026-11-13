"""Tiny local HTTP/SSE servers used by the networking examples."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager


async def _drain_request(reader: asyncio.StreamReader) -> None:
    while True:
        line = await reader.readline()
        if not line or line in (b"\r\n", b"\n"):
            return


@asynccontextmanager
async def http_server(
    responder: Callable[[asyncio.StreamWriter], Awaitable[None]],
) -> AsyncIterator[str]:
    """Start a local HTTP server that serves one close-delimited response.

    Yields the base URL, for example ``http://127.0.0.1:53122``.
    """

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await _drain_request(reader)
            await responder(writer)
            await writer.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except (ConnectionError, RuntimeError):
                pass

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    sockets = server.sockets or []
    url = f"http://127.0.0.1:{sockets[0].getsockname()[1]}"
    try:
        yield url
    finally:
        server.close()
        await server.wait_closed()


async def begin_stream(
    writer: asyncio.StreamWriter,
    *,
    content_type: str = "text/plain; charset=utf-8",
) -> None:
    writer.write(
        (
            "HTTP/1.1 200 OK\r\n"
            f"Content-Type: {content_type}\r\n"
            "Cache-Control: no-cache\r\n"
            "Connection: close\r\n\r\n"
        ).encode()
    )
    await writer.drain()
