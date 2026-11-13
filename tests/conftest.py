"""Shared fixtures: local servers only, no external services."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest
from helpers import read_request


class LocalHTTPServer:
    def __init__(self, responder: Callable[[Any, asyncio.StreamWriter], Any]) -> None:
        self._responder = responder
        self._server: asyncio.AbstractServer | None = None
        self.port = 0
        self.requests: list[Any] = []

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        sockets = self._server.sockets or []
        self.port = sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request = await read_request(reader)
            if request is None:
                return
            self.requests.append(request)
            await self._responder(request, writer)
            await writer.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except (ConnectionError, RuntimeError):
                pass


@pytest.fixture
async def http_server():
    """Start local HTTP servers; every one is stopped at teardown."""
    servers: list[LocalHTTPServer] = []

    async def start(responder: Callable[[Any, asyncio.StreamWriter], Any]) -> LocalHTTPServer:
        server = LocalHTTPServer(responder)
        await server.start()
        servers.append(server)
        return server

    yield start
    for server in servers:
        await server.stop()


@pytest.fixture
async def ws_server():
    """Start local WebSocket servers; every one is stopped at teardown."""
    websockets = pytest.importorskip("websockets")
    servers: list[Any] = []

    async def start(handler: Callable[..., Any]) -> str:
        server = await websockets.serve(handler, "127.0.0.1", 0)
        servers.append(server)
        sockets = server.sockets or []
        return f"ws://127.0.0.1:{sockets[0].getsockname()[1]}"

    yield start
    for server in servers:
        server.close()
        await server.wait_closed()
