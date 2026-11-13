from __future__ import annotations

import asyncio

import pytest
from helpers import begin_response, write_body

from datacurrent import ConfigurationError, HTTPStream, MessageTooLargeError, StreamClosedError


async def test_iter_bytes_streams_body_in_chunks(http_server) -> None:
    payload = b"x" * 50_000

    async def responder(request, writer):
        await begin_response(writer, content_type="application/octet-stream")
        await write_body(writer, payload, chunks=10, delay=0.001)

    server = await http_server(responder)
    async with HTTPStream(server.url + "/blob", chunk_size=4096) as response:
        chunks = [chunk async for chunk in response.iter_bytes()]
    assert b"".join(chunks) == payload
    assert len(chunks) > 1


async def test_read_all_and_iter_lines(http_server) -> None:
    async def responder(request, writer):
        await begin_response(writer)
        await write_body(writer, b"alpha\nbeta\ngamma", chunks=3, delay=0.001)

    server = await http_server(responder)
    async with HTTPStream(server.url) as response:
        assert await response.read_all() == b"alpha\nbeta\ngamma"

    async with HTTPStream(server.url) as response:
        lines = [line async for line in response.iter_lines()]
    assert lines == ["alpha", "beta", "gamma"]


async def test_read_all_enforces_limit(http_server) -> None:
    async def responder(request, writer):
        await begin_response(writer)
        await write_body(writer, b"0123456789")

    server = await http_server(responder)
    async with HTTPStream(server.url, max_bytes=4) as response:
        with pytest.raises(MessageTooLargeError):
            await response.read_all()


async def test_http_error_status_raises(http_server) -> None:
    httpx = pytest.importorskip("httpx")

    async def responder(request, writer):
        await begin_response(writer, status=500, reason="Server Error")
        await write_body(writer, b"failed")

    server = await http_server(responder)
    with pytest.raises(httpx.HTTPStatusError):
        async with HTTPStream(server.url):
            pass


async def test_as_stream_owns_and_closes_response(http_server) -> None:
    async def responder(request, writer):
        await begin_response(writer)
        await write_body(writer, b"hello world", chunks=2, delay=0.001)

    server = await http_server(responder)
    chunks = await HTTPStream(server.url).as_stream().collect()
    assert b"".join(chunks) == b"hello world"


async def test_using_without_context_manager_fails_clearly() -> None:
    response = HTTPStream("http://127.0.0.1:1/data")
    with pytest.raises(StreamClosedError):
        async for _ in response.iter_bytes():
            pass


async def test_invalid_configuration() -> None:
    with pytest.raises(ConfigurationError):
        HTTPStream("ftp://example.com")
    with pytest.raises(ConfigurationError):
        HTTPStream("http://example.com", chunk_size=0)
    with pytest.raises(ConfigurationError):
        HTTPStream("http://example.com", max_bytes=0)


async def test_headers_and_params_are_sent(http_server) -> None:
    async def responder(request, writer):
        await begin_response(writer)
        await write_body(writer, b"ok")

    server = await http_server(responder)
    async with HTTPStream(
        server.url + "/path",
        params={"page": "2"},
        headers={"X-Test": "yes"},
    ) as response:
        await response.read_all()
    request = server.requests[0]
    assert request.path.endswith("/path?page=2")
    assert request.headers["x-test"] == "yes"


async def test_cancellation_closes_response(http_server) -> None:
    async def responder(request, writer):
        await begin_response(writer)
        for _ in range(100):
            writer.write(b"tick\n")
            await writer.drain()
            await asyncio.sleep(0.01)

    server = await http_server(responder)

    async def consume() -> None:
        async with HTTPStream(server.url) as response:
            async for _ in response.iter_lines():
                pass

    task = asyncio.ensure_future(consume())
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
