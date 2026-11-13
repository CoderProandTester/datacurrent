from __future__ import annotations

import pytest
from helpers import begin_response, write_body

from datacurrent import (
    BoundedQueue,
    Channel,
    ConfigurationError,
    HTTPStream,
    StreamClosedError,
)
from datacurrent.buffering.backpressure import BufferPolicy
from datacurrent.config import OverflowPolicy
from datacurrent.exceptions import QueueEmptyError
from datacurrent.transports.base import StreamTransport


# --------------------------------------------------------------------------- #
# Queue / buffer policy
# --------------------------------------------------------------------------- #
async def test_queue_properties_bulk_and_context() -> None:
    queue = BoundedQueue(2, name="edge", overflow="drop_newest")
    assert queue.name == "edge"
    assert queue.maxsize == 2
    assert queue.overflow is OverflowPolicy.DROP_NEWEST
    assert queue.error is None
    assert queue.empty() is True
    assert queue.full() is False

    assert await queue.put_many([1, 2, 3]) == 2
    assert queue.full() is True
    assert queue.empty() is False
    assert await queue.get_nowait() == 1
    assert await queue.get_nowait() == 2

    with pytest.raises(QueueEmptyError):
        await queue.get_nowait()

    await queue.fail(RuntimeError("boom"))
    await queue.fail(ValueError("second"))
    assert isinstance(queue.error, RuntimeError)
    with pytest.raises(RuntimeError):
        await queue.get()
    with pytest.raises(RuntimeError):
        await queue.get_nowait()
    with pytest.raises(TypeError):
        await queue.fail("not-an-exception")  # type: ignore[arg-type]

    async with BoundedQueue(1) as scoped:
        assert scoped.qsize() == 0
        assert scoped.closed is False
    assert scoped.closed is True
    assert "BoundedQueue" in repr(queue)


async def test_queue_drains_items_before_raising_failure() -> None:
    queue = BoundedQueue(2)
    await queue.put("kept")
    await queue.fail(ValueError("later"))
    assert await queue.get() == "kept"
    with pytest.raises(ValueError):
        await queue.get()


def test_buffer_policy_validation() -> None:
    with pytest.raises(ConfigurationError):
        BufferPolicy(size=True)  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError):
        BufferPolicy(size=0)


# --------------------------------------------------------------------------- #
# Channel
# --------------------------------------------------------------------------- #
async def test_channel_drop_newest_counts_drops() -> None:
    channel = Channel(buffer_size=1, overflow="drop_newest")
    subscriber = channel.subscribe()
    assert await channel.publish(1) == 1
    assert await channel.publish(2) == 0
    assert channel.stats()["dropped"] == 1
    await channel.aclose()
    assert await subscriber.collect() == [1]


async def test_channel_properties_bulk_and_closed_subscriber() -> None:
    channel = Channel(buffer_size=2, name="edge")
    assert channel.name == "edge"
    assert channel.closed is False

    subscriber = channel.subscribe()
    assert await channel.publish_many([1, 2]) == 2
    assert await subscriber.take(2).collect() == [1, 2]

    doomed = channel.subscribe()
    queue = next(iter(channel._subscribers))
    await queue.aclose()
    assert await channel.publish(3) == 0
    assert channel.subscriber_count == 0
    assert doomed is not None

    await channel.aclose()
    await channel.aclose()
    assert "Channel" in repr(channel)

    async with Channel(name="context") as scoped:
        assert scoped.name == "context"
        assert scoped.closed is False
    assert scoped.closed is True


# --------------------------------------------------------------------------- #
# Custom transport base
# --------------------------------------------------------------------------- #
class _DemoTransport(StreamTransport[int]):
    async def open(self) -> None:
        self._opened = True
        self._closed = False

    async def close(self) -> None:
        self._closed = True
        self._opened = False

    async def messages(self):
        yield 1
        yield 2


async def test_transport_base_helpers() -> None:
    transport = _DemoTransport(name="demo")
    assert transport.is_open is False
    await transport._ensure_open()
    transport._closed = True
    with pytest.raises(StreamClosedError):
        await transport._ensure_open()

    async with _DemoTransport() as opened:
        assert opened.is_open is True
    assert await _DemoTransport().as_stream().collect() == [1, 2]


# --------------------------------------------------------------------------- #
# HTTP streaming
# --------------------------------------------------------------------------- #
async def _plain_body(request, writer) -> None:
    await begin_response(writer)
    await write_body(writer, b"hello world")


async def test_http_properties_get_and_idempotent_close(http_server) -> None:
    server = await http_server(_plain_body)
    stream = HTTPStream.get(server.url)
    assert stream.url == server.url

    async with stream as response:
        assert response.status_code == 200
        assert response.response.status_code == 200
        chunks = [chunk async for chunk in response]
    assert b"".join(chunks) == b"hello world"

    await response.aclose()
    unopened = HTTPStream(server.url)
    await unopened.aclose()


async def test_http_iter_text(http_server) -> None:
    payload = "héllo".encode()

    async def responder(request, writer) -> None:
        await begin_response(writer)
        await write_body(writer, payload, chunks=2, delay=0.001)

    server = await http_server(responder)
    async with HTTPStream(server.url) as response:
        text = "".join([chunk async for chunk in response.iter_text()])
    assert text == "héllo"


async def test_http_custom_client(http_server) -> None:
    httpx = pytest.importorskip("httpx")
    server = await http_server(_plain_body)
    client = httpx.AsyncClient()
    try:
        async with HTTPStream(server.url, client=client) as response:
            assert await response.read_all() == b"hello world"
    finally:
        await client.aclose()


async def test_http_as_stream_when_already_open(http_server) -> None:
    server = await http_server(_plain_body)
    async with HTTPStream(server.url) as response:
        chunks = await response.as_stream().collect()
    assert b"".join(chunks) == b"hello world"
