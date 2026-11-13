from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from helpers import begin_response

from datacurrent import (
    ConfigurationError,
    SSEEvent,
    SSEProtocolError,
    SSEStream,
    StreamClosedError,
    WebSocketClosedError,
    WebSocketStream,
    format_sse,
)
from datacurrent.transports.sse import SSELineDecoder, SSEParser
from datacurrent.transports.websocket import _close_details


# --------------------------------------------------------------------------- #
# SSE parser / client
# --------------------------------------------------------------------------- #
def test_sse_parser_and_decoder_edges() -> None:
    decoder = SSELineDecoder()
    assert decoder.feed("") == []

    parser = SSEParser()
    assert parser.feed_line("data") is None
    assert parser.feed_line("") == SSEEvent(data="")
    assert parser.feed_line("retry: nope") is None

    parser = SSEParser()
    parser.feed_line("id: bad\x00id")
    parser.feed_line("data: value")
    assert parser.feed_line("").id is None

    parser = SSEParser()
    parser.feed_line("retry: -1")
    parser.feed_line("data: value")
    assert parser.feed_line("").retry is None

    assert format_sse(SSEEvent(data="plain")) == "data: plain\n\n"


async def test_sse_validation_and_properties(http_server) -> None:
    with pytest.raises(ConfigurationError):
        SSEStream("http://example.com", max_reconnects=True)  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError):
        SSEStream("http://example.com", reconnect_delay=-1)
    with pytest.raises(ConfigurationError):
        SSEStream("http://example.com", max_reconnect_delay=-1)

    async def responder(request, writer) -> None:
        await begin_response(writer, content_type="text/event-stream")
        writer.write(b"data: one\n\n")
        await writer.drain()
        await asyncio.sleep(10)

    server = await http_server(responder)
    client = SSEStream(server.url)
    assert client.url == server.url
    assert client.closed is False

    iterator = client.__aiter__()
    event = await asyncio.wait_for(iterator.__anext__(), timeout=3)
    assert event.data == "one"
    await client.aclose()
    assert client.closed is True
    await iterator.aclose()

    async with SSEStream(server.url) as scoped:
        events = []
        async for event in scoped:
            events.append(event)
            break
    assert events == [SSEEvent(data="one")]
    assert scoped.closed is True

    hint = SSEStream("http://example.com")
    hint._retry_hint = 0.25
    assert hint._next_delay(5) == 0.25


class _FakeResponse:
    def __init__(
        self,
        chunks: list[bytes] | None = None,
        *,
        delay: float = 0.0,
        content_type: str = "text/event-stream",
    ) -> None:
        self.status_code = 200
        self.headers = {"content-type": content_type}
        self._chunks = chunks or []
        self._delay = delay

    async def aiter_bytes(self):
        if self._delay:
            await asyncio.sleep(self._delay)
        for chunk in self._chunks:
            yield chunk

    def raise_for_status(self) -> None:
        return None


class _FakeContext:
    def __init__(self, result: Any) -> None:
        self._result = result

    async def __aenter__(self) -> Any:
        if isinstance(self._result, BaseException):
            raise self._result
        return self._result

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False


class _FakeClient:
    def __init__(self, results: list[Any]) -> None:
        self._results = list(results)
        self.calls = 0
        self.closed = False

    def stream(self, *args: Any, **kwargs: Any) -> _FakeContext:
        index = min(self.calls, len(self._results) - 1)
        self.calls += 1
        return _FakeContext(self._results[index])

    async def aclose(self) -> None:
        self.closed = True


async def test_sse_custom_client_and_http_error_reconnect() -> None:
    fake = _FakeClient([_FakeResponse([b"data: custom\n\n"])])
    client = SSEStream("http://example.com/events", client=fake)
    events = [event async for event in client]
    assert [event.data for event in events] == ["custom"]
    assert fake.calls == 1

    fake = _FakeClient(
        [
            httpx.ConnectError("connection lost"),
            _FakeResponse([b"data: recovered\n\n"]),
        ]
    )
    client = SSEStream(
        "http://example.com/events",
        client=fake,
        max_reconnects=1,
        reconnect_delay=0,
    )
    events = [event async for event in client]
    assert [event.data for event in events] == ["recovered"]
    assert fake.calls == 2


async def test_sse_content_type_check_can_be_disabled() -> None:
    fake = _FakeClient([_FakeResponse([b"data: accepted\n\n"], content_type="text/plain")])
    client = SSEStream(
        "http://example.com/events",
        client=fake,
        check_content_type=False,
    )
    assert [event.data async for event in client] == ["accepted"]


async def test_sse_retry_hint_and_cancellation() -> None:
    fake = _FakeClient(
        [
            _FakeResponse([b"retry: 0\ndata: one\n\n"]),
            _FakeResponse([b"data: two\n\n"]),
        ]
    )
    client = SSEStream(
        "http://example.com/events",
        client=fake,
        max_reconnects=1,
        reconnect_delay=10,
    )
    events = [event async for event in client]
    assert [event.data for event in events] == ["one", "two"]
    assert client._retry_hint == 0.0

    fake = _FakeClient(
        [
            _FakeResponse([b"data: first\n\n"]),
            _FakeResponse([b"data: second\n\n"]),
        ]
    )
    client = SSEStream(
        "http://example.com/events",
        client=fake,
        max_reconnects=1,
        reconnect_delay=0.001,
    )
    assert [event.data async for event in client] == ["first", "second"]

    fake = _FakeClient([_FakeResponse([], delay=10)])
    client = SSEStream("http://example.com/events", client=fake)
    iterator = client.__aiter__()
    task = asyncio.create_task(iterator.__anext__())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await iterator.aclose()


async def test_sse_wrong_content_type_raises() -> None:
    fake = _FakeClient([_FakeResponse([b"data: nope\n\n"], content_type="text/plain")])
    client = SSEStream("http://example.com/events", client=fake)
    with pytest.raises(SSEProtocolError):
        [event async for event in client]


# --------------------------------------------------------------------------- #
# WebSocket fake-server paths
# --------------------------------------------------------------------------- #
class _FakeConnectionClosed(Exception):
    def __init__(self, code: int | None = None, reason: str | None = None) -> None:
        self.rcvd = SimpleNamespace(code=code, reason=reason)
        super().__init__(f"closed code={code} reason={reason}")


class _FakeWebSocket:
    def __init__(
        self,
        *,
        messages: list[Any] | None = None,
        exc: BaseException | None = None,
        recv_value: Any = None,
        recv_exc: BaseException | None = None,
        close_exc: BaseException | None = None,
        on_exit: Any = None,
    ) -> None:
        self.messages = list(messages or [])
        self.exc = exc
        self.recv_value = recv_value
        self.recv_exc = recv_exc
        self.close_exc = close_exc
        self.on_exit = on_exit
        self.sent: list[Any] = []
        self.closed = False
        self.subprotocol = "chat"

    async def send(self, data: Any) -> None:
        self.sent.append(data)

    async def ping(self) -> Any:
        async def waiter() -> None:
            return None

        return waiter()

    async def recv(self) -> Any:
        if self.recv_exc is not None:
            raise self.recv_exc
        return self.recv_value

    async def close(self) -> None:
        if self.close_exc is not None:
            raise self.close_exc
        self.closed = True

    def __aiter__(self) -> _FakeWebSocket:
        return self

    async def __anext__(self) -> Any:
        if self.messages:
            return self.messages.pop(0)
        if self.on_exit is not None:
            self.on_exit()
        if self.exc is not None:
            raise self.exc
        raise StopAsyncIteration


def _patch_websockets(monkeypatch: pytest.MonkeyPatch, connect: Any) -> None:
    module = SimpleNamespace(connect=connect, ConnectionClosed=_FakeConnectionClosed)
    monkeypatch.setattr(
        "datacurrent.transports.websocket.require_module",
        lambda *args, **kwargs: module,
    )


def test_websocket_close_details_fallbacks() -> None:
    frame = SimpleNamespace(code=1000, reason="ok")
    error = SimpleNamespace(rcvd=frame, sent=None, code=999, reason="ignored")
    assert _close_details(error) == (1000, "ok")

    fallback = SimpleNamespace(rcvd=None, sent=None, code=1006, reason="lost")
    assert _close_details(fallback) == (1006, "lost")
    assert _close_details(SimpleNamespace(rcvd=None, sent=None)) == (None, None)


async def test_websocket_connect_kwargs_and_properties(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    async def connect(url: str, **kwargs: Any) -> _FakeWebSocket:
        captured.update(kwargs)
        return _FakeWebSocket()

    _patch_websockets(monkeypatch, connect)
    stream = WebSocketStream(
        "ws://example.com/ws",
        subprotocols=["chat"],
        ssl="context",
        origin="https://origin",
    )
    assert stream.url == "ws://example.com/ws"
    assert stream.connected is False
    assert stream.protocol is None

    await stream._connect()
    assert captured["subprotocols"] == ["chat"]
    assert captured["ssl"] == "context"
    assert captured["origin"] == "https://origin"
    assert stream.connected is True
    assert stream.protocol == "chat"

    assert stream._ws is not None
    stream._ws.close_exc = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await stream.aclose()


async def test_websocket_recv_ping_and_json(monkeypatch: pytest.MonkeyPatch) -> None:
    async def connect(url: str, **kwargs: Any) -> _FakeWebSocket:
        return _FakeWebSocket(recv_value="hello")

    _patch_websockets(monkeypatch, connect)
    stream = WebSocketStream("ws://example.com/ws")
    assert await stream.recv() == "hello"

    jsons = WebSocketStream("ws://example.com/ws", mode="json")
    assert jsons._decode(b'{"a": 1}') == {"a": 1}
    ws = _FakeWebSocket()
    jsons._ws = ws
    await jsons.ping()
    await jsons.send("text")
    await jsons.send_json({"x": 1})
    assert ws.sent[-1] == '{"x": 1}'

    closed = WebSocketStream("ws://example.com/ws")
    closed._ws = _FakeWebSocket(recv_exc=_FakeConnectionClosed(1006, "lost"))
    with pytest.raises(WebSocketClosedError):
        await closed.recv()
    await closed.aclose()

    await WebSocketStream("ws://example.com/ws").aclose()


async def test_websocket_iteration_edges(monkeypatch: pytest.MonkeyPatch) -> None:
    async def connect(url: str, **kwargs: Any) -> _FakeWebSocket:
        return _FakeWebSocket(messages=["one"])

    _patch_websockets(monkeypatch, connect)
    stream = WebSocketStream("ws://example.com/ws")
    assert [message async for message in stream] == ["one"]

    await stream.aclose()
    with pytest.raises(StreamClosedError):
        [message async for message in stream]

    async def close_normally(url: str, **kwargs: Any) -> _FakeWebSocket:
        return _FakeWebSocket(exc=_FakeConnectionClosed(1000, "bye"))

    _patch_websockets(monkeypatch, close_normally)
    assert [message async for message in WebSocketStream("ws://example.com/ws")] == []

    connections = iter(
        [
            _FakeWebSocket(exc=_FakeConnectionClosed(1011, "restart")),
            _FakeWebSocket(messages=["two"]),
        ]
    )

    async def reconnect(url: str, **kwargs: Any) -> _FakeWebSocket:
        return next(connections)

    _patch_websockets(monkeypatch, reconnect)
    stream = WebSocketStream(
        "ws://example.com/ws",
        reconnect_attempts=1,
        reconnect_delay=0.001,
    )
    assert [message async for message in stream] == ["two"]

    async def cancelled(url: str, **kwargs: Any) -> _FakeWebSocket:
        return _FakeWebSocket(exc=asyncio.CancelledError())

    _patch_websockets(monkeypatch, cancelled)
    with pytest.raises(asyncio.CancelledError):
        [message async for message in WebSocketStream("ws://example.com/ws")]

    reference: dict[str, WebSocketStream] = {}

    def mark_closed() -> None:
        reference["stream"]._closed = True

    async def closing(url: str, **kwargs: Any) -> _FakeWebSocket:
        return _FakeWebSocket(
            exc=_FakeConnectionClosed(1011, "lost"),
            on_exit=mark_closed,
        )

    _patch_websockets(monkeypatch, closing)
    stream = WebSocketStream("ws://example.com/ws")
    reference["stream"] = stream
    with pytest.raises(WebSocketClosedError):
        [message async for message in stream]

    async def no_connect(self: WebSocketStream) -> None:
        return None

    monkeypatch.setattr(WebSocketStream, "_connect", no_connect)
    with pytest.raises(StreamClosedError):
        [message async for message in WebSocketStream("ws://example.com/ws")]


async def test_websocket_connect_cancellation(monkeypatch: pytest.MonkeyPatch) -> None:
    async def connect(url: str, **kwargs: Any) -> _FakeWebSocket:
        raise asyncio.CancelledError

    _patch_websockets(monkeypatch, connect)
    with pytest.raises(asyncio.CancelledError):
        await WebSocketStream("ws://example.com/ws")._connect()


async def test_websocket_validation() -> None:
    with pytest.raises(ConfigurationError):
        WebSocketStream("ws://example.com", reconnect_attempts=True)  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError):
        WebSocketStream("ws://example.com", ping_interval=-1)
