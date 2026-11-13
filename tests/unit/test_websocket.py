from __future__ import annotations

import asyncio

import pytest

from datacurrent import (
    ConfigurationError,
    ConnectionFailedError,
    MessageTooLargeError,
    WebSocketClosedError,
    WebSocketStream,
)


async def test_send_and_receive_text(ws_server) -> None:
    async def echo(websocket):
        async for message in websocket:
            await websocket.send("echo:" + message)

    url = await ws_server(echo)
    async with WebSocketStream.connect(url) as stream:
        await stream.send("hello")
        assert await stream.recv() == "echo:hello"


async def test_json_mode(ws_server) -> None:
    async def echo(websocket):
        async for message in websocket:
            await websocket.send(message)

    url = await ws_server(echo)
    async with WebSocketStream(url, mode="json") as stream:
        await stream.send_json({"type": "ping", "n": 1})
        assert await stream.recv() == {"type": "ping", "n": 1}


async def test_binary_frames_in_text_mode(ws_server) -> None:
    async def send_binary(websocket):
        await websocket.send(b"hello bytes")
        await asyncio.sleep(0.2)

    url = await ws_server(send_binary)
    async with WebSocketStream(url, mode="text") as stream:
        assert await stream.recv() == "hello bytes"


async def test_async_iteration(ws_server) -> None:
    async def burst(websocket):
        for number in range(3):
            await websocket.send(f"message-{number}")
        await asyncio.sleep(0.2)

    url = await ws_server(burst)
    received = []
    async with WebSocketStream(url) as stream:
        async for message in stream:
            received.append(message)
            if len(received) == 3:
                break
    assert received == ["message-0", "message-1", "message-2"]


async def test_normal_close_ends_iteration(ws_server) -> None:
    async def close_normally(websocket):
        await websocket.send("bye")
        await websocket.close(code=1000)

    url = await ws_server(close_normally)
    async with WebSocketStream(url) as stream:
        messages = [message async for message in stream]
    assert messages == ["bye"]


async def test_abnormal_close_raises(ws_server) -> None:
    async def crash(websocket):
        await websocket.send("first")
        await websocket.close(code=1011, reason="boom")

    url = await ws_server(crash)
    with pytest.raises(WebSocketClosedError):
        async with WebSocketStream(url, reconnect_attempts=0) as stream:
            async for _ in stream:
                pass


async def test_reconnects_and_continues(ws_server) -> None:
    connections = {"count": 0}

    async def restarting(websocket):
        connections["count"] += 1
        if connections["count"] == 1:
            await websocket.send("one")
            await websocket.close(code=1011, reason="restart")
        else:
            await websocket.send("two")
            await asyncio.sleep(0.3)

    url = await ws_server(restarting)
    received = []
    async with WebSocketStream(url, reconnect_attempts=1, reconnect_delay=0) as stream:
        async for message in stream:
            received.append(message)
            if len(received) == 2:
                break
    assert received == ["one", "two"]
    assert connections["count"] == 2


async def test_message_size_limit() -> None:
    stream = WebSocketStream("ws://127.0.0.1:1/ws", max_size=4)
    with pytest.raises(MessageTooLargeError):
        stream._decode("12345")


async def test_connection_failure_is_reported() -> None:
    with pytest.raises(ConnectionFailedError):
        await WebSocketStream("ws://127.0.0.1:1/ws").__aenter__()


async def test_invalid_configuration() -> None:
    with pytest.raises(ConfigurationError):
        WebSocketStream("http://example.com")
    with pytest.raises(ConfigurationError):
        WebSocketStream("ws://example.com", mode="binary")
    with pytest.raises(ConfigurationError):
        WebSocketStream("ws://example.com", reconnect_attempts=-1)
    with pytest.raises(ConfigurationError):
        WebSocketStream("ws://example.com", max_size=0)


async def test_send_before_connect_fails_clearly() -> None:
    stream = WebSocketStream("ws://127.0.0.1:1/ws")
    with pytest.raises(Exception) as info:
        await stream.send("nope")
    assert "not connected" in str(info.value)
