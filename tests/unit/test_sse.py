from __future__ import annotations

import asyncio
import socket

import pytest
from helpers import begin_response, write_body

from datacurrent import (
    ConnectionFailedError,
    SSEEvent,
    SSEProtocolError,
    SSEStream,
    StreamClosedError,
    format_heartbeat,
    format_sse,
    parse_sse,
)
from datacurrent.transports.sse import SSELineDecoder, SSEParser


async def _parse(chunks: list[str | bytes]) -> list[SSEEvent]:
    async def source():
        for chunk in chunks:
            yield chunk

    return [event async for event in parse_sse(source())]


async def test_parse_basic_event() -> None:
    events = await _parse(["data: hello\n\n"])
    assert events == [SSEEvent(data="hello")]


async def test_parse_multiline_data_and_event_type() -> None:
    events = await _parse(["event: tick\ndata: one\ndata: two\n\n"])
    assert events == [SSEEvent(data="one\ntwo", event="tick")]


async def test_comments_and_heartbeats_are_ignored() -> None:
    events = await _parse([": keep-alive\n\n", "data: real\n\n", ": ping\n"])
    assert events == [SSEEvent(data="real")]


async def test_unknown_fields_are_ignored() -> None:
    events = await _parse(["foo: bar\ndata: value\n\n"])
    assert events == [SSEEvent(data="value")]


async def test_id_persists_and_retry_recorded() -> None:
    events = await _parse(["id: 7\nretry: 1500\ndata: a\n\n", "data: b\n\n"])
    assert [event.id for event in events] == ["7", "7"]
    assert events[0].retry == 1500
    assert events[1].retry == 1500


async def test_empty_data_field_dispatches_empty_event() -> None:
    events = await _parse(["data:\n\n"])
    assert events == [SSEEvent(data="")]


async def test_block_without_data_does_not_dispatch() -> None:
    events = await _parse(["event: ignored\n\n", "data: kept\n\n"])
    assert events == [SSEEvent(data="kept")]


async def test_crlf_and_cr_line_endings_split_across_chunks() -> None:
    events = await _parse(["data: one\r", "\n\r\n", "data: two\r\r"])
    assert events == [SSEEvent(data="one"), SSEEvent(data="two")]


async def test_bytes_input_is_decoded() -> None:
    events = await _parse([b"data: caf\xc3\xa9\n\n"])
    assert events == [SSEEvent(data="café")]


async def test_trailing_event_without_blank_line_is_flushed() -> None:
    events = await _parse(["data: last"])
    assert events == [SSEEvent(data="last")]


async def test_line_decoder_waits_for_split_crlf() -> None:
    decoder = SSELineDecoder()
    assert decoder.feed("data: a\r") == []
    assert decoder.feed("\n") == ["data: a"]
    assert decoder.flush() == []


async def test_parser_json_accessor() -> None:
    events = await _parse(['data: {"n": 1}\n\n'])
    assert events[0].json() == {"n": 1}


async def test_format_sse_roundtrip() -> None:
    original = SSEEvent(data="a\nb", event="tick", id="9", retry=1000)
    events = await _parse([format_sse(original)])
    assert events == [original]


async def test_format_heartbeat() -> None:
    assert format_heartbeat() == ": keep-alive\n\n"
    assert format_heartbeat("ping") == ": ping\n\n"


async def test_format_rejects_non_events() -> None:
    with pytest.raises(TypeError):
        format_sse("data: nope")  # type: ignore[arg-type]


async def test_parser_last_event_id_property() -> None:
    parser = SSEParser()
    parser.feed_line("id: 42")
    parser.feed_line("data: x")
    assert parser.feed_line("") == SSEEvent(data="x", id="42")
    assert parser.last_event_id == "42"


async def _collect(client: SSEStream, limit: int | None = None) -> list[SSEEvent]:
    events: list[SSEEvent] = []
    iterator = client.__aiter__()
    try:
        while limit is None or len(events) < limit:
            events.append(await asyncio.wait_for(iterator.__anext__(), timeout=3))
    except StopAsyncIteration:
        pass
    finally:
        await iterator.aclose()
        await client.aclose()
    return events


async def test_client_reads_events_from_local_server(http_server) -> None:
    async def responder(request, writer):
        await begin_response(writer, content_type="text/event-stream")
        writer.write(b": heartbeat\n\n")
        writer.write(b"id: 1\nevent: tick\ndata: hello\n\n")
        await writer.drain()
        writer.write(b'data: {"n": 2}\n\n')
        await writer.drain()

    server = await http_server(responder)
    client = SSEStream.connect(server.url + "/events")
    events = await _collect(client, limit=2)
    assert [event.data for event in events] == ["hello", '{"n": 2}']
    assert events[0].event == "tick"
    assert events[0].id == "1"
    assert client.last_event_id == "1"


async def test_client_sends_accept_header(http_server) -> None:
    async def responder(request, writer):
        await begin_response(writer, content_type="text/event-stream")
        writer.write(b"data: ok\n\n")
        await writer.drain()

    server = await http_server(responder)
    await _collect(SSEStream(server.url), limit=1)
    assert server.requests[0].headers["accept"] == "text/event-stream"


async def test_client_rejects_wrong_content_type(http_server) -> None:
    async def responder(request, writer):
        await begin_response(writer, content_type="text/plain")
        await write_body(writer, b"data: nope\n\n")

    server = await http_server(responder)
    with pytest.raises(SSEProtocolError):
        await _collect(SSEStream(server.url))


async def test_client_raises_for_http_error(http_server) -> None:
    httpx = pytest.importorskip("httpx")

    async def responder(request, writer):
        await begin_response(writer, status=404, reason="Not Found")
        await write_body(writer, b"missing")

    server = await http_server(responder)
    with pytest.raises(httpx.HTTPStatusError):
        await _collect(SSEStream(server.url))


async def test_client_reconnects_with_last_event_id(http_server) -> None:
    state = {"connections": 0}

    async def responder(request, writer):
        state["connections"] += 1
        await begin_response(writer, content_type="text/event-stream")
        if state["connections"] == 1:
            writer.write(b"id: 1\ndata: first\n\n")
        else:
            writer.write(b"data: second\n\n")
        await writer.drain()

    server = await http_server(responder)
    client = SSEStream(
        server.url,
        max_reconnects=1,
        reconnect_delay=0,
    )
    events = await _collect(client)
    assert [event.data for event in events] == ["first", "second"]
    assert state["connections"] == 2
    assert server.requests[1].headers["last-event-id"] == "1"


async def test_client_connection_failure_is_reported() -> None:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    client = SSEStream(f"http://127.0.0.1:{port}/events", max_reconnects=0)
    with pytest.raises(ConnectionFailedError):
        await _collect(client)


async def test_client_requires_http_url() -> None:
    with pytest.raises(ValueError):
        SSEStream("ftp://example.com/events")
    with pytest.raises(ValueError):
        SSEStream("https://example.com", max_reconnects=-1)


async def test_closed_client_rejects_iteration() -> None:
    client = SSEStream("http://127.0.0.1:1/events")
    await client.aclose()
    with pytest.raises(StreamClosedError):
        await _collect(client)
