# Real-time transports and events

DataCurrent ships one in-memory fan-out primitive, one event bus, and three
network clients. All network clients are stream-shaped, cancellation-safe, and
use credentials-redacted logging.

## Channel (broadcast)

```python
from datacurrent import Channel

channel = Channel(buffer_size=64, overflow="block", name="news")
alerts = channel.subscribe()
archive = channel.subscribe()

await channel.publish("deploy finished")  # returns number of subscribers reached
await channel.aclose()
```

Each subscriber has its own bounded queue, so a slow consumer applies
backpressure only to itself (with `block`) or drops with a configured policy.
Delivery is at most once, in-process, with no history for late subscribers.
`channel.stats()` reports published/delivered/dropped/subscriber counts.

## EventBus

```python
from datacurrent import EventBus

bus = EventBus(buffer_size=1024, overflow="block")


@bus.subscribe("user.*")
async def on_user(event):
    print(event.topic, event.data)


@bus.subscribe("user.deleted")
async def cleanup(event):
    await delete_user_data(event.data["user_id"])


await bus.publish("user.created", {"user_id": 123})
await bus.aclose(drain=True)
```

Topics are dot-separated. Patterns: exact segments, `*` for one segment, and a
trailing `**` (or `#`) for any remaining segments. `*` alone matches everything.

Events are immutable envelopes with `topic`, `data`, `id`, `timestamp`
(UTC), read-only `metadata` and optional `source`.

Consume events as a stream:

```python
stream = bus.stream("orders.**").filter(lambda event: event.data["total"] > 10)
async for event in stream:
    ...
```

Semantics:

- at most once, in memory, no persistence or replay;
- per-subscriber FIFO queues with the configured overflow policy;
- handler errors are isolated per event and reported via `on_error` or logging;
- `aclose(drain=True)` waits for queued work and in-flight handlers before
  shutting down (bounded by `timeout`);
- publishing after close raises `StreamClosedError`.

## WebSocket

```python
from datacurrent import WebSocketStream

async with WebSocketStream.connect(
    "wss://example.com/events",
    headers={"Authorization": "Bearer ..."},
    mode="json",
    max_size=1_048_576,
    ping_interval=20,
    reconnect_attempts=3,
) as stream:
    await stream.send_json({"subscribe": "trades"})
    async for message in stream:
        print(message)
```

- `mode="auto"` yields `str`/`bytes`; `"text"` decodes binary frames; `"json"`
  parses text frames.
- `send`, `send_json`, `ping`, `recv` and async iteration are available.
- Heartbeats use protocol ping/pong (`ping_interval`, `ping_timeout`).
- Reconnects only after abnormal closes (codes other than 1000), up to
  `reconnect_attempts`, with exponential backoff.
- Normal close code 1000 ends iteration cleanly; abnormal closure raises
  `WebSocketClosedError`.
- TLS is verified by default for `wss://`. Only an explicit `ssl=` context can
  change that.
- Headers are credentials: they are redacted in logs and error messages.

`WS_URL` must start with `ws://` or `wss://`; anything else raises
`ConfigurationError`.

## Server-Sent Events

```python
from datacurrent import SSEStream

async with SSEStream.connect(
    "https://example.com/events",
    headers={"Authorization": "Bearer ..."},
    last_event_id=None,
    max_reconnects=3,
    reconnect_delay=1.0,
) as stream:
    async for event in stream:
        print(event.event, event.id, event.retry, event.json())
```

Parsing follows the WHATWG event-stream rules:

- multiple `data:` lines are joined with newlines;
- blank lines dispatch; comment lines (`: heartbeat`) are ignored;
- `event`, `id` and integer `retry` fields are honoured;
- `id` persists across events; `retry` sets the reconnection delay;
- a trailing event without a final blank line is flushed.

Reconnection sends `Last-Event-ID` from the most recent event, applies the
server's retry hint when present, and stops after `max_reconnects` additional
connections. Content type is checked (`text/event-stream`) by default and a
mismatch raises `SSEProtocolError`.

Parsing and encoding are available without a network:

```python
from datacurrent import SSEEvent, format_sse, parse_sse

text = format_sse(SSEEvent(data="hello", event="tick", id="1"))
# 'id: 1\nevent: tick\ndata: hello\n\n'


async def chunks():
    yield text


events = [event async for event in parse_sse(chunks())]
```

`format_heartbeat()` produces a comment heartbeat for server responses.

## HTTP streaming

```python
from datacurrent import HTTPStream

async with HTTPStream.get("https://example.com/feed.ndjson", timeout=30) as response:
    print(response.status_code)
    async for line in response.iter_lines():
        handle(line)

    # or:
    async for chunk in response.iter_bytes(chunk_size=65536):
        process(chunk)
```

- `iter_bytes`, `iter_text`, `iter_lines`, `read_all(max_bytes=...)`.
- `as_stream()` exposes the body as a `Stream` and owns closing the response.
- The response is always closed when the context exits, iteration ends, or the
  consuming task is cancelled.
- `max_bytes` raises `MessageTooLargeError`; HTTP errors raise
  `httpx.HTTPStatusError`.
- The object must be opened with `async with` (or `as_stream()`), otherwise
  iteration raises `StreamClosedError`.

## Custom transports

Subclass `StreamTransport` when you have another protocol:

```python
from collections.abc import AsyncIterator
from datacurrent import StreamTransport


class SensorTransport(StreamTransport[dict]):
    def __init__(self) -> None:
        super().__init__(name="sensor")

    async def open(self) -> None:
        self._opened = True
        self._closed = False

    async def close(self) -> None:
        self._closed = True
        self._opened = False

    async def messages(self) -> AsyncIterator[dict]:
        for index in range(5):
            if self._closed:
                return
            yield {"index": index}


async with SensorTransport() as transport:
    readings = await transport.as_stream().collect()
```

`as_stream()` opens the transport if needed and closes it when iteration ends,
fails or is stopped early.

## FastAPI and Starlette

```python
from fastapi import FastAPI
from datacurrent import SSEEvent
from datacurrent.integrations.fastapi import sse_response

app = FastAPI()


@app.get("/events")
async def events():
    async def generate():
        for index in range(10):
            yield SSEEvent(data=f"tick {index}", event="tick")

    return sse_response(generate())
```

Starlette helpers:

- `streaming_response(stream, media_type=..., headers=...)` — bytes pass through,
  strings are UTF-8 encoded, and the stream is closed when the response ends.
- `StreamLifespan(*resources)` — closes resources on ASGI shutdown; pass
  `lifespan.as_context_manager()` to an application.
- `websocket_messages(websocket)` — async iterator over text/binary messages.
- `websocket_stream(websocket)` — the same as a `Stream`.

Both modules import their framework lazily.

## Security notes

- Credentials in URLs and headers are redacted from logs and error messages.
- TLS verification is never disabled by DataCurrent.
- Set `max_size` on WebSockets and `max_bytes` on HTTP bodies for untrusted
  endpoints.
- Delivery from `Channel` and `EventBus` is at most once; do not use them as a
  durable queue.
