# DataCurrent

### Python streaming, made simple.

DataCurrent is a production-ready asynchronous streaming library. It gives you
one consistent API for lists, async generators, queues, WebSockets, Server-Sent
Events and streaming HTTP responses.

**Version 1.1.2 · Stable / Production**

[![PyPI version](https://img.shields.io/pypi/v/datacurrent.svg)](https://pypi.org/project/datacurrent/)
[![Python versions](https://img.shields.io/pypi/pyversions/datacurrent.svg)](https://pypi.org/project/datacurrent/)
[![CI](https://github.com/CoderProandTester/datacurrent/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/CoderProandTester/datacurrent/actions/workflows/ci.yml)
[![GitHub](https://img.shields.io/badge/GitHub-CoderProandTester%2Fdatacurrent-181717)](https://github.com/CoderProandTester/datacurrent)

- **GitHub repository:** <https://github.com/CoderProandTester/datacurrent>
- **PyPI homepage:** <https://pypi.org/project/datacurrent/>
- **Examples:** <https://github.com/CoderProandTester/datacurrent/tree/main/examples>

- 279 tests passing, with 100% line and branch coverage.
- Required networking dependencies install automatically.
- No broker, database, API key or hosted service.

## Install

```bash
pip install datacurrent
```

Python 3.11 or newer is required. `httpx` and `websockets` are installed with the
package, so HTTP streaming, SSE and WebSocket clients work immediately. The
[`examples/`](https://github.com/CoderProandTester/datacurrent/tree/main/examples)
directory contains 16 runnable scripts, from a hello-world stream to a WebSocket
server.

Framework helpers are optional:

```bash
pip install "datacurrent[fastapi]"
pip install "datacurrent[starlette]"
pip install "datacurrent[all]"
```

## Quick start

```python
import asyncio
from datacurrent import Stream


async def main() -> None:
    result = await (
        Stream.from_iterable(range(10))
        .filter(lambda number: number % 2 == 0)
        .map(lambda number: number * number)
        .take(4)
        .collect()
    )
    print(result)  # [0, 4, 16, 36]


asyncio.run(main())
```

Streams are lazy and single-consumer:

```python
stream = Stream.from_iterable([1, 2, 3])

async for item in stream:
    print(item)

# A second iteration raises StreamConsumedError.
```

Common terminals:

```python
await Stream.from_iterable([1, 2, 3]).collect()  # [1, 2, 3]
await Stream.from_iterable([1, 2, 3]).count()  # 3
await Stream.from_iterable([3, 1, 2]).reduce(max)  # 3
await Stream.empty().first("fallback")  # "fallback"
```

Async callbacks work everywhere a callback is accepted:

```python
users = await Stream.from_iterable(user_ids).map(fetch_user, concurrency=8).collect()
```

## Operators

`map`, `filter`, `flat_map`, `take`, `skip`, `batch`, `buffer`, `distinct`,
`enumerate`, `window`, `debounce`, `throttle`, `timeout`, `retry`, `recover`,
`tap`, `zip`, `merge` and `concat`.

```python
batches = Stream.from_iterable(range(1_000)).filter(lambda number: number % 3 == 0).batch(100)

async for batch in batches:
    print(len(batch))
```

## Backpressure

Buffers are bounded and block the producer by default. Data is not silently
dropped.

```python
stream = Stream.from_iterable(range(100_000)).buffer(size=64)
```

Lossy behaviour must be explicit and is counted:

```python
stream = Stream.from_iterable(range(10_000)).buffer(
    size=16,
    overflow="drop_oldest",
)
items = await stream.collect()
print(len(items), stream.stats["dropped"])
```

Policies: `block`, `drop_newest`, `drop_oldest`, `error`.

## Real-time data

### WebSocket

```python
from datacurrent import WebSocketStream

async with WebSocketStream.connect("wss://example.com/events") as stream:
    await stream.send_json({"subscribe": "trades"})
    async for message in stream:
        print(message)
```

### Server-Sent Events

```python
from datacurrent import SSEStream

async with SSEStream.connect("https://example.com/events") as stream:
    async for event in stream:
        print(event.event, event.json())
```

### Streaming HTTP

```python
from datacurrent import HTTPStream

async with HTTPStream("https://example.com/feed.ndjson") as response:
    async for line in response.iter_lines():
        handle(line)
```

### Fan-out and events

```python
from datacurrent import Channel, EventBus

channel = Channel(buffer_size=64)
first = channel.subscribe()
second = channel.subscribe()
await channel.publish("deploy finished")

bus = EventBus()


@bus.subscribe("user.*")
async def on_user(event):
    print(event.topic, event.data)


await bus.publish("user.created", {"user_id": 123})
```

## Retries

```python
from datacurrent import RetryPolicy

stream = Stream.from_factory(connect_and_read).retry(
    RetryPolicy(max_attempts=3, base_delay=0.2, jitter=0.1)
)
```

Retries require a replayable source, such as a factory or a list. A consumed
generator raises `NonReplayableStreamError` instead of silently restarting.

## Examples

Sixteen runnable scripts live in [`examples/`](https://github.com/CoderProandTester/datacurrent/tree/main/examples).
Each one is self-contained and is executed by the test suite, so the output you
see is the output the project verifies.

```bash
python examples/01_hello_stream.py
python -m pytest tests/unit/test_examples.py -q  # run them all
```

| Script | What it demonstrates |
|---|---|
| `01_hello_stream.py` | Creating a stream, iterating it, `tap` and terminal operations |
| `02_transformations.py` | `filter`, `map`, `distinct`, `batch`, `window`, `reduce`, `enumerate` |
| `03_large_dataset.py` | 100,000 records streamed in batches with bounded memory |
| `04_filter_events.py` | `EventBus` wildcard topics and multiple handlers |
| `05_multi_subscriber.py` | `Channel` fan-out to three independent consumers |
| `06_websocket_client.py` | `WebSocketStream` JSON send/receive against a local echo server |
| `07_websocket_server.py` | Broadcasting a `Channel` to connected WebSocket clients |
| `08_sse_client.py` | `SSEStream` event ids, event types and JSON payloads |
| `09_http_streaming.py` | `HTTPStream` line iteration and raw body streaming |
| `10_fastapi_integration.py` | Serving a DataCurrent stream as a FastAPI SSE endpoint |
| `11_backpressure.py` | Bounded queues, producer suspension and counted drops |
| `12_concurrent_processing.py` | Bounded concurrency with order preserved |
| `13_retry_recovery.py` | `RetryPolicy`, `recover` and replayable sources |
| `14_custom_transport.py` | Implementing `StreamTransport` for a custom protocol |
| `15_realtime_notifications.py` | Routing one event to WebSocket and email consumers |
| `16_event_pipeline.py` | End-to-end event filtering, mapping and batching |

`examples/_servers.py` holds the local HTTP/SSE test servers the networking
examples use; it is not an example itself.

## Why DataCurrent

- One model for iterables, queues, HTTP, SSE and WebSockets.
- Backpressure by default, with explicit and counted drop policies.
- Bounded concurrency and cancellation-safe cleanup.
- No infrastructure: no broker, database or external account.
- Explicit delivery semantics; no false durability claims.
- Typed API and sync or async callbacks throughout.

HTTP clients such as `httpx` and `aiohttp` handle requests; DataCurrent adds the
stream engine around them. Reactive libraries provide operator algebra, while
broker frameworks require Kafka, Redis or RabbitMQ. DataCurrent is the
in-process streaming layer that works without any of them.

## Compatibility and limits

- Python 3.11+ on asyncio; no Trio or AnyIO backend.
- In-memory `Channel` and `EventBus` delivery is at most once.
- No durable broker adapters or OpenTelemetry exporter in this release.
- The project name is provisional pending trademark clearance.

## License

MIT.
