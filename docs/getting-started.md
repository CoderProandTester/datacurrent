# Getting started

DataCurrent is an asyncio streaming library. This document covers installation,
the first stream, and the concepts you need before building pipelines.

## Requirements

- Python 3.11 or newer, running on asyncio.
- `httpx` and `websockets` are installed automatically; they power HTTP
  streaming, SSE and WebSocket clients.
- FastAPI and Starlette are optional and only needed for their integrations.

## Install

```bash
pip install datacurrent
```

Verify:

```python
import asyncio
import datacurrent
from datacurrent import Stream

print(datacurrent.__version__)  # 1.1.4
print(asyncio.run(Stream.from_iterable([1, 2]).collect()))  # [1, 2]
```

Framework helpers:

```bash
pip install "datacurrent[fastapi]"
pip install "datacurrent[starlette]"
pip install "datacurrent[all]"
```

If an integration is used without its framework, the library raises
`OptionalDependencyError` with the install command.

## First stream

```python
import asyncio
from datacurrent import Stream


async def main() -> None:
    stream = Stream.from_iterable([1, 2, 3, 4, 5]).map(lambda n: n * 2)

    async for value in stream:
        print(value)


asyncio.run(main())
```

Nothing runs until iteration. Operators are lazy and chainable:

```python
result = await (
    Stream.from_iterable(range(20))
    .filter(lambda n: n % 2 == 0)
    .map(lambda n: n * n)
    .take(4)
    .collect()
)
```

## Terminal operations

```python
await Stream.from_iterable([1, 2]).collect()  # [1, 2]
await Stream.from_iterable([1, 2]).count()  # 2
await Stream.from_iterable([1, 2]).drain()  # 2 (discarded)
await Stream.from_iterable([1, 2]).first()  # 1
await Stream.from_iterable([1, 2]).last()  # 2
await Stream.empty().first("fallback")  # "fallback"
await Stream.from_iterable([3, 1]).reduce(max)  # 3
```

## Async callbacks

Every callback may be a normal function or a coroutine function:

```python
async def enrich(order: dict) -> dict:
    await asyncio.sleep(0)
    return {**order, "enriched": True}


stream = Stream.from_iterable(orders).map(enrich, concurrency=8)
```

With `concurrency > 1`, at most that many callbacks run at once. Order is
preserved unless you pass `ordered=False`.

## Core concepts

### Laziness and single consumption

A `Stream` is single-consumer. Iterating it twice raises `StreamConsumedError`:

```python
stream = Stream.from_iterable([1, 2, 3])
await stream.collect()
await stream.collect()  # StreamConsumedError
```

For fan-out, use `Channel` (every subscriber receives every message) or
`EventBus` (topic-routed handlers and streams). Neither provides history for
subscribers that join late.

### Replayable sources

Retries need a source that can restart:

| Source | Replayable |
|---|---|
| `Stream.from_iterable(list)` | yes |
| `Stream.from_iterable(generator)` | no |
| `Stream.from_async_iterable(agen)` | no |
| `Stream.from_factory(fn)` | yes |
| `Stream.from_queue(queue)` | no |
| `Stream.from_value` / `Stream.empty` | yes |

`retry()` raises `NonReplayableStreamError` instead of resuming a consumed
iterator halfway through.

### Backpressure by default

Buffered pipelines block the producer when the buffer is full. Data is never
silently dropped with the default configuration:

```python
stream = Stream.from_iterable(range(100_000)).buffer(size=64)  # BLOCK
```

Lossy behaviour is explicit and counted:

```python
stream = Stream.from_iterable(range(10_000)).buffer(size=16, overflow="drop_oldest")
items = await stream.collect()
print(len(items), stream.stats["dropped"])
```

### Errors

Everything raised deliberately by DataCurrent derives from `DataCurrentError`:

- `ConfigurationError` — invalid arguments (also a `ValueError`).
- `StreamConsumedError` — second iteration.
- `NonReplayableStreamError` — replay required but impossible.
- `EmptyStreamError` — terminal operation needed an item.
- `StreamTimeoutError` — timeout expired (also a `TimeoutError`).
- `BufferOverflowError` — full buffer with `overflow="error"`.
- `TransportError` — connection/protocol failures.

### One event loop

Core primitives are asyncio-native, must be used from one event loop, and are not
thread safe. There is no Trio or AnyIO backend.

## Delivery semantics

| Component | Ordering | Delivery | Durability |
|---|---|---|---|
| `Stream` | source order (except merge/unordered map) | items you pull | none |
| `BoundedQueue` | FIFO | all unless a drop policy is configured | none |
| `Channel` | per-subscriber FIFO | at most once; no history | none |
| `EventBus` | per-subscriber FIFO | at most once; in-process | none |
| `SSEStream` | wire order | resumable via `Last-Event-ID`; server-defined | server-defined |
| `HTTPStream` | byte/line order | whatever the server sends | none |
| `WebSocketStream` | connection order | unacknowledged; reconnects are caller-visible | none |

No in-memory component claims durable delivery.

## Next reading

- `docs/streaming-guide.md` — streams, operators, backpressure, concurrency
- `docs/real-time-transports.md` — WebSocket, SSE, HTTP, events, integrations
- `docs/operations.md` — reliability, security, observability, troubleshooting
- `docs/api-reference.md` — public API and architecture
