# API reference

Public API of DataCurrent 1.1.2. Everything listed here is implemented and tested.

## Architecture at a glance

- A `Stream` holds a factory that returns a fresh async iterator.
- Operators return new `Stream`s whose factories wrap the upstream iterator.
- Opening a stream consumes it once; every generator closes its upstream in a
  `finally` block.
- Background tasks (buffers, merge pumps, bus workers) are owned and cancelled.
- Networking libraries are imported lazily inside transport methods.

## Core

### `Stream`

Construction:

```python
Stream(source, *, config=None, replayable=None)
Stream.from_iterable(items, *, config=None)
Stream.from_async_iterable(items, *, config=None)
Stream.from_factory(factory, *, config=None)
Stream.from_value(value, *, config=None)
Stream.empty(*, config=None)
Stream.from_queue(queue, *, config=None)
```

Properties: `config`, `replayable`, `consumed`, `stats`.

Terminals: `collect()`, `count()`, `drain()`, `first(default=...)`,
`last(default=...)`, `reduce(func, initial=...)`.

Operators: `map`, `filter`, `flat_map`, `take`, `skip`, `batch`, `buffer`,
`distinct`, `enumerate`, `window`, `debounce`, `throttle`, `timeout`, `retry`,
`recover`, `tap`, `zip`, `merge`, `concat`.

Lifecycle: `async for`, `async with stream as iterator`, `aclose()`.

### `StreamIterator`

`__anext__`, `__aiter__`, `aclose()`, `async with`, `closed`, `exhausted`.

### `StreamConfig`

Fields: `name` (`str | None`), `buffer_size` (`int`, default 1024), `overflow`
(`OverflowPolicy`, default `block`), `concurrency` (`int`, default 1), `observe`
(`bool`, default `False`).

### `OverflowPolicy`

`BLOCK`, `DROP_NEWEST`, `DROP_OLDEST`, `ERROR`; string values `block`,
`drop_newest`, `drop_oldest`, `error`. `OverflowPolicy.coerce(value)` accepts an
enum or string and raises `ConfigurationError` otherwise.

### Helpers

- `pipeline(source=None, *, config=None)`
- `merge_sources(*sources, buffer_size=None, config=None)`
- `concat_sources(*sources, config=None)`
- `consume(stream, handler, *, concurrency=1, ordered=True) -> int`
- `drain_into(stream, sink, *, concurrency=1) -> int`

## Buffering

### `BoundedQueue(maxsize=1024, *, overflow="block", name=None, on_drop=None)`

- `put(item) -> bool`, `put_many(items) -> int`
- `get()`, `get_nowait()`
- `aclose()`, `fail(error)`
- `qsize()`, `empty()`, `full()`, `stats() -> BufferStats`
- properties: `maxsize`, `overflow`, `closed`, `error`, `name`
- async iteration until closed; queued errors are raised after draining

### `BufferStats`

`size`, `maxsize`, `enqueued`, `dequeued`, `dropped`, `overflow`,
`high_watermark`, `closed`; `as_dict()`.

### `BufferPolicy(size=1024, overflow="block")`

`build(name=None, on_drop=None) -> BoundedQueue`,
`BufferPolicy.from_config(config, size=None, overflow=None)`.

## Concurrency

- `ConcurrencyLimiter(limit)` — `acquire`, `release`, `run(func, *args)`,
  `async with`, `limit`, `active`, `waiting`, `peak`.
- `TaskTracker(*, name="datacurrent")` — `spawn(coro, name=None)`, `active`,
  `tasks`, `closed`, `aclose(timeout=5.0)`, `async with`.
- `gather_limited(limit, awaitables, *, return_exceptions=False)`.

## Reliability

### `RetryPolicy`

Fields: `max_attempts=3`, `base_delay=0.1`, `max_delay=10.0`, `multiplier=2.0`,
`jitter=0.1`, `retry_on=Exception`.

Methods: `delay_for(attempt, *, rng=None)`, `should_retry(error)`,
`RetryPolicy.coerce(value)`.

- `retry_async(func, policy=None, *, on=None)`
- `with_timeout(awaitable, seconds, *, what="operation")`
- `validate_timeout(seconds)`
- `is_retryable(predicate, error)`

## Events

### `Event(topic, data, id=..., timestamp=..., metadata={}, source=None)`

Immutable; `metadata` is a read-only mapping; `to_dict()`.

### `EventBus(*, buffer_size=1024, overflow="block", on_error=None, name=None)`

- `subscribe(topic="*", handler=None, *, buffer_size=None, overflow=None)` —
  returns a `Subscription`, or a decorator when `handler` is omitted
- `stream(topic="*", *, buffer_size=None, overflow=None, name=None) -> Stream`
- `publish(topic, data=None, *, metadata=None, event_id=None, source=None) -> int`
- `publish_event(event) -> int`
- `unsubscribe(subscription)`
- `aclose(*, drain=False, timeout=5.0)`, `async with`
- `subscriber_count`, `subscriptions`, `stats()`

### `Subscription`

`topic`, `queue`, `handler`, `active`, `delivered`, `failed`, `in_flight`,
`matches(topic)`, `stats()`, `aclose()`.

Routing rules: exact segments, `*` matches one segment, trailing `**` or `#`
matches the rest, `*` alone matches everything.

## Transports

### `Channel(*, buffer_size=256, overflow="block", name=None)`

- `subscribe(*, buffer_size=None, overflow=None, name=None) -> Stream`
- `publish(item) -> int`, `publish_many(items) -> int`
- `aclose()`, `async with`, `subscriber_count`, `closed`, `stats()`

### `SSEEvent(data="", event="message", id=None, retry=None)`

`json()` parses `data`.

### `parse_sse(lines)`, `format_sse(event)`, `format_heartbeat(comment)`

`SSELineDecoder` and `SSEParser` are available for incremental parsing.

### `SSEStream(url, *, headers=None, params=None, method="GET", timeout=30.0, client=None, max_reconnects=0, reconnect_delay=1.0, max_reconnect_delay=30.0, last_event_id=None, check_content_type=True)`

`SSEStream.connect(url, **kwargs)`, async iteration over `SSEEvent`,
`last_event_id`, `closed`, `aclose()`, `async with`.

### `HTTPStream(url, *, method="GET", headers=None, params=None, json=None, data=None, content=None, timeout=30.0, chunk_size=65536, client=None, max_bytes=None, raise_for_status=True, follow_redirects=True)`

`HTTPStream.get(url, **kwargs)`, `iter_bytes(chunk_size=None)`, `iter_text()`,
`iter_lines()`, `read_all(max_bytes=None)`, `as_stream(config=None)`,
`status_code`, `response`, `aclose()`, `async with`.

### `WebSocketStream(url, *, headers=None, mode="auto", max_size=8 MiB, ping_interval=20.0, ping_timeout=20.0, open_timeout=10.0, close_timeout=5.0, reconnect_attempts=0, reconnect_delay=0.5, max_reconnect_delay=30.0, subprotocols=None, ssl=None, origin=None)`

`WebSocketStream.connect(url, **kwargs)`, `send`, `send_json`, `ping`, `recv`,
async iteration, `connected`, `protocol`, `aclose()`, `async with`.

### `StreamTransport`

Abstract base with `open()`, `close()`, `messages()`, `async with`,
`as_stream(**kwargs)` and `is_open`.

## Integrations

`datacurrent.integrations.fastapi`:

- `sse_response(events, *, headers=None, status_code=200)`
- `websocket_messages(websocket)`
- `websocket_stream(websocket, *, config=None)`

`datacurrent.integrations.starlette`:

- `streaming_response(stream, *, media_type=..., headers=None, status_code=200)`
- `StreamLifespan(*resources)` with `aclose()`, `async with`,
  `as_context_manager()`

## Observability

- `Metrics(prefix="datacurrent")` — `inc(name, amount=1, **tags)`, `set`,
  `get`, `timer(name, **tags)`, `observe_duration`, `snapshot()`, `reset()`.
- `Counter` — `inc(amount=1)`, `value`, `reset()`.
- `StreamStats` — `name`, `items_out`, `errors`, `retries`, `dropped`,
  `snapshot()`.
- Logging — `get_logger(name=None)`, `redact_url(url)`,
  `redact_headers(headers)`, `redact_secret(value)`.

## Constants

`DEFAULT_BUFFER_SIZE = 1024`, `DEFAULT_CONCURRENCY = 1`,
`DEFAULT_MAX_MESSAGE_SIZE = 8 MiB`, `DEFAULT_TIMEOUT = 30.0`.

## Exceptions

```
DataCurrentError
├── ConfigurationError (ValueError)
├── OptionalDependencyError (ImportError)
├── StreamError
│   ├── StreamConsumedError
│   ├── NonReplayableStreamError
│   ├── EmptyStreamError
│   ├── StreamTimeoutError (TimeoutError)
│   ├── StreamClosedError
│   └── BufferOverflowError (BufferError)
├── QueueClosedError
├── QueueEmptyError
├── TransportError
│   ├── ConnectionFailedError
│   ├── StreamProtocolError
│   │   └── SSEProtocolError
│   ├── MessageTooLargeError
│   └── WebSocketClosedError
├── RetryError
├── EventBusError
│   └── SubscriptionClosedError
└── (BufferOverflowError is also a StreamError, as shown above)
```

## Version

`datacurrent.__version__ == "1.1.2"`; `datacurrent.VERSION == (1, 1, 2)`.
