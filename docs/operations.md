# Operations guide

Reliability, observability, security, performance and troubleshooting for
applications running DataCurrent.

## Reliability

### Retry policies

```python
from datacurrent import RetryPolicy

policy = RetryPolicy(
    max_attempts=3,  # includes the first attempt
    base_delay=0.2,
    max_delay=10.0,
    multiplier=2.0,
    jitter=0.1,  # fraction of the delay, 0 disables
    retry_on=ConnectionError,  # class, tuple or predicate
)

stream = Stream.from_factory(make_source).retry(policy)
```

`retry_async(func, policy)` applies the same policy to a coroutine function.
Permanent errors stop immediately when a predicate excludes them. Attempts are
bounded; DataCurrent never reconnects forever.

### Replayability

Retries restart the source, so the source must be replayable:

- `Stream.from_factory`, `Stream.from_iterable(list)`,
  `Stream.from_value`, `Stream.empty` — replayable.
- bare generators, async generators, queues — not replayable.

A failed retry on a non-replayable source raises `NonReplayableStreamError`
rather than resuming mid-stream.

### Recovery and timeouts

```python
recovered = Stream.from_async_iterable(source()).recover(lambda error: ["fallback"])

stream = Stream.from_iterable(source()).timeout(5.0)
```

`timeout()` is per item. Connection timeouts are configured on the network
clients (`timeout=` on `SSEStream` and `HTTPStream`, `open_timeout` on
`WebSocketStream`).

### Shutdown

- `EventBus.aclose(drain=True)` waits for queued events and in-flight handlers.
- `Channel.aclose()` closes subscriber queues.
- Closing a stream cancels its background producer/pumps and awaits them.
- `TaskTracker.aclose()` cancels application background tasks.

## Observability

```python
from datacurrent import Metrics, StreamConfig

stream = Stream.from_iterable(range(100), config=StreamConfig(observe=True, name="jobs"))
await stream.count()
stream.stats  # items_out, errors, retries, dropped

metrics = Metrics()
metrics.inc("orders.processed", 1, queue="primary")
with metrics.timer("orders.batch"):
    process()
metrics.snapshot()
```

Available counters:

- `stream.stats` — items out, errors, retries, drops for one stream.
- `queue.stats()` — depth, high watermark, enqueued, dequeued, dropped,
  overflows, closed.
- `channel.stats()` / `bus.stats()` — published, delivered, dropped, failed,
  subscribers.
- `Metrics` — arbitrary counters/gauges keyed by name and tags.

Logging uses the `datacurrent` namespace with a `NullHandler` by default, so
importing the library never changes application logging. URLs, query strings,
authorization/cookie headers and secrets are redacted before logging.

There is no hosted dashboard and no OpenTelemetry exporter in this release; feed
the snapshots into your existing system.

## Security

Threat model summary:

- **Credential leakage:** URLs (user info and sensitive query keys) and headers
  (`Authorization`, cookies, API keys) are redacted. Error messages use redacted
  URLs.
- **TLS:** `https://` and `wss://` verify certificates by default. DataCurrent
  never disables verification; only an explicit `ssl=` context supplied by the
  application can.
- **Resource exhaustion:** bounded buffers, message-size limits, body limits,
  bounded reconnection and timeouts.
- **Leaks on cancellation:** responses, sockets, iterators and tasks are closed
  in `finally`; tests assert no `datacurrent` tasks remain after cancellation.
- **Unsafe URLs:** only `http(s)://` and `ws(s)://` are accepted.
- **Handler isolation:** one failing event handler cannot break other
  subscribers.

Out of scope: server-side auth (this is not a web framework), durable delivery,
payload validation, compromised event loop or trust store.

Report vulnerabilities privately to the maintainer. Do not open public issues
for security reports.

## Performance

Measured on CPython 3.15.2, Windows, median of three runs with
`python benchmarks/bench_core.py`:

| Benchmark | Items | Time | Throughput |
|---|---:|---:|---:|
| `map + filter + count` | 50,000 | 0.095 s | ~526,000 items/s |
| `batch(1000)` | 100,000 | 0.012 s | ~8,600,000 items/s |
| `map(concurrency=10)` | 20,000 | 0.046 s | ~432,000 items/s |
| `buffer(64)` | 20,000 | 0.033 s | ~612,000 items/s |
| `BoundedQueue` put/get | 100,000 | 0.102 s | ~982,000 items/s |
| Event bus publish + deliver | 20,000 | 0.106 s | ~188,000 events/s |
| Channel fan-out (×3) | 60,000 | 0.080 s | ~752,000 deliveries/s |

Peak allocation for a 200,000-item `map + filter + count` pipeline is about
6.4 KB (tracemalloc) because items are streamed, not collected.

Methodology: `benchmarks/bench_core.py`, three timed runs per scenario using the
median, CPython 3.15.2 on Windows. Results are machine-specific. **No comparison
against other libraries has been measured, so DataCurrent makes no comparative
speed claim.**

## Delivery guarantees

| Component | Ordering | Delivery | Durability |
|---|---|---|---|
| `Stream` | source order (except merge/unordered map) | exactly what you pull | none |
| `BoundedQueue` | FIFO | all unless a drop policy is configured | none |
| `Channel` | per-subscriber FIFO | at most once, no history | none |
| `EventBus` | per-subscriber FIFO | at most once, in-process | none |
| `SSEStream` | wire order | server-defined, resumable with `Last-Event-ID` | server-defined |
| `HTTPStream` | byte/line order | server-defined | none |
| `WebSocketStream` | connection order | unacknowledged, reconnects surfaced | none |

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `StreamConsumedError` | The stream was already iterated. Build a new stream or use `Channel`/`EventBus` for fan-out. |
| `NonReplayableStreamError` | Retry needs a restartable source. Wrap it with `Stream.from_factory`. |
| `BufferOverflowError` | Buffer full with `overflow="error"`. Increase `size`, slow the producer, or choose another policy. |
| `StreamTimeoutError` | No item within `timeout()`. Check the source, or raise the timeout. |
| `SSEProtocolError` | Response was not `text/event-stream`, or the URL returned HTML/JSON. Check the endpoint and `check_content_type`. |
| `ConnectionFailedError` | SSE connection failed after configured retries. Check DNS/TLS/proxy and `max_reconnects`. |
| `WebSocketClosedError` | Abnormal close. Inspect the code/reason, add `reconnect_attempts`, or reconnect explicitly. |
| `OptionalDependencyError` | Framework integration used without its framework. Install `datacurrent[fastapi]` or `datacurrent[starlette]`. |
| Hanging iteration | A bare `async for` with a source that never closes, or a queue that was never closed. Use `async with` and `queue.aclose()`. |
| Memory growth | Unbounded application-side storage, or a source producing faster than a `collect()`. Use `buffer(size=...)` and process incrementally. |
| Tasks left running on shutdown | Cancel the consuming task and close streams/trackers; don't create untracked tasks. |

## Compatibility

- Python 3.11+ with asyncio.
- No Trio/AnyIO backend.
- Windows, macOS and Linux (path-agnostic, no native extensions).
- Runtime dependencies: `httpx`, `websockets`. Optional: FastAPI, Starlette.
