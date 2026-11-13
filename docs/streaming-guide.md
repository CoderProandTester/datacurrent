# Streaming guide

Everything about creating streams, transforming data, applying backpressure and
running concurrent work.

## Creating streams

```python
Stream.from_iterable([1, 2, 3])  # list, replayable
Stream.from_iterable(range(1_000_000))  # lazy
Stream.from_async_iterable(agen)  # async generator, one-shot
Stream.from_factory(make_source)  # replayable; called per attempt
Stream.from_value("x")  # one item
Stream.empty()  # no items
Stream.from_queue(BoundedQueue(16))  # live queue, one consumer
Stream(range(5))  # constructor form
Stream(other_stream)  # drains another stream
```

Configure a stream:

```python
from datacurrent import Stream, StreamConfig

stream = Stream.from_iterable(
    [1, 2, 3],
    config=StreamConfig(
        name="orders",
        buffer_size=256,
        overflow="block",
        concurrency=4,
        observe=True,
    ),
)
```

Invalid sizes, policies or names raise `ConfigurationError`.

## Iteration and cleanup

```python
async with stream as items:
    async for item in items:
        if finished(item):
            break
# upstream iterators, tasks and connections are closed here
```

If you use a bare `async for`, call `await stream.aclose()` on early exit to make
cleanup immediate instead of waiting for garbage collection. Terminal methods and
`take(n)` close the chain automatically.

`StreamIterator` exposes `aclose()`, `closed`, `exhausted` and supports
`async with`.

## Operators

### Transform

- `map(func, *, concurrency=None, ordered=True)` — async or sync callback.
  Ordered mode preserves input order; `ordered=False` yields completion order.
  Errors cancel remaining callbacks.
- `filter(predicate)` — truthy keeps the item.
- `flat_map(func)` — callback returns an iterable, async iterable or `Stream`;
  `None` skips. Strings/bytes are rejected.
- `tap(func)` — side effect, original item passes through.

### Shape

- `take(n)` — closes upstream after `n`; `take(0)` never opens it.
- `skip(n)` — discard the first `n`.
- `batch(size, partial=True)` — lists of `size`; final short batch optional.
- `window(size, step=1)` — full sliding windows only.
- `distinct(key=None, capacity=None)` — deduplicate; `capacity` bounds memory
  with FIFO eviction.
- `enumerate(start=0)` — `(index, item)` pairs.

### Timing

- `debounce(seconds)` — emit after a quiet period; flushes the last item.
- `throttle(interval)` — at most one item per interval (leading edge); extras are
  consumed and dropped.

### Combine

- `concat(*others)` — sequential.
- `merge(*others, buffer_size=None, overflow=None)` — concurrent interleaving as
  items arrive.
- `zip(*others)` — one item from each stream; stops at the shortest. Items
  already pulled from longer streams are discarded.

### Reliability

- `timeout(seconds)` — per-item deadline; raises `StreamTimeoutError`.
- `retry(policy=None, *, on=None)` — restarts a replayable source with backoff.
  `RetryPolicy` fields: `max_attempts`, `base_delay`, `max_delay`, `multiplier`,
  `jitter`, `retry_on`. `on` accepts an exception class, tuple or predicate.
- `recover(handler)` — `handler(exception)` returns replacement items or `None`.

### Terminals

`collect`, `count`, `drain`, `first(default=...)`, `last(default=...)`,
`reduce(func, initial=...)`.

Bad arguments (`batch(0)`, `map(x, concurrency=0)`, `window(3, step=0)`,
`debounce(0)`) raise before iteration starts.

## Backpressure

```python
from datacurrent import BoundedQueue

queue = BoundedQueue(32)  # policy defaults to BLOCK
await queue.put(item)  # waits while full
item = await queue.get()  # waits while empty
await queue.aclose()  # buffered items can still be drained
queue.stats().as_dict()
# size, maxsize, enqueued, dequeued, dropped, overflow, high_watermark, closed
```

Policies:

| Policy | Behaviour |
|---|---|
| `block` | producer waits (default, no loss) |
| `drop_newest` | discard the incoming item |
| `drop_oldest` | discard the oldest buffered item |
| `error` | raise `BufferOverflowError` |

Buffered streams:

```python
stream = Stream.from_iterable(range(100_000)).buffer(size=64)
```

One producer task runs between upstream and consumer. When the queue is full the
producer suspends, so memory is bounded and upstream iteration pauses. Closing
the stream cancels and awaits the producer, then closes upstream.

```python
stream = Stream.from_iterable(range(10_000)).buffer(size=16, overflow="drop_oldest")
received = await stream.collect()
assert len(received) + stream.stats["dropped"] == 10_000
```

Sizing guidance: a few hundred items for network bursts, small buffers for large
payloads, `drop_oldest` for live UI/telemetry where freshness wins, `block` or
`error` for strict pipelines. Size from measured bursts, not guesses.

## Concurrency

```python
stream = Stream.from_iterable(urls).map(fetch, concurrency=16)
```

- `concurrency` bounds in-flight callbacks.
- Errors cancel siblings and propagate.
- Cancelling the consumer cancels and awaits callbacks.

Outside a pipeline:

```python
from datacurrent import ConcurrencyLimiter, TaskTracker, consume

count = await consume(stream, write_row, concurrency=8)

limiter = ConcurrencyLimiter(4)
async with limiter:
    await do_work()
limiter.active, limiter.peak, limiter.waiting

async with TaskTracker(name="workers") as tracker:
    tracker.spawn(worker())
# all tracked tasks cancelled and awaited
```

Rules: never create unbounded tasks, always cancel and await, never swallow
`CancelledError`, close resources in `finally`, keep everything in one loop.

## Verification

`tests/unit/test_operators_*.py`, `test_backpressure.py`, `test_stream_basics.py`
and `tests/stress/test_stress.py` cover ordering, async callbacks, validation,
policy behaviour, blocked-producer wakeups, cancellation cleanup and bounded
memory under load. Property tests compare operators against plain Python
semantics for randomized input.
