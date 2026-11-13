from __future__ import annotations

import asyncio
import importlib
import logging

import pytest

from datacurrent import (
    ConfigurationError,
    EmptyStreamError,
    NonReplayableStreamError,
    OptionalDependencyError,
    RetryError,
    Stream,
    StreamClosedError,
    StreamConsumedError,
    TaskTracker,
    WebSocketClosedError,
)
from datacurrent.concurrency.limits import ConcurrencyLimiter
from datacurrent.config import OverflowPolicy, StreamConfig
from datacurrent.core._util import as_aiterator
from datacurrent.core.lifecycle import aclose, aclose_many, cancel_task, cancel_tasks
from datacurrent.core.pipeline import concat_sources, merge_sources
from datacurrent.core.sink import consume, drain_into
from datacurrent.core.stream import StreamIterator
from datacurrent.reliability.retry import (
    RetryPolicy,
    is_retryable,
    retry_async,
)
from datacurrent.reliability.timeout import validate_timeout


# --------------------------------------------------------------------------- #
# Configuration / exceptions
# --------------------------------------------------------------------------- #
def test_config_validation_edges() -> None:
    with pytest.raises(ConfigurationError):
        StreamConfig(buffer_size="8")  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError):
        StreamConfig(name=123)  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError):
        OverflowPolicy.coerce("sometimes")
    with pytest.raises(ConfigurationError):
        OverflowPolicy.coerce(1)
    assert OverflowPolicy.coerce("DROP-OLDEST") is OverflowPolicy.DROP_OLDEST


def test_exception_construction_edges() -> None:
    with_extra = OptionalDependencyError("fastapi", extra="fastapi", purpose="SSE")
    assert with_extra.package == "fastapi"
    assert with_extra.extra == "fastapi"
    assert "datacurrent[fastapi]" in str(with_extra)

    plain = OptionalDependencyError("httpx")
    assert "pip install httpx" in str(plain)

    assert "code=1006" in str(WebSocketClosedError(1006))
    assert "reason=bye" in str(WebSocketClosedError(None, "bye"))
    assert str(WebSocketClosedError()) == "websocket connection closed"

    assert RetryError(2).attempts == 2
    original = ValueError("last")
    error = RetryError(3, original)
    assert error.last_error is original
    assert "last" in str(error)


# --------------------------------------------------------------------------- #
# Retry / timeout
# --------------------------------------------------------------------------- #
def test_retry_predicate_and_policy_validation() -> None:
    assert is_retryable(None, ValueError())
    assert is_retryable((ValueError, KeyError), ValueError())
    assert is_retryable(lambda error: isinstance(error, ValueError), ValueError())
    with pytest.raises(ConfigurationError):
        is_retryable("invalid", ValueError())  # type: ignore[arg-type]

    with pytest.raises(ConfigurationError):
        RetryPolicy(max_attempts=True)  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError):
        RetryPolicy(base_delay="1")  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError):
        RetryPolicy(base_delay=-0.1)
    with pytest.raises(ConfigurationError):
        RetryPolicy.coerce(True)

    policy = RetryPolicy(max_attempts=2, retry_on=ValueError)
    assert policy.should_retry(ValueError())
    with pytest.raises(ConfigurationError):
        policy.delay_for(0)


async def test_retry_async_retries_and_propagates_cancellation() -> None:
    attempts = 0

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise ValueError("not yet")
        return "ok"

    policy = RetryPolicy(max_attempts=2, base_delay=0.001, jitter=0)
    assert await retry_async(flaky, policy) == "ok"
    assert attempts == 2

    async def cancelled() -> None:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await retry_async(cancelled)


def test_timeout_validation_non_number() -> None:
    with pytest.raises(ConfigurationError):
        validate_timeout("soon")  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# Stream internals
# --------------------------------------------------------------------------- #
class _BareIterator:
    def __aiter__(self) -> _BareIterator:
        return self

    async def __anext__(self) -> None:
        raise StopAsyncIteration


async def test_stream_iterator_lifecycle_edges() -> None:
    iterator = StreamIterator(_BareIterator())
    assert iterator.closed is False
    assert iterator.exhausted is False
    with pytest.raises(StopAsyncIteration):
        await iterator.__anext__()
    assert iterator.closed is True
    assert iterator.exhausted is True
    with pytest.raises(StopAsyncIteration):
        await iterator.__anext__()
    await iterator.aclose()

    bare = StreamIterator(_BareIterator())
    await bare.aclose()


async def test_stream_construction_and_open_raw_edges() -> None:
    assert await Stream.from_value("one").collect() == ["one"]
    with pytest.raises(TypeError):
        Stream.from_async_iterable([1])  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Stream.from_factory(1)  # type: ignore[arg-type]

    stream = Stream.from_iterable([1, 2])
    assert stream.consumed is False
    await stream.collect()
    assert stream.consumed is True
    with pytest.raises(StreamConsumedError):
        stream.__aiter__()

    one_shot = Stream(iter([1]))
    await one_shot.collect()
    with pytest.raises(NonReplayableStreamError):
        one_shot._open_raw()

    fresh = Stream.empty()
    await fresh.aclose()
    with pytest.raises(TypeError):
        await Stream.from_iterable([1]).reduce(1)  # type: ignore[arg-type]
    with pytest.raises(EmptyStreamError):
        await Stream.empty().first()


async def test_stream_context_manager_and_stats() -> None:
    stream = Stream.from_iterable([1, 2, 3])
    async with stream as iterator:
        assert iterator.closed is False
        assert await iterator.__anext__() == 1
    assert iterator.closed is True

    iterator = Stream.from_iterable([1]).__aiter__()
    async with iterator as entered:
        assert entered is iterator
        assert await entered.__anext__() == 1
    assert iterator.closed is True

    one_shot = Stream(iter([1]))
    first = one_shot._factory()
    second = one_shot._factory()
    assert first is second
    await first.aclose()


# --------------------------------------------------------------------------- #
# Operators / sinks / pipelines
# --------------------------------------------------------------------------- #
async def test_operator_validation_edges() -> None:
    with pytest.raises(ConfigurationError):
        Stream.from_iterable([1]).take("1")  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError):
        Stream.from_iterable([1]).map(lambda item: item, concurrency="2")  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError):
        Stream.from_iterable([1]).map(lambda item: item, ordered="yes")  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError):
        Stream.from_iterable([1]).batch(2, partial="yes")  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError):
        Stream.from_iterable([1]).concat()
    assert await Stream.from_iterable([1]).map(lambda item: item + 1, concurrency=4).collect() == [
        2
    ]


async def test_operator_cancellation_edges() -> None:
    async def endless() -> object:
        index = 0
        while True:
            await asyncio.sleep(0.01)
            yield index
            index += 1

    merged = Stream.from_async_iterable(endless()).merge(Stream.from_iterable([1]))
    iterator = merged.__aiter__()
    await iterator.__anext__()
    await iterator.aclose()

    async def slow() -> object:
        for index in range(3):
            await asyncio.sleep(5)
            yield index

    debounced = Stream.from_async_iterable(slow()).debounce(1)
    debounce_iterator = debounced.__aiter__()
    pending = asyncio.create_task(debounce_iterator.__anext__())
    await asyncio.sleep(0.01)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    await debounce_iterator.aclose()

    async def cancelled_source() -> object:
        if False:  # pragma: no cover - keeps this an async generator
            yield None
        raise asyncio.CancelledError

    recovered = Stream.from_async_iterable(cancelled_source()).recover(lambda error: [1])
    with pytest.raises(asyncio.CancelledError):
        await recovered.collect()


async def test_sink_paths() -> None:
    with pytest.raises(ConfigurationError):
        await consume([], lambda item: item)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        await consume(Stream.empty(), 1)  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError):
        await consume(Stream.empty(), lambda item: item, concurrency="1")  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError):
        await consume(Stream.empty(), lambda item: item, concurrency=0)

    seen: list[int] = []
    assert await consume(Stream.from_iterable([1, 2]), seen.append) == 2
    assert seen == [1, 2]

    async def async_handler(item: int) -> None:
        await asyncio.sleep(0)
        seen.append(item)

    assert await consume(Stream.from_iterable([3, 4]), async_handler, concurrency=2) == 2
    assert seen == [1, 2, 3, 4]

    with pytest.raises(ConfigurationError):
        await drain_into(Stream.empty(), object())

    class Sink:
        def __init__(self) -> None:
            self.items: list[int] = []

        async def put(self, item: int) -> None:
            self.items.append(item)

    sink = Sink()
    assert await drain_into(Stream.from_iterable([5, 6]), sink) == 2
    assert sink.items == [5, 6]


async def test_pipeline_and_iterator_helpers() -> None:
    assert await merge_sources([1, 2]).collect() == [1, 2]
    assert await concat_sources([3, 4]).collect() == [3, 4]
    with pytest.raises(TypeError):
        as_aiterator(123)


# --------------------------------------------------------------------------- #
# Lifecycle
# --------------------------------------------------------------------------- #
class _SyncClose:
    def __init__(self, error: Exception | None = None) -> None:
        self.closed = False
        self.error = error

    def close(self) -> None:
        self.closed = True
        if self.error is not None:
            raise self.error


class _AsyncClose:
    async def aclose(self) -> None:
        await asyncio.sleep(0)


class _CancelClose:
    async def aclose(self) -> None:
        raise asyncio.CancelledError


async def test_aclose_variants() -> None:
    await aclose(None)
    await aclose(object())
    sync = _SyncClose()
    await aclose(sync)
    assert sync.closed
    await aclose(_AsyncClose())

    with pytest.raises(asyncio.CancelledError):
        await aclose(_CancelClose())

    suppressed = _SyncClose(ValueError("ignored"))
    await aclose(suppressed)
    with pytest.raises(ValueError):
        await aclose(_SyncClose(ValueError("raised")), suppress=False)

    await aclose_many([None, _SyncClose()])


async def test_cancel_task_paths() -> None:
    await cancel_task(None)

    async def fail() -> None:
        raise ValueError("failed")

    failed = asyncio.create_task(fail())
    await asyncio.sleep(0)
    await cancel_task(failed)

    pending = asyncio.create_task(asyncio.sleep(10))
    await cancel_task(pending)
    assert pending.cancelled()

    release = asyncio.Event()

    async def stubborn() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await release.wait()

    slow = asyncio.create_task(stubborn())
    await asyncio.sleep(0)
    await cancel_task(slow, timeout=0.001)
    assert slow.done() is False
    release.set()
    await asyncio.gather(slow, return_exceptions=True)


async def test_cancel_tasks_paths() -> None:
    async def quick() -> int:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            return 1

    release = asyncio.Event()

    async def slow() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await release.wait()

    quick_task = asyncio.create_task(quick())
    slow_task = asyncio.create_task(slow())
    await asyncio.sleep(0)
    await cancel_tasks([quick_task, slow_task], timeout=0.001)
    assert quick_task.done() is True
    assert slow_task.done() is False
    release.set()
    await asyncio.gather(slow_task, return_exceptions=True)


# --------------------------------------------------------------------------- #
# Concurrency / logging
# --------------------------------------------------------------------------- #
async def test_task_tracker_and_limiter_properties() -> None:
    tracker = TaskTracker(name="edge")
    assert tracker.tasks == ()
    assert tracker.closed is False
    assert tracker.active == 0

    async def fail() -> None:
        raise ValueError("background failure")

    tracker.spawn(fail())
    await asyncio.sleep(0.01)
    assert tracker.active == 0

    successful = tracker.spawn(asyncio.sleep(0, result=1))
    assert await successful == 1
    await asyncio.sleep(0)
    assert tracker.active == 0

    await tracker.aclose()
    assert tracker.closed is True
    with pytest.raises(StreamClosedError):
        tracker.spawn(asyncio.sleep(0))

    future = asyncio.get_running_loop().create_future()
    with pytest.raises(StreamClosedError):
        tracker.spawn(future)
    future.cancel()

    limiter = ConcurrencyLimiter(2)
    assert limiter.limit == 2
    assert limiter.waiting == 0
    assert await limiter.run(lambda value: value + 1, 2) == 3

    async def work() -> str:
        return "done"

    assert await limiter.run(work) == "done"
    limiter.release()

    gate = ConcurrencyLimiter(1)
    await gate.acquire()
    started = asyncio.Event()

    async def waiter() -> None:
        started.set()
        await gate.acquire()
        gate.release()

    waiting = asyncio.create_task(waiter())
    await started.wait()
    await asyncio.sleep(0)
    assert gate.waiting == 1
    gate.release()
    await waiting


def test_logging_redaction_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    from datacurrent.observability import logging as data_logging

    data_logging._root_logger.addHandler(logging.NullHandler())
    reloaded = importlib.reload(data_logging)
    assert reloaded.get_logger("edge").name == "datacurrent.edge"

    def broken_parse(*args: object, **kwargs: object) -> list[tuple[str, str]]:
        raise ValueError("malformed query")

    monkeypatch.setattr(reloaded, "parse_qsl", broken_parse)
    assert reloaded.redact_url("http://example.com/?a=1").endswith("?***")
