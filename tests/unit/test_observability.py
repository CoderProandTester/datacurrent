from __future__ import annotations

import time

from datacurrent import (
    BufferStats,
    Counter,
    Metrics,
    StreamStats,
    get_logger,
    redact_headers,
    redact_secret,
    redact_url,
)


def test_counter() -> None:
    counter = Counter()
    assert counter.value == 0
    counter.inc()
    counter.inc(4)
    assert counter.value == 5
    assert int(counter) == 5
    counter.reset()
    assert counter.value == 0


def test_metrics_counters_gauges_and_tags() -> None:
    metrics = Metrics()
    metrics.inc("streams.opened")
    metrics.inc("streams.opened")
    metrics.inc("items", 5, stream="orders")
    assert metrics.get("streams.opened") == 2
    assert metrics.get("items", stream="orders") == 5
    metrics.set("queue.depth", 3, queue="orders")
    snapshot = metrics.snapshot()
    assert snapshot["counters"]["items{stream=orders}"] == 5
    assert snapshot["gauges"]["queue.depth{queue=orders}"] == 3.0
    metrics.reset()
    assert metrics.snapshot()["counters"] == {}


def test_metrics_timer_records_milliseconds() -> None:
    metrics = Metrics()
    with metrics.timer("work", stage="one"):
        time.sleep(0.002)
    value = metrics.snapshot()["gauges"]["work.milliseconds{stage=one}"]
    assert value >= 1.0


def test_stream_stats_snapshot() -> None:
    stats = StreamStats(name="demo")
    stats.items_out += 3
    stats.errors += 1
    stats.retries += 2
    stats.dropped += 4
    assert stats.snapshot() == {
        "name": "demo",
        "items_out": 3,
        "errors": 1,
        "retries": 2,
        "dropped": 4,
    }


def test_buffer_stats_as_dict() -> None:
    stats = BufferStats(
        size=1,
        maxsize=2,
        enqueued=3,
        dequeued=2,
        dropped=1,
        overflow=1,
        high_watermark=2,
        closed=False,
    )
    data = stats.as_dict()
    assert data["size"] == 1
    assert data["maxsize"] == 2
    assert data["closed"] is False


def test_redact_url_removes_credentials_and_tokens() -> None:
    redacted = redact_url("https://user:secret@example.com/path?token=abc&page=2&api_key=xyz")
    assert "secret" not in redacted
    assert "abc" not in redacted
    assert "xyz" not in redacted
    assert "user" not in redacted
    assert "example.com/path" in redacted
    assert "page=2" in redacted


def test_redact_url_handles_bad_input() -> None:
    assert redact_url(None) == "<redacted-url>"  # type: ignore[arg-type]
    assert redact_url("http://[bad") == "<invalid-url>"


def test_redact_headers_is_case_insensitive() -> None:
    headers = {
        "Authorization": "Bearer supersecret",
        "X-Api-Key": "key-123",
        "Content-Type": "application/json",
    }
    redacted = redact_headers(headers)
    assert redacted["Authorization"] == "***"
    assert redacted["X-Api-Key"] == "***"
    assert redacted["Content-Type"] == "application/json"
    assert redact_headers(None) == {}


def test_redact_secret() -> None:
    assert redact_secret(None) == "<none>"
    assert redact_secret("") == "<empty>"
    assert "abc" not in redact_secret("abcdef")
    assert "6 chars" in redact_secret("abcdef")


def test_get_logger_namespacing() -> None:
    assert get_logger().name == "datacurrent"
    assert get_logger("http").name == "datacurrent.http"
    assert get_logger("datacurrent.sse").name == "datacurrent.sse"
