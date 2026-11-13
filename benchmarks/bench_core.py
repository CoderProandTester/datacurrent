"""Reproducible micro-benchmarks for DataCurrent.

Run with::

    python benchmarks/bench_core.py

Results are written to ``benchmarks/results/latest.json`` and printed as a table.
The machine, Python version and workload are recorded so numbers are comparable.
Every benchmark reports throughput over the median of three timed runs.
"""

from __future__ import annotations

import asyncio
import gc
import json
import platform
import statistics
import sys
import time
import tracemalloc
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from datacurrent import Channel, EventBus, Stream, __version__  # noqa: E402

RESULTS_DIR = ROOT / "benchmarks" / "results"


async def _measure(factory, *, repeats: int = 3) -> tuple[float, int]:
    samples: list[float] = []
    items = 0
    for _ in range(repeats):
        gc.collect()
        started = time.perf_counter()
        items = await factory()
        samples.append(time.perf_counter() - started)
    return statistics.median(samples), items


def _record(name: str, seconds: float, items: int, unit: str, **extra: object) -> dict:
    return {
        "name": name,
        "unit": unit,
        "items": items,
        "seconds": round(seconds, 4),
        "per_second": round(items / seconds) if seconds else None,
        **extra,
    }


async def bench_pipeline() -> dict:
    count = 100_000

    async def run() -> int:
        return await (
            Stream.from_iterable(range(count))
            .map(lambda n: n + 1)
            .filter(lambda n: n % 2 == 0)
            .count()
        )

    seconds, items = await _measure(run)
    return _record("map+filter+count", seconds, items, "items/s")


async def bench_batch() -> dict:
    count = 100_000

    async def run() -> int:
        total = 0
        async for batch in Stream.from_iterable(range(count)).batch(1_000):
            total += len(batch)
        return total

    seconds, items = await _measure(run)
    return _record("batch(1000)", seconds, items, "items/s")


async def bench_concurrent_map() -> dict:
    count = 20_000

    async def run() -> int:
        return await Stream.from_iterable(range(count)).map(lambda n: n * 2, concurrency=10).count()

    seconds, items = await _measure(run)
    return _record("map(concurrency=10)", seconds, items, "items/s")


async def bench_buffered_stream() -> dict:
    count = 20_000

    async def run() -> int:
        received = 0
        stream = Stream.from_iterable(range(count)).buffer(size=64)
        async for _ in stream:
            received += 1
            if received % 500 == 0:
                await asyncio.sleep(0)
        return received

    seconds, items = await _measure(run)
    return _record("buffer(64)", seconds, items, "items/s")


async def bench_queue() -> dict:
    from datacurrent import BoundedQueue

    count = 100_000

    async def run() -> int:
        queue: BoundedQueue[int] = BoundedQueue(1024)

        async def produce() -> None:
            for number in range(count):
                await queue.put(number)
            await queue.aclose()

        async def consume() -> int:
            total = 0
            async for _ in queue:
                total += 1
            return total

        producer = asyncio.create_task(produce())
        total = await consume()
        await producer
        return total

    seconds, items = await _measure(run)
    return _record("bounded queue put/get", seconds, items, "items/s")


async def bench_event_bus() -> dict:
    count = 20_000

    async def run() -> int:
        bus = EventBus(buffer_size=2048)
        delivered = 0

        def handler(event) -> None:
            nonlocal delivered
            delivered += 1

        bus.subscribe("bench.**", handler)
        for number in range(count):
            await bus.publish("bench.item", number)
        await bus.aclose(drain=True)
        return delivered

    seconds, items = await _measure(run)
    return _record("event bus publish+deliver", seconds, items, "events/s")


async def bench_channel() -> dict:
    count = 20_000
    subscribers = 3

    async def run() -> int:
        channel: Channel[int] = Channel(buffer_size=1024)
        streams = [channel.subscribe() for _ in range(subscribers)]

        async def consume(stream) -> int:
            total = 0
            async for _ in stream:
                total += 1
            return total

        consumers = [asyncio.create_task(consume(stream)) for stream in streams]
        for number in range(count):
            await channel.publish(number)
        await channel.aclose()
        totals = await asyncio.gather(*consumers)
        assert all(total == count for total in totals)
        return count * subscribers

    seconds, items = await _measure(run)
    return _record(f"channel fan-out x{subscribers}", seconds, items, "deliveries/s")


async def bench_memory() -> dict:
    gc.collect()
    tracemalloc.start()
    try:
        await (
            Stream.from_iterable(range(200_000))
            .map(lambda n: n + 1)
            .filter(lambda n: n % 2 == 0)
            .count()
        )
        current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return {
        "name": "peak memory (200k pipeline)",
        "unit": "bytes",
        "items": 200_000,
        "seconds": None,
        "per_second": None,
        "peak_bytes": peak,
        "retained_bytes": current,
    }


async def main() -> None:
    scenarios = [
        bench_pipeline,
        bench_batch,
        bench_concurrent_map,
        bench_buffered_stream,
        bench_queue,
        bench_event_bus,
        bench_channel,
        bench_memory,
    ]
    results = [await scenario() for scenario in scenarios]

    report = {
        "package": "datacurrent",
        "version": __version__,
        "python": sys.version.split()[0],
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "processor": platform.processor() or "unknown",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "results": results,
    }

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    (RESULTS_DIR / "latest.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"DataCurrent {__version__} on Python {report['python']}")
    print(f"{'benchmark':32s} {'items':>9s} {'seconds':>9s} {'rate':>14s}")
    for result in results:
        rate = f"{result['per_second']:,} {result['unit']}" if result["per_second"] else "-"
        seconds = f"{result['seconds']:.4f}" if result["seconds"] is not None else "-"
        print(f"{result['name']:32s} {result['items']:>9,d} {seconds:>9s} {rate:>14s}")
    print(f"\nwritten to {RESULTS_DIR / 'latest.json'}")


if __name__ == "__main__":
    asyncio.run(main())
