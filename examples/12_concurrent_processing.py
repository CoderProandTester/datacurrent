"""Concurrent processing with a bounded number of workers and preserved order."""

from __future__ import annotations

import asyncio
import time

from datacurrent import ConcurrencyLimiter, Stream, consume


async def fake_fetch(url: str) -> dict[str, str]:
    await asyncio.sleep(0.01)  # simulate network latency
    return {"url": url, "status": "ok"}


async def main() -> None:
    urls = [f"https://example.com/item/{index}" for index in range(16)]

    started = time.perf_counter()
    results = await Stream.from_iterable(urls).map(fake_fetch, concurrency=4).collect()
    elapsed = time.perf_counter() - started

    print("results:", len(results), "all ok:", all(r["status"] == "ok" for r in results))
    print(f"elapsed with concurrency=4: {elapsed * 1000:.0f} ms (serial would be ~160 ms)")

    processed = await consume(
        Stream.from_async_iterable(results_ready(results)), lambda item: None, concurrency=8
    )
    print("processed:", processed)

    # ConcurrencyLimiter gives the same bound to arbitrary code.
    limiter = ConcurrencyLimiter(3)

    async def task(number: int) -> int:
        async with limiter:
            await asyncio.sleep(0.005)
            return number

    values = await asyncio.gather(*(task(number) for number in range(9)))
    print("limiter peak:", limiter.peak, "values:", values)


async def results_ready(results):
    for result in results:
        yield result


if __name__ == "__main__":
    asyncio.run(main())
