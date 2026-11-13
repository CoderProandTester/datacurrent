"""Backpressure: a slow consumer suspends the producer instead of growing memory."""

from __future__ import annotations

import asyncio
import time

from datacurrent import BoundedQueue, Stream


async def main() -> None:
    queue: BoundedQueue[int] = BoundedQueue(2, overflow="block", name="demo")
    timeline: list[str] = []

    async def producer() -> None:
        for number in range(5):
            await queue.put(number)
            timeline.append(f"produced {number} (depth={queue.qsize()})")

    async def consumer() -> list[int]:
        received: list[int] = []
        for _ in range(5):
            await asyncio.sleep(0.01)
            received.append(await queue.get())
        return received

    started = time.perf_counter()
    producer_task = asyncio.create_task(producer())
    received = await consumer()
    await producer_task
    await queue.aclose()
    elapsed = time.perf_counter() - started

    print("received:", received)
    print("producer timeline:")
    for line in timeline:
        print(" ", line)
    print(f"elapsed: {elapsed * 1000:.0f} ms")
    print("stats:", queue.stats().as_dict())

    # Lossy fan-out is explicit: drops are counted, never silent.
    lossy = Stream.from_iterable(range(1000)).buffer(size=4, overflow="drop_oldest")
    kept = await lossy.collect()
    print("lossy kept:", len(kept), "dropped:", lossy.stats["dropped"])

    # The default is safe: no drops unless you ask for them.
    safe = Stream.from_iterable(range(10)).buffer()
    print("safe received:", await safe.collect())


if __name__ == "__main__":
    asyncio.run(main())
