"""Retries and recovery for flaky sources.

Retries need a replayable stream: use ``Stream.from_factory``.
"""

from __future__ import annotations

import asyncio

from datacurrent import RetryPolicy, Stream


async def main() -> None:
    attempts = 0

    def flaky_source():
        nonlocal attempts
        attempts += 1
        current = attempts

        async def source():
            yield f"attempt {current}: first record"
            if current < 3:
                raise ConnectionError("upstream reset the connection")
            yield f"attempt {current}: second record"

        return source()

    stream = Stream.from_factory(flaky_source).retry(
        RetryPolicy(max_attempts=3, base_delay=0.01, jitter=0)
    )
    records = await stream.collect()
    print("records:", records)
    print("attempts:", attempts, "retries:", stream.stats["retries"])

    async def broken():
        yield "partial"
        raise TimeoutError("deadline exceeded")

    recovered = (
        await Stream.from_async_iterable(broken())
        .recover(lambda error: [f"fallback after {type(error).__name__}"])
        .collect()
    )
    print("recovered:", recovered)

    # Non-replayable streams fail with a clear error instead of silently
    # resuming in the middle.
    async def one_shot():
        yield 1
        raise RuntimeError("cannot restart me")

    try:
        await Stream.from_async_iterable(one_shot()).retry(3).collect()
    except Exception as error:
        print("expected failure:", type(error).__name__, "-", error)


if __name__ == "__main__":
    asyncio.run(main())
