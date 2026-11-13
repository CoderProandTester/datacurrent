"""Process a large synthetic dataset without loading it into memory."""

from __future__ import annotations

import asyncio

from datacurrent import Stream


def generate_reading(index: int) -> dict[str, float]:
    return {"sensor": index % 20, "value": (index * 37 % 1000) / 10}


async def read_sensor_data():
    """A generator that stands in for a database cursor or a file reader."""
    for index in range(100_000):
        yield generate_reading(index)


async def main() -> None:
    async def source():
        async for reading in read_sensor_data():
            yield reading

    batch_sums = await (
        Stream.from_async_iterable(source())
        .batch(5_000)
        .map(lambda batch: sum(reading["value"] for reading in batch))
        .collect()
    )

    print(f"batches: {len(batch_sums)}")
    print(f"total: {sum(batch_sums):.1f}")

    hottest = await (
        Stream.from_async_iterable(read_sensor_data())
        .filter(lambda reading: reading["sensor"] == 7)
        .map(lambda reading: reading["value"])
        .reduce(max)
    )
    print(f"hottest sensor 7 reading: {hottest:.1f}")

    # Batches can be processed concurrently while order is preserved.
    rows = await (
        Stream.from_iterable(range(20))
        .map(lambda number: {"n": number, "square": number * number}, concurrency=4)
        .take(5)
        .collect()
    )
    print("concurrent rows:", rows)


if __name__ == "__main__":
    asyncio.run(main())
