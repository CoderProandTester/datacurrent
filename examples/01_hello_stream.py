"""Hello stream: create a stream, iterate it, and reduce it."""

from __future__ import annotations

import asyncio

from datacurrent import Stream


async def main() -> None:
    stream = Stream.from_iterable([1, 2, 3, 4, 5])

    total = 0
    async for number in stream.tap(lambda n: print(f"received {n}")):
        total += number

    print(f"total = {total}")

    # Terminal operations are one-liners.
    names = Stream.from_iterable(["ada", "linus", "grace"])
    print("first:", await names.first())
    print("count:", await Stream.from_iterable(range(10)).count())


if __name__ == "__main__":
    asyncio.run(main())
