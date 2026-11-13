"""Custom transports: wrap any protocol as a DataCurrent stream."""

from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator

from datacurrent import StreamTransport


class SensorTransport(StreamTransport[dict]):
    """A fake sensor feed that emits a finite number of readings."""

    def __init__(self, *, count: int = 5, name: str = "sensor") -> None:
        super().__init__(name=name)
        self._count = count
        self._closed = False

    async def open(self) -> None:
        self._opened = True
        self._closed = False

    async def close(self) -> None:
        self._closed = True
        self._opened = False

    async def messages(self) -> AsyncIterator[dict]:
        self._ensure_open_sync()
        for index in range(self._count):
            if self._closed:
                return
            await asyncio.sleep(0.001)
            yield {"sensor": self.name, "reading": random.randint(10, 20), "index": index}

    def _ensure_open_sync(self) -> None:
        if not self._opened:
            raise RuntimeError("transport is not open")


async def main() -> None:
    transport = SensorTransport(count=5)

    async with transport:
        readings = (
            await transport.as_stream().filter(lambda reading: reading["reading"] >= 0).collect()
        )

    print("readings:", readings)
    print("count:", len(readings))

    # as_stream() closes the transport when iteration ends or is stopped early.
    transport = SensorTransport(count=100)
    first_two = await transport.as_stream().take(2).collect()
    print("first two:", first_two)
    print("closed after take:", transport.is_open is False)


if __name__ == "__main__":
    asyncio.run(main())
