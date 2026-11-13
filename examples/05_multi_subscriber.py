"""Fan-out: several independent consumers receive every message."""

from __future__ import annotations

import asyncio

from datacurrent import Channel


async def main() -> None:
    channel: Channel[str] = Channel(buffer_size=16, name="news")
    alerts = channel.subscribe(name="alerts")
    archive = channel.subscribe(name="archive")
    metrics = channel.subscribe(name="metrics")

    async def consume(name: str, stream) -> list[str]:
        received = [message async for message in stream.take(3)]
        print(f"{name} received: {received}")
        return received

    consumers = asyncio.gather(
        consume("alerts", alerts),
        consume("archive", archive),
        consume("metrics", metrics),
    )

    for message in ("deploy started", "deploy finished", "deploy verified"):
        delivered = await channel.publish(message)
        print(f"published {message!r} to {delivered} subscriber(s)")

    await consumers
    print("channel stats:", channel.stats())
    await channel.aclose()


if __name__ == "__main__":
    asyncio.run(main())
