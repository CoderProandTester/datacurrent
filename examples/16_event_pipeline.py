"""End to end: publish events, stream them, transform and batch them."""

from __future__ import annotations

import asyncio

from datacurrent import EventBus


async def main() -> None:
    bus = EventBus(name="pipeline")

    pipeline = (
        bus.stream("orders.**")
        .filter(lambda event: event.data["total"] >= 10)
        .map(lambda event: {**event.data, "topic": event.topic})
        .batch(2)
    )

    async def publish_orders() -> None:
        await asyncio.sleep(0.01)
        orders = [
            ("orders.created", {"id": 1, "total": 12.0}),
            ("orders.created", {"id": 2, "total": 3.5}),
            ("orders.paid", {"id": 1, "total": 12.0}),
            ("orders.paid", {"id": 3, "total": 99.0}),
        ]
        for topic, payload in orders:
            await bus.publish(topic, payload)

    publisher = asyncio.create_task(publish_orders())
    batches = []
    async for batch in pipeline:
        batches.append(batch)
        if batches:
            break
    await publisher
    await bus.aclose()

    print("batches:")
    for batch in batches:
        print(" ", batch)


if __name__ == "__main__":
    asyncio.run(main())
