"""Transformations: filter, map, distinct, batch, window and reduce."""

from __future__ import annotations

import asyncio

from datacurrent import Stream


async def main() -> None:
    orders = [
        {"id": 1, "total": 12.5, "status": "paid"},
        {"id": 2, "total": 4.0, "status": "paid"},
        {"id": 3, "total": 99.0, "status": "refunded"},
        {"id": 1, "total": 12.5, "status": "paid"},
        {"id": 4, "total": 25.0, "status": "paid"},
    ]

    paid_totals = (
        Stream.from_iterable(orders)
        .filter(lambda order: order["status"] == "paid")
        .distinct(lambda order: order["id"])
        .map(lambda order: order["total"])
    )

    print("paid totals:", await paid_totals.collect())

    batches = await Stream.from_iterable(range(7)).batch(3).collect()
    print("batches:", batches)

    windows = await Stream.from_iterable(range(5)).window(3).collect()
    print("windows:", windows)

    revenue = await (
        Stream.from_iterable(orders)
        .filter(lambda order: order["status"] == "paid")
        .map(lambda order: order["total"])
        .reduce(lambda acc, total: acc + total, 0.0)
    )
    print(f"revenue = {revenue:.2f}")

    pairs = await Stream.from_iterable(["a", "b"]).enumerate(start=1).collect()
    print("indexed:", pairs)


if __name__ == "__main__":
    asyncio.run(main())
