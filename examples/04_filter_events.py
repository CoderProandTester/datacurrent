"""Filter events with topic patterns and payload predicates."""

from __future__ import annotations

import asyncio

from datacurrent import EventBus


async def main() -> None:
    bus = EventBus()
    audit_log: list[str] = []

    @bus.subscribe("user.*")
    async def on_user_event(event):
        audit_log.append(f"{event.topic}: {event.data}")

    @bus.subscribe("user.deleted")
    async def on_deleted(event):
        print("cascade cleanup for", event.data["user_id"])

    await bus.publish("user.created", {"user_id": 1})
    await bus.publish("user.updated", {"user_id": 1, "field": "email"})
    await bus.publish("order.created", {"order_id": 9})  # not subscribed
    await bus.publish("user.deleted", {"user_id": 1})
    await bus.aclose(drain=True)

    print("audit entries:", len(audit_log))
    for entry in audit_log:
        print(" ", entry)


if __name__ == "__main__":
    asyncio.run(main())
