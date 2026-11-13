"""Real-time notifications: one event, several delivery channels."""

from __future__ import annotations

import asyncio

from datacurrent import Channel, Event, EventBus


async def main() -> None:
    bus = EventBus(name="notifications")
    websocket_channel: Channel[str] = Channel(buffer_size=32, name="ws")
    email_queue: list[str] = []

    @bus.subscribe("notifications.**")
    async def fan_out(event: Event) -> None:
        message = f"{event.topic}: {event.data['text']}"
        await websocket_channel.publish(message)

    @bus.subscribe("notifications.email")
    async def send_email(event: Event) -> None:
        email_queue.append(event.data["text"])

    ws_client = websocket_channel.subscribe(name="ws-client")

    async def read_ws(n: int) -> list[str]:
        return [message async for message in ws_client.take(n)]

    reader = asyncio.gather(read_ws(3))

    await bus.publish("notifications.in_app", {"text": "you have a new follower"})
    await bus.publish("notifications.email", {"text": "weekly digest is ready"})
    await bus.publish("notifications.in_app", {"text": "your export finished"})

    delivered = await reader
    await bus.aclose(drain=True)
    await websocket_channel.aclose()

    print("websocket messages:")
    for message in delivered:
        print(" ", message)
    print("emails:", email_queue)
    print("bus stats:", bus.stats())


if __name__ == "__main__":
    asyncio.run(main())
