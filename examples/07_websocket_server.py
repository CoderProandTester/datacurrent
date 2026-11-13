"""Broadcast server: a Channel feeds every connected WebSocket client."""

from __future__ import annotations

import asyncio

from datacurrent import Channel, WebSocketStream


async def main() -> None:
    websockets = __import__("websockets")
    channel: Channel[str] = Channel(buffer_size=8, name="broadcast")

    async def handler(websocket):
        subscription = channel.subscribe(name="websocket-client")
        try:
            async for message in subscription:
                await websocket.send(message)
        finally:
            await websocket.close()

    server = await websockets.serve(handler, "127.0.0.1", 0)
    port = (server.sockets or [])[0].getsockname()[1]

    try:
        async with WebSocketStream(f"ws://127.0.0.1:{port}") as client:
            for _ in range(100):
                if channel.subscriber_count:
                    break
                await asyncio.sleep(0.01)

            await channel.publish("broadcast one")
            print("client saw:", await client.recv())
            await channel.publish("broadcast two")
            print("client saw:", await client.recv())
    finally:
        # Closing the channel ends every handler's stream, then the server
        # can shut down cleanly without waiting on an idle client.
        await channel.aclose()
        server.close()
        await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
