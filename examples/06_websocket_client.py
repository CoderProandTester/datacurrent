"""Consume a WebSocket stream, sending JSON and receiving structured messages."""

from __future__ import annotations

import asyncio

from datacurrent import WebSocketStream


async def main() -> None:
    websockets = __import__("websockets")

    async def echo(websocket):
        async for message in websocket:
            await websocket.send(message)

    server = await websockets.serve(echo, "127.0.0.1", 0)
    port = (server.sockets or [])[0].getsockname()[1]
    url = f"ws://127.0.0.1:{port}"

    try:
        async with WebSocketStream.connect(url, mode="json", ping_interval=5) as stream:
            await stream.send_json({"type": "ping", "n": 1})
            print("echo 1:", await stream.recv())

            await stream.send_json({"type": "ping", "n": 2})
            print("echo 2:", await stream.recv())
    finally:
        server.close()
        await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
