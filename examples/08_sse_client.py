"""SSE client: event ids, event types and JSON payloads."""

from __future__ import annotations

import asyncio
import json

from _servers import begin_stream, http_server

from datacurrent import SSEStream


async def responder(writer: asyncio.StreamWriter) -> None:
    await begin_stream(writer, content_type="text/event-stream")
    writer.write(b": connected\n\n")
    await writer.drain()
    for index in range(3):
        payload = json.dumps({"tick": index})
        writer.write(f"id: {index}\nevent: tick\ndata: {payload}\n\n".encode())
        await writer.drain()
        await asyncio.sleep(0.01)


async def main() -> None:
    async with http_server(responder) as url:
        client = SSEStream.connect(url + "/events", max_reconnects=0)
        iterator = client.__aiter__()
        try:
            for _ in range(3):
                event = await asyncio.wait_for(iterator.__anext__(), timeout=5)
                print(f"{event.event} #{event.id}: {event.json()}")
                assert event.id in {"0", "1", "2"}
        finally:
            await iterator.aclose()
            await client.aclose()

        print("last event id:", client.last_event_id)


if __name__ == "__main__":
    asyncio.run(main())
