"""HTTP streaming: consume newline-delimited JSON without buffering everything."""

from __future__ import annotations

import asyncio
import json

from _servers import begin_stream, http_server

from datacurrent import HTTPStream


async def responder(writer: asyncio.StreamWriter) -> None:
    await begin_stream(writer, content_type="application/x-ndjson")
    for index in range(5):
        line = json.dumps({"id": index, "value": index * 10}) + "\n"
        writer.write(line.encode())
        await writer.drain()
        await asyncio.sleep(0.01)


async def main() -> None:
    async with http_server(responder) as url:
        rows = []
        async with HTTPStream(url + "/stream") as response:
            async for line in response.iter_lines():
                rows.append(json.loads(line))
        print("rows:", rows)
        print("total:", sum(row["value"] for row in rows))

        # The same body can be exposed as a DataCurrent stream.
        payload = b"".join(await HTTPStream(url + "/raw").as_stream().collect())
        assert payload == b"".join(
            (json.dumps({"id": index, "value": index * 10}) + "\n").encode() for index in range(5)
        )
        print("raw bytes:", len(payload))


if __name__ == "__main__":
    asyncio.run(main())
