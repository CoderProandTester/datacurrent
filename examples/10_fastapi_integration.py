"""FastAPI integration: serve DataCurrent events as an SSE endpoint.

Run the demo directly::

    python examples/10_fastapi_integration.py

It builds the app and verifies it with an in-process ASGI request, so it runs
without opening a port. To serve it for real, start uvicorn from the examples
directory::

    cd examples && uvicorn 10_fastapi_integration:app --port 8000
"""

from __future__ import annotations

import asyncio

import httpx
from fastapi import FastAPI

from datacurrent import SSEEvent
from datacurrent.integrations.fastapi import sse_response


def build_app() -> FastAPI:
    app = FastAPI(title="DataCurrent SSE demo")

    @app.get("/events")
    async def events():
        async def generate():
            for index in range(3):
                yield SSEEvent(data=f"tick {index}", event="tick", id=str(index))

        return sse_response(generate())

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


app = build_app()


async def main() -> None:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        health = await client.get("/health")
        print("health:", health.json())

        async with client.stream("GET", "/events") as response:
            print("content-type:", response.headers["content-type"])
            body = "".join([chunk async for chunk in response.aiter_text()])

        print("body:")
        print(body, end="")
        assert "tick 2" in body


if __name__ == "__main__":
    asyncio.run(main())
