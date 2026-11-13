"""Helpers for the local HTTP test server."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass
class Request:
    method: str
    path: str
    version: str
    headers: dict[str, str]


async def read_request(reader: asyncio.StreamReader) -> Request | None:
    line = await reader.readline()
    if not line:
        return None
    parts = line.decode("latin-1").strip().split(" ")
    if len(parts) != 3:
        return None
    headers: dict[str, str] = {}
    while True:
        raw = await reader.readline()
        if not raw or raw in (b"\r\n", b"\n"):
            break
        key, _, value = raw.decode("latin-1").partition(":")
        headers[key.strip().lower()] = value.strip()
    return Request(parts[0], parts[1], parts[2], headers)


async def begin_response(
    writer: asyncio.StreamWriter,
    *,
    status: int = 200,
    reason: str = "OK",
    content_type: str = "text/plain; charset=utf-8",
    headers: dict[str, str] | None = None,
) -> None:
    lines = [
        f"HTTP/1.1 {status} {reason}",
        f"Content-Type: {content_type}",
        "Connection: close",
    ]
    for key, value in (headers or {}).items():
        lines.append(f"{key}: {value}")
    writer.write(("\r\n".join(lines) + "\r\n\r\n").encode("latin-1"))
    await writer.drain()


async def write_body(
    writer: asyncio.StreamWriter,
    body: bytes,
    *,
    chunks: int = 1,
    delay: float = 0.0,
) -> None:
    if chunks <= 1:
        writer.write(body)
        await writer.drain()
        return
    size = max(1, len(body) // chunks)
    for index in range(0, len(body), size):
        writer.write(body[index : index + size])
        await writer.drain()
        if delay:
            await asyncio.sleep(delay)
