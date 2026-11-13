"""Server-Sent Events (SSE).

The parser follows the WHATWG event-stream rules: ``data`` lines are joined with
newlines, blank lines dispatch, comment lines are ignored, ``event`` type,
``id`` and ``retry`` fields are honoured.  The client supports ``Last-Event-ID``
resumption, heartbeat comments and configurable reconnection limits.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterable, AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Any

from ..config import DEFAULT_TIMEOUT
from ..exceptions import (
    ConfigurationError,
    ConnectionFailedError,
    SSEProtocolError,
    StreamClosedError,
)
from ..observability.logging import redact_url
from ._deps import require_module

__all__ = [
    "SSEEvent",
    "SSELineDecoder",
    "SSEParser",
    "SSEStream",
    "format_heartbeat",
    "format_sse",
    "parse_sse",
]


@dataclass(frozen=True, slots=True)
class SSEEvent:
    """A dispatched Server-Sent Event."""

    data: str = ""
    event: str = "message"
    id: str | None = None
    retry: int | None = None

    def json(self) -> Any:
        """Parse ``data`` as JSON."""
        return json.loads(self.data)


class SSELineDecoder:
    """Split arbitrary text chunks into lines following the SSE line rules."""

    __slots__ = ("_buffer",)

    def __init__(self) -> None:
        self._buffer = ""

    def feed(self, chunk: str) -> list[str]:
        if chunk:
            self._buffer += chunk
        lines: list[str] = []
        while True:
            index = -1
            for candidate in ("\n", "\r"):
                position = self._buffer.find(candidate)
                if position != -1 and (index == -1 or position < index):
                    index = position
            if index == -1:
                break
            character = self._buffer[index]
            if character == "\r" and index == len(self._buffer) - 1:
                # A CRLF pair may be split across chunks; wait for the next chunk.
                break
            line = self._buffer[:index]
            skip = 1
            if (
                character == "\r"
                and index + 1 < len(self._buffer)
                and self._buffer[index + 1] == "\n"
            ):
                skip = 2
            self._buffer = self._buffer[index + skip :]
            lines.append(line)
        return lines

    def flush(self) -> list[str]:
        """Return a trailing line that had no newline terminator."""
        if not self._buffer:
            return []
        line = self._buffer
        self._buffer = ""
        return [line]


class SSEParser:
    """Incremental SSE event parser.

    Feed complete lines with :meth:`feed_line`; a non-``None`` return value is a
    dispatched event.
    """

    __slots__ = ("_data", "_event", "_has_data", "_last_id", "_retry")

    def __init__(self) -> None:
        self._data: list[str] = []
        self._event = "message"
        self._has_data = False
        self._last_id: str | None = None
        self._retry: int | None = None

    @property
    def last_event_id(self) -> str | None:
        return self._last_id

    def feed_line(self, line: str) -> SSEEvent | None:
        line = line.rstrip("\r\n")
        if line == "":
            return self._dispatch()
        if line.startswith(":"):
            return None
        field, separator, value = line.partition(":")
        if not separator:
            value = ""
        elif value.startswith(" "):
            value = value[1:]

        if field == "data":
            self._data.append(value)
            self._has_data = True
        elif field == "event":
            self._event = value or "message"
        elif field == "id":
            if "\x00" not in value:
                self._last_id = value
        elif field == "retry":
            try:
                parsed = int(value)
            except ValueError:
                return None
            if parsed >= 0:
                self._retry = parsed
        return None

    def _dispatch(self) -> SSEEvent | None:
        if not self._has_data:
            self._event = "message"
            return None
        event = SSEEvent(
            data="\n".join(self._data),
            event=self._event or "message",
            id=self._last_id,
            retry=self._retry,
        )
        self._data = []
        self._has_data = False
        self._event = "message"
        return event


async def parse_sse(lines: AsyncIterable[str | bytes]) -> AsyncIterator[SSEEvent]:
    """Parse an async iterable of text or bytes chunks into events."""
    decoder = SSELineDecoder()
    parser = SSEParser()
    async for chunk in lines:
        text = (
            chunk.decode("utf-8", errors="replace")
            if isinstance(chunk, (bytes, bytearray))
            else str(chunk)
        )
        for line in decoder.feed(text):
            event = parser.feed_line(line)
            if event is not None:
                yield event
    for line in decoder.flush():
        event = parser.feed_line(line)
        if event is not None:
            yield event
    trailing = parser.feed_line("")
    if trailing is not None:
        yield trailing


def format_sse(event: SSEEvent) -> str:
    """Encode an :class:`SSEEvent` for a server response."""
    if not isinstance(event, SSEEvent):
        raise TypeError(f"format_sse() expects an SSEEvent, got {type(event).__name__}")
    lines: list[str] = []
    if event.id is not None:
        lines.append(f"id: {event.id}")
    if event.event and event.event != "message":
        lines.append(f"event: {event.event}")
    if event.retry is not None:
        lines.append(f"retry: {int(event.retry)}")
    for line in event.data.split("\n"):
        lines.append(f"data: {line}")
    return "\n".join(lines) + "\n\n"


def format_heartbeat(comment: str = "keep-alive") -> str:
    """Encode an SSE comment line, used as a heartbeat."""
    return f": {comment}\n\n"


class SSEStream:
    """Async client for an SSE endpoint.

    Args:
        url: ``http`` or ``https`` endpoint.
        headers: Extra request headers (credentials are never logged).
        params: Query parameters.
        method: HTTP method, ``GET`` by default.
        timeout: Per-request timeout in seconds.
        client: Optional ``httpx.AsyncClient``; one is created when omitted.
        max_reconnects: Additional connections after the first one ends or
            fails.  ``0`` (default) never reconnects.
        reconnect_delay / max_reconnect_delay: Exponential backoff bounds.
        last_event_id: Resume from this event id on the first request.
        check_content_type: Require ``text/event-stream`` responses.

    The client yields :class:`SSEEvent` objects and tracks the last event id so a
    reconnect sends ``Last-Event-ID`` automatically.
    """

    def __init__(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        method: str = "GET",
        timeout: float = DEFAULT_TIMEOUT,
        client: Any = None,
        max_reconnects: int = 0,
        reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 30.0,
        last_event_id: str | None = None,
        check_content_type: bool = True,
    ) -> None:
        if not isinstance(url, str) or not url.lower().startswith(("http://", "https://")):
            raise ConfigurationError("SSEStream requires an http:// or https:// URL")
        if isinstance(max_reconnects, bool) or not isinstance(max_reconnects, int):
            raise ConfigurationError("max_reconnects must be an integer")
        if max_reconnects < 0:
            raise ConfigurationError("max_reconnects must be >= 0")
        if reconnect_delay < 0 or max_reconnect_delay < 0:
            raise ConfigurationError("reconnect delays must be >= 0")
        self._url = url
        self._headers = {"Accept": "text/event-stream", **dict(headers or {})}
        self._params = dict(params) if params else None
        self._method = method.upper()
        self._timeout = float(timeout)
        self._client = client
        self._owned_client: Any = None
        self._max_reconnects = max_reconnects
        self._reconnect_delay = float(reconnect_delay)
        self._max_reconnect_delay = float(max_reconnect_delay)
        self._last_event_id = last_event_id
        self._retry_hint: float | None = None
        self._check_content_type = check_content_type
        self._closed = False

    # ------------------------------------------------------------------ #
    # Construction / lifecycle
    # ------------------------------------------------------------------ #
    @classmethod
    def connect(cls, url: str, **kwargs: Any) -> SSEStream:
        """Create a client; use it with ``async for`` or ``async with``."""
        return cls(url, **kwargs)

    @property
    def url(self) -> str:
        return self._url

    @property
    def last_event_id(self) -> str | None:
        return self._last_event_id

    @property
    def closed(self) -> bool:
        return self._closed

    async def __aenter__(self) -> SSEStream:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close the client and drop the owned HTTP client, if any."""
        self._closed = True
        client, self._owned_client = self._owned_client, None
        if client is not None:
            await client.aclose()

    def __aiter__(self) -> AsyncIterator[SSEEvent]:
        return self._events()

    # ------------------------------------------------------------------ #
    # Streaming
    # ------------------------------------------------------------------ #
    def _next_delay(self, reconnects: int) -> float:
        if self._retry_hint is not None:
            return self._retry_hint
        base = self._reconnect_delay * (2 ** min(reconnects, 10))
        return min(self._max_reconnect_delay, base)

    async def _events(self) -> AsyncIterator[SSEEvent]:
        httpx = require_module("httpx", purpose="SSEStream")
        if self._closed:
            raise StreamClosedError("SSEStream is closed")
        if self._client is not None:
            client = self._client
        else:
            client = httpx.AsyncClient(follow_redirects=True)
            self._owned_client = client

        reconnects = 0
        try:
            while True:
                headers = dict(self._headers)
                if self._last_event_id is not None:
                    headers["Last-Event-ID"] = self._last_event_id
                try:
                    async with client.stream(
                        self._method,
                        self._url,
                        headers=headers,
                        params=self._params,
                        timeout=self._timeout,
                    ) as response:
                        if response.status_code >= 400:
                            response.raise_for_status()
                        if self._check_content_type:
                            content_type = response.headers.get("content-type", "")
                            if "text/event-stream" not in content_type.lower():
                                raise SSEProtocolError(
                                    f"expected a 'text/event-stream' response, got {content_type!r}"
                                )
                        # aiter_bytes() gives decoded body chunks; parse_sse splits
                        # lines itself so events spanning chunks are handled.
                        async for event in parse_sse(response.aiter_bytes()):
                            if event.id is not None:
                                self._last_event_id = event.id
                            if event.retry is not None:
                                self._retry_hint = event.retry / 1000.0
                            yield event
                except asyncio.CancelledError:
                    raise
                except SSEProtocolError:
                    raise
                except httpx.HTTPStatusError:
                    raise
                except (httpx.HTTPError, OSError) as error:
                    if reconnects >= self._max_reconnects:
                        raise ConnectionFailedError(
                            f"SSE connection to {redact_url(self._url)} failed: {error}"
                        ) from error
                    delay = self._next_delay(reconnects)
                    reconnects += 1
                    await asyncio.sleep(delay)
                    continue

                if reconnects >= self._max_reconnects:
                    return
                delay = self._next_delay(reconnects)
                reconnects += 1
                if delay:
                    await asyncio.sleep(delay)
        finally:
            if self._owned_client is client:
                self._owned_client = None
                await client.aclose()
