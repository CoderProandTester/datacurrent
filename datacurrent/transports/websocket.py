"""WebSocket streaming over the maintained ``websockets`` library."""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import AsyncIterator, Mapping
from typing import Any

from ..config import DEFAULT_MAX_MESSAGE_SIZE
from ..exceptions import (
    ConfigurationError,
    ConnectionFailedError,
    MessageTooLargeError,
    StreamClosedError,
    WebSocketClosedError,
)
from ..observability.logging import get_logger, redact_headers, redact_url
from ._deps import require_module

__all__ = ["WebSocketStream"]


def _close_details(error: Any) -> tuple[int | None, str | None]:
    """Read close code/reason without the deprecated attributes."""
    frame = getattr(error, "rcvd", None) or getattr(error, "sent", None)
    code = getattr(frame, "code", None)
    reason = getattr(frame, "reason", None)
    if code is None:
        code = getattr(error, "code", None)
    if reason is None:
        reason = getattr(error, "reason", None)
    return code, reason


_log = get_logger("websocket")

_VALID_MODES = {"auto", "text", "json"}


class WebSocketStream:
    """A WebSocket client that behaves like an async sequence.

    Args:
        url: ``ws://`` or ``wss://`` endpoint.
        headers: Handshake headers; values are redacted in logs and errors.
        mode: ``auto`` yields ``str``/``bytes`` as received, ``text`` decodes
            binary frames to text, ``json`` parses text frames as JSON.
        max_size: Maximum accepted message size in bytes.
        ping_interval / ping_timeout: Protocol heartbeats in seconds.
        open_timeout / close_timeout: Handshake and close deadlines.
        reconnect_attempts: Automatic reconnects after an abnormal close.
        reconnect_delay / max_reconnect_delay: Exponential backoff bounds.
        subprotocols: Optional WebSocket subprotocols.
        ssl: Optional TLS context passed to ``websockets``.
        origin: Optional ``Origin`` header.

    TLS certificates are verified by default: ``wss://`` uses the system trust
    store through ``websockets``.  Only an explicit ``ssl=`` context can change
    that, and doing so is the caller's decision.
    """

    def __init__(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        mode: str = "auto",
        max_size: int = DEFAULT_MAX_MESSAGE_SIZE,
        ping_interval: float | None = 20.0,
        ping_timeout: float | None = 20.0,
        open_timeout: float = 10.0,
        close_timeout: float = 5.0,
        reconnect_attempts: int = 0,
        reconnect_delay: float = 0.5,
        max_reconnect_delay: float = 30.0,
        subprotocols: list[str] | None = None,
        ssl: Any = None,
        origin: str | None = None,
    ) -> None:
        if not isinstance(url, str) or not url.lower().startswith(("ws://", "wss://")):
            raise ConfigurationError("WebSocketStream requires a ws:// or wss:// URL")
        if mode not in _VALID_MODES:
            raise ConfigurationError(f"mode must be one of {sorted(_VALID_MODES)}, got {mode!r}")
        if isinstance(reconnect_attempts, bool) or not isinstance(reconnect_attempts, int):
            raise ConfigurationError("reconnect_attempts must be an integer")
        if reconnect_attempts < 0:
            raise ConfigurationError("reconnect_attempts must be >= 0")
        if max_size < 1:
            raise ConfigurationError("max_size must be >= 1")
        if ping_interval is not None and ping_interval < 0:
            raise ConfigurationError("ping_interval must be >= 0 or None")
        self._url = url
        self._headers = dict(headers or {})
        self._mode = mode
        self._max_size = max_size
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._open_timeout = open_timeout
        self._close_timeout = close_timeout
        self._reconnect_attempts = reconnect_attempts
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay
        self._subprotocols = subprotocols
        self._ssl = ssl
        self._origin = origin
        self._ws: Any = None
        self._closed = False

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    @classmethod
    def connect(cls, url: str, **kwargs: Any) -> WebSocketStream:
        """Create a client; use ``async with WebSocketStream.connect(...)``."""
        return cls(url, **kwargs)

    @property
    def url(self) -> str:
        return self._url

    @property
    def connected(self) -> bool:
        return self._ws is not None

    @property
    def protocol(self) -> str | None:
        return None if self._ws is None else getattr(self._ws, "subprotocol", None)

    async def __aenter__(self) -> WebSocketStream:
        await self._connect()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def _connect(self) -> None:
        websockets = require_module("websockets", purpose="WebSocketStream")
        parameters = inspect.signature(websockets.connect).parameters
        header_key = "additional_headers" if "additional_headers" in parameters else "extra_headers"
        kwargs: dict[str, Any] = {
            header_key: self._headers,
            "max_size": self._max_size,
            "ping_interval": self._ping_interval,
            "ping_timeout": self._ping_timeout,
            "open_timeout": self._open_timeout,
            "close_timeout": self._close_timeout,
        }
        if self._subprotocols:
            kwargs["subprotocols"] = self._subprotocols
        if self._ssl is not None:
            kwargs["ssl"] = self._ssl
        if self._origin is not None:
            kwargs["origin"] = self._origin
        try:
            self._ws = await websockets.connect(self._url, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise ConnectionFailedError(
                f"could not connect to {redact_url(self._url)}: {error}"
            ) from error
        _log.debug(
            "websocket connected url=%s headers=%s",
            redact_url(self._url),
            redact_headers(self._headers),
        )

    async def aclose(self) -> None:
        """Close the connection without raising on an already closed socket."""
        self._closed = True
        ws, self._ws = self._ws, None
        if ws is None:
            return
        try:
            await ws.close()
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - close races are harmless
            return

    # ------------------------------------------------------------------ #
    # Sending
    # ------------------------------------------------------------------ #
    def _require_ws(self) -> Any:
        if self._ws is None:
            raise StreamClosedError(
                "websocket is not connected; use 'async with WebSocketStream.connect(...)'"
            )
        return self._ws

    async def send(self, data: str | bytes) -> None:
        """Send a text or binary frame."""
        await self._require_ws().send(data)

    async def send_json(self, value: Any) -> None:
        """Serialize ``value`` and send it as a text frame."""
        await self._require_ws().send(json.dumps(value))

    async def ping(self) -> None:
        """Send a ping and wait for the pong."""
        waiter = await self._require_ws().ping()
        await waiter

    # ------------------------------------------------------------------ #
    # Receiving
    # ------------------------------------------------------------------ #
    def _decode(self, message: str | bytes) -> Any:
        size = len(message.encode("utf-8") if isinstance(message, str) else message)
        if size > self._max_size:
            raise MessageTooLargeError(size, self._max_size)
        if self._mode == "json":
            if isinstance(message, bytes):
                message = message.decode("utf-8")
            return json.loads(message)
        if self._mode == "text" and isinstance(message, bytes):
            return message.decode("utf-8")
        return message

    async def recv(self) -> Any:
        """Receive one message from the open connection.

        Reconnection applies to ``async for``; ``recv()`` raises
        :class:`WebSocketClosedError` when the connection ends.
        """
        websockets = require_module("websockets", purpose="WebSocketStream")
        if self._ws is None:
            await self._connect()
        assert self._ws is not None
        try:
            message = await self._ws.recv()
        except websockets.ConnectionClosed as error:
            code, reason = _close_details(error)
            raise WebSocketClosedError(code, reason) from error
        return self._decode(message)

    def __aiter__(self) -> AsyncIterator[Any]:
        return self._messages()

    async def _messages(self) -> AsyncIterator[Any]:
        websockets = require_module("websockets", purpose="WebSocketStream")
        if self._closed:
            raise StreamClosedError("WebSocketStream is closed")
        if self._ws is None:
            await self._connect()
        attempts = 0
        while True:
            ws = self._ws
            if ws is None:
                raise StreamClosedError("websocket is not connected")
            try:
                async for message in ws:
                    yield self._decode(message)
                return
            except asyncio.CancelledError:
                raise
            except websockets.ConnectionClosed as error:
                code, reason = _close_details(error)
                if self._closed:
                    raise WebSocketClosedError(code, reason) from error
                if code == 1000:
                    return
                if attempts >= self._reconnect_attempts:
                    raise WebSocketClosedError(code, reason) from error
                attempts += 1
                await self.aclose()
                self._closed = False
                delay = min(
                    self._max_reconnect_delay,
                    self._reconnect_delay * (2 ** (attempts - 1)),
                )
                if delay:
                    await asyncio.sleep(delay)
                await self._connect()

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return (
            f"WebSocketStream(url={self._url!r}, mode={self._mode!r}, "
            f"connected={self._ws is not None})"
        )
