"""HTTP streaming over ``httpx``.

Use :class:`HTTPStream` to consume slow endpoints, streaming downloads or
newline-delimited JSON without buffering the whole response in memory.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Any

from ..config import DEFAULT_TIMEOUT
from ..core.stream import Stream
from ..exceptions import ConfigurationError, MessageTooLargeError, StreamClosedError
from ._deps import require_module

__all__ = ["HTTPStream"]


class HTTPStream:
    """An HTTP response consumed incrementally.

    Example::

        async with HTTPStream("https://example.com/feed.ndjson") as response:
            async for line in response.iter_lines():
                print(line)

    The object must be opened with ``async with``; closing it releases the
    response and the owned HTTP client.
    """

    def __init__(
        self,
        url: str,
        *,
        method: str = "GET",
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any = None,
        data: Any = None,
        content: Any = None,
        timeout: float = DEFAULT_TIMEOUT,
        chunk_size: int = 64 * 1024,
        client: Any = None,
        max_bytes: int | None = None,
        raise_for_status: bool = True,
        follow_redirects: bool = True,
    ) -> None:
        if not isinstance(url, str) or not url.lower().startswith(("http://", "https://")):
            raise ConfigurationError("HTTPStream requires an http:// or https:// URL")
        if isinstance(chunk_size, bool) or not isinstance(chunk_size, int) or chunk_size < 1:
            raise ConfigurationError("chunk_size must be an integer >= 1")
        if max_bytes is not None and max_bytes < 1:
            raise ConfigurationError("max_bytes must be >= 1 when provided")
        self._url = url
        self._method = method.upper()
        self._headers = dict(headers or {})
        self._params = dict(params) if params else None
        self._json = json
        self._data = data
        self._content = content
        self._timeout = float(timeout)
        self._chunk_size = chunk_size
        self._client = client
        self._owned_client: Any = None
        self._max_bytes = max_bytes
        self._raise_for_status = raise_for_status
        self._follow_redirects = follow_redirects
        self._response: Any = None
        self._closed = False

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    @classmethod
    def get(cls, url: str, **kwargs: Any) -> HTTPStream:
        """Convenience constructor for GET requests."""
        kwargs.setdefault("method", "GET")
        return cls(url, **kwargs)

    @property
    def url(self) -> str:
        return self._url

    @property
    def status_code(self) -> int:
        return int(self._require_response().status_code)

    @property
    def response(self) -> Any:
        return self._require_response()

    async def __aenter__(self) -> HTTPStream:
        httpx = require_module("httpx", purpose="HTTPStream")
        if self._client is not None:
            client = self._client
        else:
            client = httpx.AsyncClient(
                follow_redirects=self._follow_redirects,
                timeout=self._timeout,
            )
            self._owned_client = client
        self._client = client
        request = client.build_request(
            self._method,
            self._url,
            headers=self._headers,
            params=self._params,
            json=self._json,
            data=self._data,
            content=self._content,
            timeout=self._timeout,
        )
        self._response = await client.send(request, stream=True)
        if self._raise_for_status and self._response.is_error:
            await self._response.aread()
            try:
                self._response.raise_for_status()
            finally:
                await self._close_response()
                await self._close_owned_client()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def _close_response(self) -> None:
        response, self._response = self._response, None
        if response is not None:
            await response.aclose()

    async def _close_owned_client(self) -> None:
        client, self._owned_client = self._owned_client, None
        if client is not None:
            await client.aclose()

    async def aclose(self) -> None:
        """Close the response and the owned client.  Idempotent."""
        if self._closed:
            return
        self._closed = True
        await self._close_response()
        await self._close_owned_client()

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self.iter_bytes()

    def _require_response(self) -> Any:
        if self._response is None:
            raise StreamClosedError(
                "HTTPStream is not open; use 'async with HTTPStream(...) as response'"
            )
        return self._response

    # ------------------------------------------------------------------ #
    # Body iteration
    # ------------------------------------------------------------------ #
    async def iter_bytes(self, *, chunk_size: int | None = None) -> AsyncIterator[bytes]:
        """Yield decoded response chunks as they arrive."""
        response = self._require_response()
        size = self._chunk_size if chunk_size is None else chunk_size
        async for chunk in response.aiter_bytes(size):
            yield chunk

    async def iter_text(self, encoding: str = "utf-8") -> AsyncIterator[str]:
        """Yield text chunks decoded with ``encoding``."""
        async for chunk in self.iter_bytes():
            yield chunk.decode(encoding, errors="replace")

    async def iter_lines(self) -> AsyncIterator[str]:
        """Yield response lines without their terminators."""
        response = self._require_response()
        async for line in response.aiter_lines():
            yield line

    async def read_all(self, *, max_bytes: int | None = None) -> bytes:
        """Read the whole body, honouring the configured byte limit."""
        limit = self._max_bytes if max_bytes is None else max_bytes
        chunks: list[bytes] = []
        total = 0
        async for chunk in self.iter_bytes():
            total += len(chunk)
            if limit is not None and total > limit:
                raise MessageTooLargeError(total, limit)
            chunks.append(chunk)
        return b"".join(chunks)

    def as_stream(self, *, config: Any = None) -> Stream[bytes]:
        """Expose the body as a stream of chunks.

        The stream owns the response and closes it when iteration ends.
        """
        http = self

        async def factory() -> AsyncIterator[bytes]:
            if http._response is None:
                await http.__aenter__()
            try:
                async for chunk in http.iter_bytes():
                    yield chunk
            finally:
                await http.aclose()

        return Stream(factory, config=config, replayable=False)

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return (
            f"HTTPStream(method={self._method!r}, url={self._url!r}, "
            f"open={self._response is not None})"
        )
