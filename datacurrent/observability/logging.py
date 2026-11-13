"""Logging helpers.

DataCurrent attaches a :class:`logging.NullHandler` so importing the library
never changes application logging.  URLs and headers are redacted before they
are handed to the logging module: tokens in query strings, user info in URLs,
``Authorization`` and cookie headers never reach log records.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any
from urllib.parse import parse_qsl, urlsplit

__all__ = [
    "LOGGER_NAME",
    "get_logger",
    "redact_headers",
    "redact_secret",
    "redact_url",
]

LOGGER_NAME = "datacurrent"

_SENSITIVE_FRAGMENTS = (
    "token",
    "secret",
    "password",
    "passwd",
    "api_key",
    "apikey",
    "access_key",
    "private_key",
    "credential",
    "signature",
    "session",
    "authorization",
    "auth",
    "code",
    "key",
)
_SENSITIVE_HEADERS = {
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "x-api-key",
    "x-auth-token",
    "api-key",
    "sec-websocket-key",
    "sec-websocket-protocol",
    "x-amz-security-token",
}

_root_logger = logging.getLogger(LOGGER_NAME)
if not any(isinstance(handler, logging.NullHandler) for handler in _root_logger.handlers):
    _root_logger.addHandler(logging.NullHandler())


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a namespaced logger. Never configure the logging system here."""
    if not name:
        return _root_logger
    if name.startswith(LOGGER_NAME):
        return logging.getLogger(name)
    return logging.getLogger(f"{LOGGER_NAME}.{name}")


def _is_sensitive(name: str) -> bool:
    lowered = name.lower()
    return any(fragment in lowered for fragment in _SENSITIVE_FRAGMENTS)


def redact_url(url: str) -> str:
    """Return ``url`` with user info and sensitive query values replaced by ``***``.

    The structure (scheme, host, port, path, non-sensitive parameters) is kept so
    the value is still useful in logs.
    """
    if not isinstance(url, str):  # defensive: never log an object's repr
        return "<redacted-url>"
    try:
        parts = urlsplit(url)
    except ValueError:
        return "<invalid-url>"

    netloc = parts.netloc
    if "@" in netloc:
        host = netloc.rsplit("@", 1)[1]
        netloc = f"***@{host}"

    query = parts.query
    if query:
        rendered: list[str] = []
        try:
            pairs = parse_qsl(query, keep_blank_values=True)
        except ValueError:
            pairs = []
        if pairs:
            for key, value in pairs:
                rendered.append(f"{key}=***" if _is_sensitive(key) else f"{key}={value}")
            query = "&".join(rendered)
        else:
            query = "***"

    return parts._replace(netloc=netloc, query=query).geturl()


def redact_headers(headers: Mapping[str, Any] | None) -> dict[str, str]:
    """Return a copy of ``headers`` with sensitive values replaced by ``***``."""
    if not headers:
        return {}
    redacted: dict[str, str] = {}
    for key, value in headers.items():
        name = str(key)
        redacted[name] = "***" if name.lower() in _SENSITIVE_HEADERS else str(value)
    return redacted


def redact_secret(value: Any) -> str:
    """Render a secret for logs without revealing its contents."""
    if value is None:
        return "<none>"
    text = str(value)
    if not text:
        return "<empty>"
    return f"*** ({len(text)} chars)"
