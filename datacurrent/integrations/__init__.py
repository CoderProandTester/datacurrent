"""Framework integrations.

The helpers here import FastAPI/Starlette lazily, so ``import datacurrent`` and
this package work in applications that do not use a web framework.
"""

from __future__ import annotations

from .fastapi import sse_response, websocket_messages, websocket_stream
from .starlette import StreamLifespan, streaming_response

__all__ = [
    "StreamLifespan",
    "sse_response",
    "streaming_response",
    "websocket_messages",
    "websocket_stream",
]
