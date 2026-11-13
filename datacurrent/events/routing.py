"""Topic matching for the event bus.

Topics are dot-separated strings such as ``user.created``.  Patterns support
exact segments, ``*`` for exactly one segment and a trailing ``**`` (or ``#``)
for any remaining segments.  ``*`` alone matches every topic.
"""

from __future__ import annotations

from ..exceptions import ConfigurationError

__all__ = ["matches", "validate_pattern", "validate_topic"]


def _validate(value: str, *, name: str) -> str:
    if not isinstance(value, str):
        raise ConfigurationError(f"{name} must be a string, got {type(value).__name__}")
    if not value or value.strip() != value:
        raise ConfigurationError(f"{name} must be non-empty and have no surrounding whitespace")
    return value


def validate_topic(topic: str) -> str:
    """Validate an event topic."""
    return _validate(topic, name="topic")


def validate_pattern(pattern: str) -> str:
    """Validate a subscription pattern."""
    return _validate(pattern, name="pattern")


def matches(pattern: str, topic: str) -> bool:
    """Return whether ``topic`` matches the subscription ``pattern``.

    Examples:
        >>> matches("user.created", "user.created")
        True
        >>> matches("user.*", "user.created")
        True
        >>> matches("user.*", "user.created.v2")
        False
        >>> matches("user.**", "user.created.v2")
        True
        >>> matches("*", "anything.at.all")
        True
    """
    if pattern in ("*", "#", "**"):
        return True
    pattern_parts = pattern.split(".")
    topic_parts = topic.split(".")
    for index, part in enumerate(pattern_parts):
        if part in ("**", "#"):
            return index == len(pattern_parts) - 1
        if index >= len(topic_parts):
            return False
        if part != "*" and part != topic_parts[index]:
            return False
    return len(pattern_parts) == len(topic_parts)
