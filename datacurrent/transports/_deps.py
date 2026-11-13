"""Lazy imports for optional transport dependencies."""

from __future__ import annotations

import importlib
from types import ModuleType

from ..exceptions import OptionalDependencyError

__all__ = ["require_module"]


def require_module(
    name: str,
    *,
    extra: str | None = None,
    purpose: str,
) -> ModuleType:
    """Import a dependency or raise a helpful install error."""
    try:
        return importlib.import_module(name)
    except ImportError as error:  # pragma: no cover - depends on environment
        raise OptionalDependencyError(name.split(".")[0], extra=extra, purpose=purpose) from error
