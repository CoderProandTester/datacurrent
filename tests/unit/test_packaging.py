from __future__ import annotations

import re
import subprocess
import sys
import tomllib
from pathlib import Path

import datacurrent

ROOT = Path(__file__).resolve().parents[2]


def test_version_is_semantic() -> None:
    assert re.fullmatch(r"\d+\.\d+\.\d+", datacurrent.__version__)
    assert datacurrent.VERSION == tuple(int(part) for part in datacurrent.__version__.split("."))


def test_py_typed_marker_exists() -> None:
    assert (ROOT / "datacurrent" / "py.typed").is_file()


def test_pyproject_metadata() -> None:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = data["project"]
    assert project["name"] == "datacurrent"
    assert project["requires-python"] == ">=3.11"
    assert len(project["keywords"]) >= 15
    assert "Development Status :: 5 - Production/Stable" in project["classifiers"]
    dependencies = " ".join(project["dependencies"])
    assert "httpx" in dependencies
    assert "websockets" in dependencies
    for extra in ("fastapi", "starlette", "all", "dev"):
        assert extra in project["optional-dependencies"]


def test_importing_the_package_stays_lazy() -> None:
    code = (
        "import sys, datacurrent\n"
        "loaded = [name for name in ('httpx', 'websockets', 'fastapi', 'starlette') "
        "if name in sys.modules]\n"
        "print(loaded)\n"
        "assert not loaded, loaded\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert "[]" in result.stdout


def test_public_api_is_exported() -> None:
    for name in (
        "Stream",
        "Channel",
        "EventBus",
        "BoundedQueue",
        "RetryPolicy",
        "SSEStream",
        "HTTPStream",
        "WebSocketStream",
        "Metrics",
        "DataCurrentError",
    ):
        assert name in datacurrent.__all__
        assert hasattr(datacurrent, name)


def test_integrations_import_without_frameworks_installed() -> None:
    import datacurrent.integrations as integrations

    assert callable(integrations.sse_response)
    assert callable(integrations.streaming_response)
