from __future__ import annotations

import asyncio
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES_DIR = ROOT / "examples"
if str(EXAMPLES_DIR) not in sys.path:
    sys.path.insert(0, str(EXAMPLES_DIR))
EXAMPLES = sorted(
    path for path in (ROOT / "examples").glob("*.py") if not path.name.startswith("_")
)


def _load(path: Path):
    spec = importlib.util.spec_from_file_location(f"datacurrent_example_{path.stem}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.examples
@pytest.mark.parametrize("path", EXAMPLES, ids=lambda path: path.stem)
async def test_example_runs(path: Path) -> None:
    module = _load(path)
    assert hasattr(module, "main"), f"{path.name} must define async main()"
    await asyncio.wait_for(module.main(), timeout=30)
