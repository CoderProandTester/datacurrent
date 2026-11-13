"""Cross-platform development task runner.

Usage::

    python scripts/dev.py test
    python scripts/dev.py check
    python scripts/dev.py build

Every command runs with the interpreter that launched this script, so it always
uses the active virtual environment.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable

COMMANDS: dict[str, list[list[str]]] = {
    "test": [[PYTHON, "-m", "pytest"]],
    "cov": [[PYTHON, "-m", "pytest", "--cov=datacurrent", "--cov-report=term-missing"]],
    "examples": [[PYTHON, "-m", "pytest", "tests/unit/test_examples.py", "-q"]],
    "lint": [[PYTHON, "-m", "ruff", "check", "."]],
    "fmt": [[PYTHON, "-m", "ruff", "format", "."]],
    "typecheck": [[PYTHON, "-m", "mypy"]],
    "bench": [[PYTHON, "benchmarks/bench_core.py"]],
    "build": [[PYTHON, "-m", "build"]],
    "check-dist": [[PYTHON, "-m", "twine", "check", "dist/*"]],
    "release": [
        [PYTHON, "-m", "ruff", "check", "."],
        [PYTHON, "-m", "mypy"],
        [PYTHON, "-m", "pytest"],
        [PYTHON, "-m", "build"],
        [PYTHON, "-m", "twine", "check", "dist/*"],
    ],
}


def run(arguments: list[str]) -> None:
    print("$ " + " ".join(arguments))
    result = subprocess.run(arguments, cwd=ROOT)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] in {"-h", "--help"}:
        print("Available commands:")
        for name in COMMANDS:
            print(f"  {name}")
        raise SystemExit(0)

    command = sys.argv[1]
    if command == "all":
        for name in ("lint", "typecheck", "test", "build", "check-dist"):
            run(COMMANDS[name][0])
        return
    if command not in COMMANDS:
        print(f"unknown command {command!r}; try --help", file=sys.stderr)
        raise SystemExit(2)

    for step in COMMANDS[command]:
        run(step)


if __name__ == "__main__":
    main()
