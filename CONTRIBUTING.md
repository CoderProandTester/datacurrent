# Contributing to DataCurrent

Thanks for helping improve DataCurrent. This guide covers setup, the checks your
change must pass, and how releases are published.

- Repository: <https://github.com/CoderProandTester/datacurrent>
- Issues: <https://github.com/CoderProandTester/datacurrent/issues>
- PyPI: <https://pypi.org/project/datacurrent/>

## Development setup

Python 3.11 or newer is required.

```bash
git clone https://github.com/CoderProandTester/datacurrent.git
cd datacurrent
python -m venv .venv
```

Activate the environment, then install the package with the development extras:

```bash
# Windows (Git Bash)
source .venv/Scripts/activate
# macOS / Linux
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

`dev` pulls in `pytest`, `pytest-asyncio`, `pytest-cov`, `hypothesis`, `build`,
`twine`, `ruff` and `mypy`.

## Before you change anything

```bash
python scripts/dev.py check    # ruff + mypy
python scripts/dev.py test     # full test suite
```

`scripts/dev.py` runs every command with the interpreter that launched it, so it
always uses the active virtual environment. `python scripts/dev.py --help`
lists all tasks.

## Layout

| Path | Contents |
|---|---|
| `datacurrent/core/` | `Stream`, operators, pipelines, terminals |
| `datacurrent/buffering/` | `BoundedQueue`, backpressure policies |
| `datacurrent/concurrency/` | `ConcurrencyLimiter`, task tracking, `gather_limited` |
| `datacurrent/reliability/` | retries, timeouts |
| `datacurrent/transports/` | HTTP, SSE, WebSocket and the transport base |
| `datacurrent/events/` | `Channel`, `EventBus`, routing |
| `datacurrent/integrations/` | FastAPI and Starlette helpers |
| `datacurrent/observability/` | metrics, stats, structured logging |
| `docs/` | guides and the API reference |
| `examples/` | sixteen runnable, test-covered scripts |
| `tests/` | unit, property-based, stress and packaging tests |
| `benchmarks/` | reproducible micro-benchmarks |

## Making a change

1. Keep the public API in `datacurrent/__init__.py` and `__all__` in sync when
   you add or remove an export.
2. Preserve the existing guarantees: laziness, single consumption, bounded
   buffers that block by default, counted drops, and cancellation-safe cleanup.
3. New behaviour needs tests. The project enforces **100% line and branch
   coverage**, so aim for that before you open a pull request.
4. Add an example when you add a user-facing feature, and list it in the README
   examples table. Examples must define `async def main()` and run in under 30
   seconds; `tests/unit/test_examples.py` executes every file that does not start
   with `_`.
5. Update `CHANGELOG.md` under an "Unreleased" or new version heading. The format
   follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions
   follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Checks

```bash
python scripts/dev.py lint                  # ruff check .
python scripts/dev.py fmt                   # ruff format .
python scripts/dev.py typecheck             # mypy
python scripts/dev.py cov                   # pytest with coverage (must be 100%)
python scripts/dev.py examples              # just the example scripts
python scripts/dev.py build                 # sdist + wheel
python scripts/dev.py check-dist            # twine check dist/*
```

A change is ready when `lint`, `typecheck` and `cov` all pass. Please do not
weaken assertions, skip tests or add suppressions to get a green run; fix the
cause instead.

## Style

- `ruff` with a 100-character line length, targeting Python 3.11 syntax.
- Full type annotations on public functions; the package ships `py.typed`.
- Async-first: no blocking I/O on the event loop.
- Prefer editing existing modules over adding new ones.
- Comments explain why, not what.

## Reporting a bug

Open an issue at <https://github.com/CoderProandTester/datacurrent/issues> with
the version (`python -c "import datacurrent; print(datacurrent.__version__)"`),
your Python version and platform, a minimal reproduction, and what you expected
instead. Security issues should be reported privately rather than in a public
issue.

## Release process

Versions are read from `datacurrent/_version.py`; that file is the single source
of truth. Releases are published by GitHub Actions with PyPI trusted publishing,
so the upload carries a signed provenance attestation.

```bash
# 1. Bump the version and document it
#    datacurrent/_version.py -> __version__ = "1.1.4"
#    CHANGELOG.md -> new heading with the date

# 2. Verify everything
python scripts/dev.py release      # lint, mypy, tests, build, twine check

# 3. Publish
git commit -am "Release 1.1.4"
git tag v1.1.4
git push origin main --tags        # runs .github/workflows/release.yml
```

`.github/workflows/release.yml` runs on a published GitHub release and on any
`v*` tag push. Its `publish` job uses the `release` environment and requests
`id-token: write`, which is what PyPI trusted publishing needs to mint the
upload credential and attach the provenance attestation. The PyPI publisher is
configured with owner `CoderProandTester`, repository `datacurrent`, workflow
`release.yml` and environment `release`.

Set `TWINE_USERNAME=__token__` and `TWINE_PASSWORD=<PyPI API token>` (or use
`~/.pypirc`) for the manual path. PyPI rejects re-uploads of an existing version,
so a failed upload always needs a new version number.

## License

By contributing you agree that your contributions are licensed under the MIT
license in `LICENSE`.
