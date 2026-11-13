# Changelog

All notable changes to DataCurrent are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.2] — 2026-11-13

Coverage and packaging release. The public API is unchanged.

### Added

- CI status badge in the README, alongside the PyPI and GitHub badges.

### Fixed

- `take(n)` no longer trips a false partial-branch report under coverage on
  Python 3.11-3.13. It now uses the explicit `__anext__` loop that `timeout()`
  already used, which removed the last blocker for the 100% coverage gate.

### Tests

- An exhausted `take()` (source shorter than the limit, including an empty
  source), a `recover()` that never sees an error, and an event-bus worker that
  exits when its queue closes.

## [1.0.2] — 2026-11-13

Automation and packaging release. The public API is unchanged.

### Added

- GitHub Actions CI (`.github/workflows/ci.yml`) running `ruff`, `mypy` and the
  coverage-enforcing test suite on Python 3.11 through 3.14 for every push and
  pull request.

### Fixed

- The source distribution ships `CONTRIBUTING.md`; the manifest entry was added
  after the 1.0.1 build.
- The release workflow now skips the PyPI upload when the version is already
  published, instead of failing on a duplicate file.

## [1.0.1] — 2026-11-13

Documentation, metadata and packaging release. The public API is unchanged.

### Added

- `CONTRIBUTING.md`: development setup, the check suite, and the release
  process, including PyPI trusted publishing.
- A "Documented examples" section in the README that maps each of the sixteen
  runnable scripts to the feature it demonstrates.
- `[project.urls]` in `pyproject.toml`: homepage and repository (GitHub), issues,
  changelog and PyPI.
- A GitHub Actions release workflow that publishes to PyPI over OIDC, so
  uploads carry a signed provenance attestation.

### Fixed

- The FastAPI example documented `uvicorn examples.10_fastapi_integration:app`,
  which is not a runnable module path. It now shows the verified commands.

### Changed

- README links to the GitHub repository, the PyPI homepage and the examples
  directory.
- The "not published to PyPI" note was removed; the package is on PyPI and this
  is the current release.

## [1.0.0] — 2026-11-08

First stable release. Built and verified locally; not published to PyPI.

### Added

- `Stream` engine: construction from iterables, async iterables, factories,
  queues, values and other streams; single-consumer semantics with explicit
  `StreamConsumedError`; replayable sources for retries.
- Operators: `map` (bounded, ordered concurrency), `filter`, `flat_map`,
  `take`, `skip`, `batch`, `buffer`, `distinct`, `enumerate`, `window`,
  `debounce`, `throttle`, `zip`, `merge`, `concat`, `timeout`, `retry`,
  `recover`, `tap`.
- Terminal operations: `collect`, `count`, `drain`, `first`, `last`, `reduce`.
- Bounded `BoundedQueue` with `block`, `drop_newest`, `drop_oldest` and `error`
  overflow policies, high-watermark statistics and producer suspension.
- `ConcurrencyLimiter`, `TaskTracker`, `gather_limited` and `consume()` with
  bounded concurrency and cancellation-safe cleanup.
- `RetryPolicy` with exponential backoff, jitter, attempt limits and exception
  predicates; `retry_async` helper; `with_timeout`.
- In-memory `Channel` broadcast with per-subscriber bounded queues and drop
  accounting.
- `EventBus` with `Event` envelopes, wildcard topic routing, handler isolation,
  stream subscriptions, draining shutdown and statistics.
- Transports: `SSEStream` (WHATWG parser, `Last-Event-ID`, reconnection),
  `HTTPStream` (chunked iteration, line iteration, byte limits) and
  `WebSocketStream` (text/binary/JSON, heartbeats, size limits, reconnection),
  backed by `httpx` and `websockets`, which install with the package.
- `StreamTransport` extension base for custom protocols.
- FastAPI / Starlette helpers: SSE responses, streaming responses, WebSocket
  message streams and lifespan cleanup.
- Observability: `Metrics`, `Counter`, per-stream `StreamStats`, structured
  logging with credential redaction.
- Complete exception hierarchy rooted at `DataCurrentError`.
- 279 tests: unit, property-based, stress, networking with local servers,
  packaging and executable examples (100% line and branch coverage).
- 16 runnable examples, full documentation, reproducible benchmarks and a
  cross-platform developer task runner.

### Security

- TLS verification is not disabled anywhere by default.
- Credentials in URLs, headers and secrets are redacted from logs and errors.
- Message-size and body-size limits are configurable and enforced.
- Resource cleanup is guaranteed on cancellation and early exit.

### Known limitations

- No durable broker adapters (Redis, Kafka, RabbitMQ, NATS) in this release.
- No OpenTelemetry exporter; metrics are exposed as snapshots only.
- asyncio only; AnyIO/Trio backends are not supported.
- The project name has not been trademark-cleared and the package is not on
  PyPI.
