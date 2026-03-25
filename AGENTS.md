# AGENTS.md

Guidance for coding agents working in `muffin-kafka`.

## Project Snapshot

- Language: Python
- Packaging: UV (`pyproject.toml`)
- Runtime target: Python 3.10+
- Main package: `muffin_kafka`
- Core modules: `muffin_kafka/plugin.py`, `muffin_kafka/cluster.py`
- Tests: `tests/`
- CI entrypoint: `.github/workflows/tests.yml`

## Environment Setup

- Optional bootstrap via Make: `make .venv`

## Lint / Test Commands

### Lint and Formatting

- Ruff check package: `uv run ruff`
- Ruff autofix/format package: `uv run ruff check --fix`
- Type check: `make types`
- Run all hooks locally: `uv run pre-commit run --all-files`

### Tests

- Run all tests: `make t`
- Verbose mode from project defaults is enabled via pytest config (`-lxsv`)

### Run a Single Test (important)

- Single file: `uv run pytest tests/test_common.py`
- Single test function: `uv run pytest tests/test_common.py::test_base`
- Filter by test name pattern: `uv run pytest -k consume_decorator`

## CI and Hook Expectations

- CI runs on pushes/PRs to `main` and `develop`.
- Python versions in CI matrix: 3.10, 3.11, 3.12, 3.13, 3.14 (latest stable).

## Commit Message Convention

- Commit messages are validated by `qoomon/git-conventional-commits`.
- Allowed types: `build`, `chore`, `docs`, `feat`, `fix`, `merge`, `ops`, `perf`, `refactor`,
  `style`, `test`.
- Keep commit titles conventional and scoped when useful.

## Code Style Guidelines

### Imports

- Prefer absolute imports.
- Import ordering should follow Ruff/isort defaults:
  - standard library
  - third-party
  - local package
- Keep one import per line when it improves readability.
- Remove unused imports; Ruff is strict (`select = ["ALL"]`).

### Formatting

- Use Ruff for formatting.
- Max line length: 100.

### Typing

- Type hints are expected for new/modified code.
- Use built-in generics (`list[str]`, `dict[str, int]`) where possible.
- Prefer `X | None` syntax in new code (project has a mixed legacy style).

### Naming

- Modules/files: `snake_case`.
- Functions/variables: `snake_case`.
- Classes: `PascalCase`.
- Constants/default maps: `UPPER_CASE` or clear class attributes.
- Type aliases may use `T` prefix (existing pattern: `TCallable`, `TErrCallable`).

### Async and Concurrency

- Prefer `async def` boundaries for I/O paths.
- Use `asyncio.gather` for coordinated startup/shutdown operations.
- Track background tasks and cancel on shutdown.
- Attach done callbacks for task error logging when spawning background tasks.

### Error Handling and Logging

- Raise `PluginError` for invalid plugin state or misuse.
- Use guard clauses for invalid preconditions.
- Log exceptions with context (`logger.exception(...)`) before fallback/continue paths.
- Avoid silent `except` blocks.
- If catching broad exceptions in long-running loops, include explicit logging.

### Kafka-Specific Patterns

- Respect producer/consumer feature flags (`produce`, `listen`, `monitor`).
- Encode keys and values consistently before send.
- Keep handler registration explicit (`handle_topics`, `consume`).
- Healthcheck logic should fail closed (return `False` on uncertainty).

### Testing Conventions

- Use `pytest` fixtures from `tests/conftest.py`.
- Keep tests small and behavior-oriented.
- For async tests, rely on configured async test support (pytest-aio + `aiolib` fixture).
- Prefer explicit test names: `test_<unit>_<behavior>`.

## Agent Working Rules for This Repo

- Make minimal, focused changes.
- Do not refactor unrelated files in the same patch.
- Preserve public API aliases (`Kafka`, `Plugin`) unless explicitly requested.
- Update tests alongside behavior changes.
- Run relevant lint/type/test commands before finishing.
