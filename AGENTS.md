# AGENTS.md

- Language: Python, Packaging: UV (`pyproject.toml`)
- Runtime: Python 3.10+, Main package: `muffin_kafka`
- Tests: `tests/`, CI: `.github/workflows/tests.yml`

## Commands

```bash
make t           # run tests
make types       # type check
uv run ruff      # lint
uv run ruff check --fix  # auto-fix
```

- CI matrix: 3.10–3.14.
    Runs on pushes/PRs to `main` and `develop`.

## Code Style

- Ruff formats, max line 100, `select = ["ALL"]`.
- Type hints expected: built-in generics (`list[str]`), `X | None`.
- One import per line; ordering: stdlib, third-party, local.
- Prefer `async def` for I/O; use `asyncio.gather` for startup/shutdown.
- Raise `PluginError` for invalid plugin state.
    Avoid silent `except`.
- Log exceptions with `logger.exception(...)` before continuing.

## Kafka Patterns

- Feature flags: `produce`, `listen`, `monitor`.
- Encode keys/values before `send`.
    Keep handler registration explicit.
- Healthcheck fails closed (return `False` on uncertainty).

## Testing

- Use `pytest` fixtures from `tests/conftest.py`.
- Async tests via `pytest-aio`.
    Name: `test_<unit>_<behavior>`.
- Run lint/type/test before finishing.

## Rules

- Minimal, focused changes.
    Preserve public API aliases (`Kafka`, `Plugin`).
- Update tests alongside behavior changes.
