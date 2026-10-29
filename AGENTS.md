# AGENTS.md

- Language: Python, Packaging: UV (`pyproject.toml`)
- Runtime: Python 3.11+; CI runs 3.11–3.14 (`.github/workflows/tests.yml`), the package has no upper bound.
- Tests: `tests/`, CI: `.github/workflows/tests.yml`

## Commands

```bash
make t           # run tests
make types       # type check (pyrefly)
uv run ruff      # lint
uv run ruff check --fix  # auto-fix
uv run pytest    # run all tests
```

- CI matrix: 3.11–3.14.
    Runs on pushes/PRs to `main` and `develop`.
- README.md is the only documentation (no mkdocs site).

## Code Style

- Ruff formats, max line 100, `select = ["ALL"]`.
- Type hints expected: built-in generics (`list[str]`), `X | None`.
- One import per line; ordering: stdlib, third-party, local.
- Prefer `async def` for I/O; use `asyncio.TaskGroup` for startup/shutdown.
- Raise `PluginError` for invalid plugin state.
    Avoid silent `except`.
- Log exceptions with `logger.exception(...)` before continuing.

## Kafka Patterns

- Feature flags: `produce` (default `False`), `listen` (default `True`), `monitor`
  (default `False`).
- Encode keys/values before `send`.
    Keep handler registration explicit.
- Consumer pool: one `AIOKafkaConsumer` per topic, one asyncio task per consumer.
    `SinglePoolRunner` uses `getone()`, `BatchPoolRunner` uses `getmany()` and is selected by a
    truthy `batch_size`.
- Offsets: with `enable_auto_commit=False` the runner commits only after successful handling,
    and `commit()` stores the current position. A later success therefore commits past a failed
    record; see the README for the exact semantics.
- A pool/runner start is idempotent: `ConsumerPool.start()` does nothing when already started and
    `KafkaPlugin.listen()` returns the running runner. `stop()` then `start()` again still needs fresh
    aiokafka consumers (restart the process or build a new pool).
- Healthcheck fails closed: `process()` returns `False` when nothing could be inspected (pool not
    started or no assigned partitions) and the command exits `1`.
- Monitors: only `ConsumerPoolLogger` is wired into the runner; custom monitors are run by the user.

## Testing

- Use `pytest` fixtures from `tests/conftest.py`.
- Async tests via `pytest-aio`.
    Name: `test_<unit>_<behavior>`.
- Run lint/type/test before finishing.

## Rules

- Minimal, focused changes.
    Preserve public API aliases (`Kafka`, `Plugin`).
- Update tests alongside behavior changes.
