# Muffin-Kafka

**Muffin-Kafka** is an [Apache Kafka](https://kafka.apache.org) integration plugin for the
[Muffin](https://github.com/klen/muffin) web framework, built on top of
[`aiokafka`](https://github.com/aio-libs/aiokafka).

[![Tests Status](https://github.com/klen/muffin-kafka/workflows/tests/badge.svg)](https://github.com/klen/muffin-kafka/actions)
[![PYPI Version](https://img.shields.io/pypi/v/muffin-kafka)](https://pypi.org/project/muffin-kafka/)
[![Python Versions](https://img.shields.io/pypi/pyversions/muffin-kafka)](https://pypi.org/project/muffin-kafka/)

---

## Features

- Consumer pool with one `AIOKafkaConsumer` per topic, each consumed in its own asyncio task.
- Single-message (`getone()`) or batch (`getmany()`) reading, selected by `batch_size`.
- Handler registration with `@plugin.handle_topics(...)`, optional global error handler.
- Manual offset commits after successful processing, or aiokafka auto-commit.
- Producer with `send()` (JSON/bytes encoding, keys, partitions, headers).
- `kafka-listen` and `kafka-healthcheck` management commands.
- Built-in monitor task that logs per-partition offsets, lag and poll delay.

## Requirements

- Python ≥ 3.11 (CI runs 3.11–3.14; the package has no upper version bound).
- Muffin 3.x (developed and tested against `muffin` 3.0).
- A Kafka cluster or a local broker.

## Installation

```bash
pip install muffin-kafka
```

## Quickstart

```python
from muffin import Application
from muffin_kafka import Kafka

app = Application("example")

kafka = Kafka(
    app,
    bootstrap_servers="localhost:9092",
    group_id="example-workers",
    produce=True,
    listen=False,  # consumers are started by `kafka-listen` below
)

@kafka.handle_topics("events.user", "events.auth")
async def handle_event(message):
    print("Received:", message.value.decode())

@kafka.handle_error
async def on_error(exc):
    print("Kafka handler failed:", exc)
```

`Kafka` is an alias of `KafkaPlugin` (and so is `Plugin`). Consumers only need a
`group_id` when they commit offsets manually, which is the default; without one, the
first successful commit raises and stops the consumer task.

Run the consumers with the management command:

```bash
muffin example kafka-listen
```

`kafka-listen` starts the consumers in its own process, which is why the plugin uses
`listen=False` above. Set `listen=True` instead when the application process itself should
consume (then do not run the command in that process).

Send a message from the application:

```python
@kafka.handle_topics("events.user")
async def handle_event(message): ...

await kafka.send("events.user", {"action": "signup"}, key="user123")
```

## How it works

```
muffin example kafka-listen
        │
        ▼
KafkaPlugin.listen()
        │  one consumer per topic
        ▼
ConsumerPool ──► PoolRunner ──► one asyncio task per consumer
                    │            (SinglePoolRunner or BatchPoolRunner)
                    ▼
               ConsumerHandlers ──► your handlers (one call per message)
                    │
                    ▼
            offset commit after success
                    │
                    └──► ConsumerPoolLogger (optional monitor task)
```

The parts, in order:

| Step | What happens |
| ---- | ------------ |
| `kafka-listen` | Runs inside the application lifespan, then calls `KafkaPlugin.listen()`. |
| `listen()` | Picks the runner and calls `consumer_pool.init(*topics)`. |
| `ConsumerPool` | Holds one `AIOKafkaConsumer` per topic; `init()` creates only consumers for topics that are not covered yet. |
| `PoolRunner` | Starts the pool, creates one asyncio task per consumer, optionally one monitor task. |
| `SinglePoolRunner` / `BatchPoolRunner` | Fetch one record (`getone()`) or a batch (`getmany()`), pass each record to the handlers, commit offsets. |
| `ConsumerHandlers` | Looks up the handlers registered for `message.topic` and calls them in registration order. |
| Commit | With `enable_auto_commit=False` the runner commits after a successfully handled message or batch. |
| Monitor | `ConsumerPoolLogger` logs every partition every `monitor_interval` seconds. |

### One task per consumer, one consumer per topic

Every topic gets its own `AIOKafkaConsumer` and its own asyncio task. Topics therefore
make progress independently: a slow or failing handler on one topic does not block the
others. Within one topic the task is sequential, so messages are handled one at a time
in fetch order. Kafka only guarantees order inside a partition, not across partitions.

### What happens when a handler fails

The dispatch loop catches the exception, logs it with a traceback and calls the error
handler. The remaining handlers registered for that message still run. The record is
marked as failed, so the runner does not commit after it.

The consuming loop itself survives: the next record is processed normally. However, the
next successful commit sends the consumer's current position, which is already past the
failed record, so that record is not retried while the process keeps running. It is
retried only if the process stops before any later commit on the same partition.

### What `stop()` does

`runner.stop()` sets a stop flag, commits the consumer positions, stops the consumers and
only then cancels the consumer tasks. Because the commit happens before the cancel, a
message that is still being handled at shutdown can be committed before its handler
finishes. `plugin.shutdown()` always commits, even with `enable_auto_commit=True`.

## Offsets and errors

This is the part worth reading before production.

### Manual commits (`enable_auto_commit=False`, default)

The runners call `consumer.commit()` explicitly. Like any aiokafka commit, it stores the
consumer's **current position** for every assigned partition: the offset of the next
record to fetch, not the offset of the record that was just handled.

| Runner | When the commit happens |
| ------ | ----------------------- |
| `SinglePoolRunner` | After every record whose handlers all succeeded. |
| `BatchPoolRunner` | Once per batch, only if every record in the batch succeeded. |

Consequences:

- A handler exception skips that commit. The error handler is called and the loop
  continues, but the next success commits a position past the failed record. A poison
  message does not block the consumer; it is skipped from the next successful commit on
  that partition onward, not retried per message.
- Messages fetched and handled after the last successful commit are replayed after a
  process restart or a rebalance, so handlers must be idempotent (at-least-once at commit
  boundaries).
- A `CommitFailedError` means the group rebalanced. `safe_commit()` logs it and returns;
  the consumer rejoins and the uncommitted records are delivered again.
- A `group_id` is mandatory. `consumer.commit()` raises `IllegalOperation("Requires
  group_id")` when the consumer has no group, and that exception propagates out of the
  runner loop: the consumer task dies after the first successfully handled message.
- A topic without registered handlers counts as successfully handled (`ConsumerHandlers`
  returns `True` for an empty handler list), so those records are committed and dropped.

### Auto-commit (`enable_auto_commit=True`)

The plugin does not commit in the consume loop at all. aiokafka commits the consumer's
last fetched position every `auto_commit_interval_ms` (default 5000, not a plugin option;
pass it with `plugin.listen(auto_commit_interval_ms=1000)` if needed), regardless of what
your handlers did. A failing handler is therefore invisible to the commit logic: the
record is marked consumed anyway and is not retried. Use this mode only when losing a
failed message is acceptable or the handler has its own retry/outbox logic.

### Error handler

`@kafka.handle_error` registers one global handler (not per topic):

```python
@kafka.handle_error
async def on_error(exc):
    metrics.increment("kafka_handler_error")
```

- Called once for every handler exception, after the exception is logged.
- The exception is not re-raised, the consumer loop continues, and the remaining handlers
  for the same message still run.
- If the error handler itself raises, that exception escapes into the consumer loop. The
  consumer task crashes, the runner logs `Kafka task crashed: ...` and does not restart
  it; that topic stops being consumed until the process restarts.

### `auto_offset_reset`

`auto_offset_reset` decides where a consumer starts when its group has no committed offset
for a partition: a new `group_id`, an expired offset, or a consumer without a `group_id`.

- `"earliest"` (default): replay the topic from the beginning.
- `"latest"`: read only messages produced after the consumer joined.

Committed offsets always win over this setting.

### Several workers with the same `group_id`

Kafka assigns each partition of a topic to exactly one consumer in the group. Start two
processes with the same `group_id` and the partitions are split between them; with more
processes than partitions, the extra processes stay idle. Different `group_id` values are
different groups, and each group receives every partition. The plugin does not shard work
itself; it creates one consumer per topic per process and lets Kafka do the assignment.

## Batch mode

Set `batch_size` (config option or `--batch-size`) to read records with
`getmany(timeout_ms=100, max_records=batch_size)` instead of `getone()`:

```bash
muffin example kafka-listen --batch-size=100
```

`getmany()` returns `{TopicPartition: [records]}` and may wait up to 100 ms for data.
The runner unpacks that mapping and calls your handlers once per record: there is no list
argument in batch mode, so the same handler works in both modes.

```python
@kafka.handle_topics("events.user")
async def handle_event(message):  # still one ConsumerRecord per call
    await db.execute(INSERT, message.value)
```

Batching changes when offsets are committed, not the handler signature: the whole batch is
committed once at the end, and only if every record in it succeeded. One failed record
prevents the commit for the entire batch, but the next successful batch still commits a
position past it (see [Offsets and errors](#offsets-and-errors)). A `--batch-size=0` is
treated as "not set" and falls back to the `batch_size` config value.

`max_poll_records` is passed through to `AIOKafkaConsumer`, but `getone()` returns a single
record and `getmany()` is called with `max_records=batch_size`, so the option does not cap
what this plugin reads.

## Management commands

Both commands are registered by `plugin.setup()` unless `setup_commands=False`.

### `kafka-listen`

```bash
muffin example kafka-listen                            # all topics with handlers
muffin example kafka-listen events.user events.order   # only these topics
muffin example kafka-listen --group-id=workers-v2      # override the group for this run
muffin example kafka-listen --batch-size=100           # batch mode
muffin example kafka-listen --no-monitor               # disable the monitor task
```

| Option | Description |
| ------ | ----------- |
| `topics` | Topics to consume (positional, optional; defaults to topics with registered handlers). |
| `--group-id` | Override the configured `group_id` for this run. |
| `--batch-size` | Batch size; `0` means "use the `batch_size` config value". |
| `--no-monitor` | Do not start the monitor task. Monitoring is **on** by default for this command. |

The monitor interval comes from the `monitor_interval` config option; there is no
`--monitor-interval` CLI flag.

The command runs inside the application lifespan, so `plugin.startup()` executes first.
With the default `listen=True` that already starts the consumers. `listen()` is
idempotent, so the command reuses the running runner instead of starting the consumers a
second time; in that case the command options (`topics`, `--group-id`, `--batch-size`,
`--no-monitor`) are ignored, because the runner already exists. Set `listen=False` (for
example `KAFKA_LISTEN=false`) when the command should own the consumers and honor its
options.

If no topics are given and no handlers are registered, there is nothing to consume: the
command starts an empty runner and exits immediately.

### `kafka-healthcheck`

```bash
muffin example kafka-healthcheck                         # registered topics, max lag 1000
muffin example kafka-healthcheck events.user --max-lag=500
```

The command starts a temporary consumer pool when the plugin is not consuming in this
process (`init(topics)` then `start()`), runs the check and stops that pool with
`commit=False`. The plugin's own pool is left untouched, so the command can run
repeatedly. When the plugin is already consuming in this process, the running pool is
reused and left running.

The check reads the latest offset (`end_offsets`) and the committed offset of every
partition assigned to a started consumer and compares `end_offset - committed` with
`max_lag`. A missing committed offset counts as `0`, and the comparison is strict, so a
partition is unhealthy only when `lag > max_lag`. The command exits with `1` when at least
one partition is unhealthy and also when the check could not be performed at all: no
consumer has an assigned partition (nothing to inspect). A successful check exits with
`0`.

`ConsumerPoolHealthcheck.process()` returns whether anything was inspected, so code that
runs the check itself has to test both results:

```python
from muffin_kafka.consumers import ConsumerPoolHealthcheck

async def kafka_is_healthy(max_lag: int = 1000) -> bool:
    healthcheck = ConsumerPoolHealthcheck(kafka.consumer_pool, max_lag=max_lag)
    performed = await healthcheck.process()
    return performed and bool(healthcheck)
```

Keep in mind that the healthcheck consumer joins the configured `group_id`: running the
command from a separate process triggers a group rebalance and only inspects the
partitions assigned to that temporary consumer. Partitions handled by other group members
are not examined. A check where no consumer got an assignment at all (for example while a
rebalance is in progress) counts as failed, not as healthy.

## Monitoring

With monitoring enabled (`monitor=True` in config, or `kafka-listen` without
`--no-monitor`), the runner starts a `ConsumerPoolLogger` task. Every `monitor_interval`
seconds it logs one line per assigned partition:

```
[Monitor] events.user-0 | pos: 42 | committed: 40 | end: 55 | lag: 15 | poll_delay: 812ms
```

- `pos` — next offset the consumer will read.
- `committed` — last offset committed for the group.
- `end` — latest offset on the broker.
- `lag` — `end - committed`.
- `poll_delay` — milliseconds since the consumer last polled that partition.

`monitor_interval=0` (or any falsy value) disables the loop.

The interval is a plugin option, so for `kafka-listen` set it in config:

```bash
export MUFFIN_KAFKA_MONITOR_INTERVAL=30
```

There is no configuration hook for a custom monitor: `PoolRunner.start()` always creates
`ConsumerPoolLogger`. To collect your own metrics, subclass `ConsumerPoolMonitor` and run
the passes yourself.

```python
import asyncio

from muffin_kafka.consumers import ConsumerPoolMonitor

class LagGauge(ConsumerPoolMonitor):
    async def process_tp(self, consumer, tp, end_offset):
        committed = await consumer.committed(tp) or 0
        metrics.gauge("kafka_lag", end_offset - committed, topic=tp.topic, partition=tp.partition)

monitor = LagGauge(pool=kafka.consumer_pool)

async def monitor_loop():
    while True:
        await monitor.process()
        await asyncio.sleep(30)
```

Start `monitor_loop()` as a task after `kafka.listen()` (or from your own
`on_startup`), and cancel it on shutdown. `process()` returns `False` and does nothing
while the pool is not started, so a monitor created before startup simply reports that it
could not check. For a logger-style loop you can subclass `ConsumerPoolLogger` (it
implements `process_tp` and a `__call__` loop) and await the instance.

## Configuration

Options can be passed as keyword arguments to the plugin, through the application config
with the `KAFKA_` prefix, or through environment variables with the `MUFFIN_KAFKA_` prefix
(JSON is parsed for complex values). Options that the plugin does not know are ignored.

```python
kafka = Kafka(app, bootstrap_servers="localhost:9092", produce=True)
```

```python
# application config
"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
"KAFKA_PRODUCE": True,
```

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `bootstrap_servers` | `list[str]` | `["localhost:9092"]` | Broker addresses; a plain string works too. |
| `client_id` | `str` | `"muffin"` | Kafka client ID. |
| `request_timeout_ms` | `int` | `30000` | Request timeout in milliseconds. |
| `retry_backoff_ms` | `int` | `1000` | Retry backoff in milliseconds. |
| `security_protocol` | `str` | `"PLAINTEXT"` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`. |
| `sasl_mechanism` | `str` | `"PLAIN"` | SASL mechanism. |
| `sasl_plain_username` | `str` or `None` | `None` | SASL user. |
| `sasl_plain_password` | `str` or `None` | `None` | SASL password. |
| `ssl_cafile` | `str` or `None` | `None` | CA bundle; turned into an `ssl_context`. |
| `produce` | `bool` | `False` | Create and start the producer on startup. `send()` raises `PluginError` when disabled. |
| `listen` | `bool` | `True` | Start consumers for registered topics on startup. |
| `group_id` | `str` or `None` | `None` | Consumer group. Required for manual commits. |
| `enable_auto_commit` | `bool` | `False` | Let aiokafka commit positions every 5 s instead of committing after successful handling. |
| `auto_offset_reset` | `str` | `"earliest"` | Start position when the group has no committed offset. |
| `max_poll_records` | `int` or `None` | `None` | Passed to `AIOKafkaConsumer`; the runners pass their own limit, so it has no effect here. |
| `batch_size` | `int` or `None` | `None` | Enables `BatchPoolRunner` with this `getmany()` limit. |
| `monitor` | `bool` | `False` | Start the `ConsumerPoolLogger` task with the consumers. |
| `monitor_interval` | `int` | `60` | Seconds between monitor passes. |
| `setup_commands` | `bool` | `True` | Register `kafka-listen` and `kafka-healthcheck`. |

To pass extra aiokafka consumer options (for example `session_timeout_ms`,
`max_poll_interval_ms`, `auto_commit_interval_ms` or a `value_deserializer`), call
`plugin.listen(**params)` yourself; the parameters are forwarded to `AIOKafkaConsumer`.
`plugin.cfg` only exposes the options above.

## API

### Plugin

| Member | Description |
| ------ | ----------- |
| `kafka.cfg` | Plugin configuration (the table above). |
| `kafka.producer` | `AIOKafkaProducer`, created on startup when `produce=True`, otherwise `None`. |
| `kafka.handlers` | `ConsumerHandlers` registry filled by `handle_topics`/`handle_error`. |
| `kafka.consumer_pool` | `ConsumerPool` with one consumer per topic. |
| `kafka.runner` | Active `PoolRunner`, or `None` before `listen()`. |
| `kafka.setup(app, **options)` | Bind the app, register commands, configure the pool. |
| `kafka.startup()` / `kafka.shutdown()` | Plugin lifecycle hooks (called by Muffin). |
| `kafka.listen(*topics, monitor=None, batch_size=None, context=None, **params)` | Create consumers and start a runner; returns it and reuses the running runner on repeated calls. |
| `kafka.send(topic, value, key=None, **params)` | Publish a message; returns aiokafka's delivery future. |
| `kafka.handle_topics(*topics)` | Decorator registering a message handler. |
| `kafka.handle_error(fn)` | Decorator registering the global error handler. |
| `kafka.conftest()` | Context manager patching `start`/`stop` on producer and consumers for tests. |

`send()` encodes `str` as UTF-8, keeps `bytes` untouched and JSON-encodes everything else
(including `None`, so a tombstone needs `kafka.producer.send_and_wait(topic, None, key=...)`
directly). A truthy `str` key is UTF-8 encoded, and extra `params` (`partition`, `headers`,
`timestamp_ms`) go to `AIOKafkaProducer.send()`. Awaiting `send()` only queues the record;
await the returned future to observe delivery errors:

```python
future = await kafka.send("events.user", {"action": "signup"}, key="user123")
metadata = await future
```

### Consumer subsystem

Import from `muffin_kafka.consumers`:

| Class | Description |
| ----- | ----------- |
| `ConsumerPool` | One consumer per topic. Lifecycle: `setup(group_id, **params)` → `init(*topics, **params)` / `init_consumer(*topics, **params)` → `start()` → `stop(commit=True)`. `init()` skips topics that an existing consumer already subscribes to; `start()` is idempotent. |
| `SinglePoolRunner` | `getone()` loop, commit after each successful record. |
| `BatchPoolRunner` | `getmany()` loop, commit once per successful batch; handlers still get single records. |
| `ConsumerHandlers` | Topic → handlers mapping used by the runners. |
| `ConsumerPoolMonitor` | Base monitor: `process()` returns whether any assigned partition was inspected, via `process_consumer()` and `process_tp()`. |
| `ConsumerPoolLogger` | Monitor that logs offsets, lag and poll delay; `__call__(interval)` is the loop task. |
| `ConsumerPoolHealthcheck` | Monitor that sets `is_healthy = False` when `lag > max_lag`; `process()` returns `False` when nothing could be checked, so test both. |
| `safe_commit(consumer)` | `consumer.commit()` that ignores `CommitFailedError` after a rebalance. |

`PoolRunner` itself is the extension point for a different fetching strategy: subclass it
and implement `_run_consumer(consumer)`, then instantiate it with the pool and handlers
instead of calling `plugin.listen()`. `start(monitor, context)` accepts a monitor interval
and a callable returning an async context manager, which wraps each consumer loop (useful
for a per-consumer DB session).

## Limitations and troubleshooting

- `listen()` is idempotent: a second call returns the running runner and ignores its
  arguments. To consume different topics or switch batch mode, restart the process.
  `plugin.restart()` still cannot revive aiokafka consumers that were stopped (`start()`
  after `stop()` asserts inside aiokafka), so use a fresh process or a fresh
  `ConsumerPool`.
- `enable_auto_commit=False` without `group_id` crashes the consumer task on the first
  successful commit (`IllegalOperation`). Set a `group_id`, or switch auto-commit on.
- A failed record does not stop the consumer, but it is not retried per message: the next
  successful commit on that partition moves past it. Treat handler failures as dropped
  unless the process stops first.
- A crashing consumer task is logged by `PoolRunner` and never restarted. Other topics
  keep consuming; `kafka-listen` exits when the runner's tasks finish or raise.
- `stop()` commits before cancelling the tasks. A handler still running at shutdown may
  have its offset committed, so in-flight work can be lost on a graceful stop.
- `kafka-healthcheck` joins the configured consumer group, so it triggers a rebalance and
  only inspects the partitions assigned to that temporary consumer. A pool without any
  assigned partition is reported as a failed check (exit code 1).
- The built-in monitor is the only configurable one; custom monitors must be instantiated
  and scheduled manually.
- `max_poll_records` does not limit reads in this plugin; use `batch_size`.
- Messages for a listened topic without handlers are committed and silently discarded.
- With `produce=False` (default) `send()` raises `PluginError`.

## Bug Tracker

Found a bug or have a feature request?
Please open an issue at:
[https://github.com/klen/muffin-kafka/issues](https://github.com/klen/muffin-kafka/issues)

---

## Contributing

Pull requests are welcome!
Development happens here:
[https://github.com/klen/muffin-kafka](https://github.com/klen/muffin-kafka)

---

## License

**MIT** – See [LICENSE](./LICENSE) for full details.
