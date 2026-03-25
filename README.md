# Muffin-Kafka

**Muffin-Kafka** is an [Apache Kafka](https://kafka.apache.org) integration plugin
for the [Muffin](https://klen.github.io/muffin) web framework, built on top of `aiokafka`.

[![Tests Status](https://github.com/klen/muffin-kafka/workflows/tests/badge.svg)](https://github.com/klen/muffin-kafka/actions)
[![PYPI Version](https://img.shields.io/pypi/v/muffin-kafka)](https://pypi.org/project/muffin-kafka/)
[![Python Versions](https://img.shields.io/pypi/pyversions/muffin-kafka)](https://pypi.org/project/muffin-kafka/)

---

## 🚀 Features

- **Async Kafka integration** using `aiokafka`
- **Per-topic task model** — each topic is consumed in an isolated asyncio task
- **Simple handler registration** using `@plugin.handle_topics(...)`
- **Manual or auto-commit support**, custom group IDs
- **Producer support** (`send` / `send_and_wait`)
- **Built-in monitoring** with offsets, lag, and poll delay
- **Healthcheck support** for liveness probes and observability
- Optional error handler via `@plugin.handle_error(...)`

---

## ✅ Requirements

- Python ≥ 3.10
- Muffin ≥ 0.71
- Kafka cluster or broker (local or cloud)

---

## 📦 Installation

```bash
pip install muffin-kafka
```

## ⚙️ Usage

```python
    from muffin import Application
    from muffin_kafka import Kafka

    app = Application("example")

    # Initialize the plugin with config options
    kafka = Kafka(app, bootstrap_servers="localhost:9092", produce=True, listen=True)
```

### 🧩 Registering Handlers

Use `@kafka.handle_topics(...)` to register a handler for specific Kafka topics:

```python
    @kafka.handle_topics("events.user", "events.auth")
    async def handle_event(message):
        data = message.value.decode()
        print("Received:", data)
```

You can also register a global error handler:

```python
    @kafka.handle_error
    async def on_error(exc):
        print("Kafka error:", exc)
```

### 📤 Sending Messages

You can send messages to Kafka topics using the `send` or `send_and_wait` methods:

```python
    # Send a message without waiting for acknowledgment
    await kafka.send("events.user", {"action": "signup"}, key="user123")

    # Or wait for broker acknowledgment
    result = await kafka.send_and_wait("events.user", {"action": "login"})
```

### 🔄 Healthcheck

You can monitor consumer health by checking lag across partitions:

```python
    # Check if Kafka lag is within acceptable limits
    ok = await kafka.healthcheck(max_lag=1000)
    if not ok:
        raise RuntimeError("Kafka lag too high")
```

## 📊 Monitoring

If monitor=True is passed, the plugin will log:

- Committed offsets
- Latest offsets
- Poll timestamps
- Per-partition lag and delay

This data can be extended for Prometheus/Grafana metrics or alerting.

## ⚙️ Configuration Options

You can pass configuration options either as keyword arguments to the plugin:

```python
kafka = Kafka(app, bootstrap_servers="localhost:9092", produce=True)
```

Or set them via Muffin's config system (e.g. `.env`, YAML):

```python
"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
"KAFKA_PRODUCE": True,
```

### Supported Options

| Option                | Type   | Default            | Description                                 |
| --------------------- | ------ | ------------------ | ------------------------------------------- |
| `bootstrap_servers`   | `str`  | `"localhost:9092"` | Kafka broker connection string              |
| `group_id`            | `str`  | `None`             | Kafka consumer group ID                     |
| `client_id`           | `str`  | `"muffin"`         | Kafka client ID                             |
| `produce`             | `bool` | `False`            | Enable Kafka producer                       |
| `listen`              | `bool` | `True`             | Enable consumers (message listening)        |
| `monitor`             | `bool` | `False`            | Enable internal consumer monitor            |
| `monitor_interval`    | `int`  | `60`               | Monitor frequency in seconds                |
| `auto_offset_reset`   | `str`  | `"earliest"`       | Where to start if no committed offset       |
| `enable_auto_commit`  | `bool` | `False`            | Automatically commit offsets                |
| `max_poll_records`    | `int`  | `None`             | Max records to poll in one batch            |
| `request_timeout_ms`  | `int`  | `30000`            | Request timeout                             |
| `retry_backoff_ms`    | `int`  | `1000`             | Retry interval on failure                   |
| `security_protocol`   | `str`  | `"PLAINTEXT"`      | Kafka protocol (`SSL`, `SASL_PLAINTEXT`, …) |
| `sasl_mechanism`      | `str`  | `"PLAIN"`          | SASL auth mechanism                         |
| `sasl_plain_username` | `str`  | `None`             | SASL auth user                              |
| `sasl_plain_password` | `str`  | `None`             | SASL auth password                          |
| `ssl_cafile`          | `str`  | `None`             | Path to trusted CA certs                    |

---

## 🐞 Bug Tracker

Found a bug or have a feature request?
Please open an issue at:
[https://github.com/klen/muffin-kafka/issues](https://github.com/klen/muffin-kafka/issues)

---

## 🤝 Contributing

Pull requests are welcome!
Development happens here:
[https://github.com/klen/muffin-kafka](https://github.com/klen/muffin-kafka)

---

## 🪪 License

**MIT** – See [LICENSE](./LICENSE) for full details.
