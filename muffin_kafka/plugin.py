from __future__ import annotations

from asyncio import Task, gather
from asyncio import sleep as aio_sleep
from collections import defaultdict
from collections.abc import Awaitable
from time import time
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Coroutine,
    Mapping,
    Optional,
    TypedDict,
)

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, helpers
from aiokafka.client import create_task
from asgi_tools._compat import json_dumps
from muffin.plugins import BasePlugin, PluginError

TCallable = Callable[..., Awaitable[Any]]
TErrCallable = Callable[[BaseException], Awaitable[Any]]

if TYPE_CHECKING:
    from muffin.app import Application


class Options(TypedDict):
    auto_offset_reset: str
    bootstrap_servers: str
    client_id: str
    enable_auto_commit: bool
    group_id: Optional[str]
    max_poll_records: Optional[int]
    request_timeout_ms: int
    retry_backoff_ms: int
    sasl_mechanism: str
    sasl_plain_password: Optional[str]
    sasl_plain_username: Optional[str]
    security_protocol: str
    ssl_cafile: Optional[str]
    produce: bool
    listen: bool
    monitor: bool
    monitor_interval: int


class KafkaPlugin(BasePlugin):
    name = "kafka"
    defaults: ClassVar[Mapping[str, Any]] = {
        "auto_offset_reset": "earliest",
        "bootstrap_servers": "localhost:9092",
        "client_id": "muffin",
        "enable_auto_commit": False,
        "group_id": None,
        "max_poll_records": None,
        "request_timeout_ms": 30000,
        "retry_backoff_ms": 1000,
        "sasl_mechanism": "PLAIN",
        "sasl_plain_password": None,
        "sasl_plain_username": None,
        "security_protocol": "PLAINTEXT",
        "ssl_cafile": None,
        "produce": False,
        "listen": True,
        "monitor": False,
        "monitor_interval": 60,
    }

    def __init__(self, app: Optional[Application] = None, **kwargs):
        self.map: defaultdict[str, list[Callable]] = defaultdict(list)
        self.tasks: list[Task] = []
        self.consumers: dict[str, AIOKafkaConsumer] = {}
        self.handlers: list[tuple[tuple[str, ...], Callable[..., Coroutine]]] = []
        self.error_handler: Optional[TErrCallable] = None
        super().__init__(app, **kwargs)

    async def startup(self):
        self.app.logger.info("Kafka: Starting plugin")
        await self.connect()

    async def shutdown(self):
        self.app.logger.info("Stopping Kafka plugin")
        for task in self.tasks:
            task.cancel()
        await gather(*self.tasks, return_exceptions=True)

        if self.cfg.produce and hasattr(self, "producer"):
            await self.producer.stop()

        if self.cfg.listen:
            consumers = self.consumers.values()
            await gather(*[consumer.commit() for consumer in consumers])
            await gather(*[consumer.stop() for consumer in consumers])

    def get_params(self, **params):
        cfg = self.cfg
        kafka_params = dict(
            {
                "bootstrap_servers": cfg.bootstrap_servers,
                "client_id": cfg.client_id,
                "request_timeout_ms": cfg.request_timeout_ms,
                "retry_backoff_ms": cfg.retry_backoff_ms,
                "sasl_mechanism": cfg.sasl_mechanism,
                "sasl_plain_password": cfg.sasl_plain_password,
                "sasl_plain_username": cfg.sasl_plain_username,
                "security_protocol": cfg.security_protocol,
            },
            **params,
        )
        if cfg.ssl_cafile:
            kafka_params["ssl_context"] = helpers.create_ssl_context(cafile=cfg.ssl_cafile)

        return kafka_params

    def init_consumer(self, *topics: str, **params):
        cfg = self.cfg
        params.setdefault("group_id", cfg.group_id)
        params.setdefault("max_poll_records", cfg.max_poll_records)
        params.setdefault("auto_offset_reset", cfg.auto_offset_reset)
        params.setdefault("enable_auto_commit", cfg.enable_auto_commit)
        return AIOKafkaConsumer(*topics, **self.get_params(**params))

    async def send(self, topic: str, value: Any, key=None, **params):
        """Send a value to Kafka topic."""
        if not self.cfg.produce:
            raise PluginError("Kafka: Producer is not enabled")

        if key and isinstance(key, str):
            key = key.encode("utf-8")

        if not isinstance(value, bytes):
            value = value.encode("utf-8") if isinstance(value, str) else json_dumps(value)

        return await self.producer.send(topic, value, key=key, **params)

    async def send_and_wait(self, topic: str, value: Any, key=None, **params):
        """Send a value to Kafka topic and wait for a result."""
        fut = await self.send(topic, value, key, **params)
        return await fut

    def handle_topics(self, *topics: str) -> Callable[[TCallable], TCallable]:
        """Register a handler for Kafka messages."""

        def wrapper(fn):
            self.handlers.append((topics, fn))
            return fn

        return wrapper

    def handle_error(self, fn: TErrCallable) -> TErrCallable:
        """Register a handler for Kafka errors."""

        self.error_handler = fn
        return fn

    async def connect(
        self,
        *only: str,
        listen: bool | None = None,
        produce: bool | None = None,
        monitor: bool | None = None,
        group_id: str | None = None,
        **params,
    ):
        cfg = self.cfg
        kafka_params = self.get_params(**params)
        logger = self.app.logger

        logger.info("Kafka: Connecting to %s", self.cfg.bootstrap_servers)
        logger.info("Kafka: Params %r", kafka_params)

        listen = cfg.listen if listen is None else listen
        if listen:
            logger.info("Kafka: Setup listeners")
            for topics, fn in self.handlers:
                filtered = [t for t in topics if t in only] if only else topics
                for topic in filtered:
                    if topic not in self.consumers:
                        logger.info("Kafka: Listen to %s", topic)
                        consumer = self.consumers[topic] = self.init_consumer(
                            topic,
                            group_id=group_id or cfg.group_id,
                        )
                        await consumer.start()
                    self.map[topic].append(fn)

            self.tasks = [
                create_task(self.__process__(consumer)) for consumer in self.consumers.values()
            ]
            for task in self.tasks:
                task.add_done_callback(self._log_task_errors)

        produce = cfg.produce if produce is None else produce
        if produce:
            logger.info("Kafka: Setup producer")
            self.producer = AIOKafkaProducer(**kafka_params)
            await self.producer.start()

        monitor = cfg.monitor if monitor is None else monitor
        if monitor:
            logger.info("Kafka: Setup monitor")
            self.tasks.append(create_task(self.__monitor__()))

    def _log_task_errors(self, task: Task):
        try:
            exc = task.exception()
            if exc:
                self.app.logger.error("Kafka task crashed: %s", exc)
        except Exception:
            self.app.logger.exception("Kafka task error")

    async def __process__(self, consumer: AIOKafkaConsumer):
        logger = self.app.logger
        logger.info("Start listening Kafka messages")
        try:
            async for msg in consumer:
                logger.debug("Kafka msg: %s-%s-%s", msg.topic, msg.partition, msg.offset)
                for fn in self.map.get(msg.topic, []):
                    try:
                        await fn(msg)
                    except Exception as exc:  # noqa: PERF203
                        logger.exception("Kafka: Error while processing message: %r", msg)
                        if self.error_handler:
                            await self.error_handler(exc)
        except Exception:
            logger.exception("Kafka: Error while listening messages")

    async def __monitor__(self):
        logger = self.app.logger
        interval = self.cfg.monitor_interval

        while interval:
            now = int(time() * 1000)

            for consumer in self.consumers.values():
                assigned = sorted(consumer.assignment(), key=lambda p: p.partition)
                end_offsets = await consumer.end_offsets(assigned)

                for tp in assigned:
                    try:
                        pos = await consumer.position(tp)
                        committed = await consumer.committed(tp) or 0
                        end = end_offsets.get(tp, 0)
                        last_poll = consumer.last_poll_timestamp(tp)
                        lag = end - committed
                        poll_delay = now - last_poll if last_poll else None

                        logger.info(
                            (
                                "[Kafka Monitor] %s-%d | pos: %d | committed: %d "
                                "| end: %d | lag: %d | poll_delay: %sms"
                            ),
                            tp.topic,
                            tp.partition,
                            pos,
                            committed,
                            end,
                            lag,
                            poll_delay,
                        )

                    except Exception as e:  # noqa: BLE001, PERF203
                        logger.warning(f"[Kafka Monitor] Failed to fetch info for {tp}: {e}")

            await aio_sleep(interval)

    async def get_consumer_lag(self) -> dict[str, int]:
        """Return lag per topic-partition."""
        lags = {}
        for consumer in self.consumers.values():
            assigned = consumer.assignment()
            if not assigned:
                continue

            end_offsets = await consumer.end_offsets(assigned)
            for tp in assigned:
                committed = await consumer.committed(tp) or 0
                lag = end_offsets[tp] - committed
                lags[f"{tp.topic}:{tp.partition}"] = lag
        return lags

    async def healthcheck(self, max_lag: int = 1000) -> bool:
        """Check consumer health by analyzing lag."""
        lags = await self.get_consumer_lag()
        if not lags:
            return False

        for topic_partition, lag in lags.items():
            if lag > max_lag:
                self.app.logger.warning(f"[Kafka Healthcheck] High lag in {topic_partition}: {lag}")
                return False

        return True
