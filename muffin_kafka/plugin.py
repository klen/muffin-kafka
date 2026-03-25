from __future__ import annotations

from asyncio import Task, gather, sleep
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
)

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, helpers
from aiokafka.client import create_task
from asgi_tools._compat import json_dumps
from muffin.plugins import BasePlugin, PluginError

TCallable = Callable[..., Awaitable[Any]]
TErrCallable = Callable[[BaseException], Awaitable[Any]]

if TYPE_CHECKING:
    from muffin.app import Application


class KafkaPlugin(BasePlugin):
    name = "kafka"
    defaults: ClassVar[Mapping[str, Any]] = {
        # Plugin options
        "produce": False,  # Enable producer functionality
        "listen": True,  # Auto start consumers for registered handlers
        "monitor": False,  # Enable consumer monitoring task
        "monitor_interval": 60,
        #
        # Kafka connection parameters
        "bootstrap_servers": "localhost:9092",
        "client_id": "muffin",
        "request_timeout_ms": 30000,
        "retry_backoff_ms": 1000,
        "sasl_mechanism": "PLAIN",
        "sasl_plain_password": None,
        "sasl_plain_username": None,
        "security_protocol": "PLAINTEXT",
        "ssl_cafile": None,
        #
        # Consumer parameters
        "auto_offset_reset": "earliest",
        "enable_auto_commit": False,
        "group_id": None,
        "max_poll_records": None,
    }

    def __init__(self, app: Optional[Application] = None, **kwargs):
        self.error_handler: Optional[TErrCallable] = None

        self.tasks: list[Task] = []
        self.consumers: list[AIOKafkaConsumer] = []
        self.producer: AIOKafkaProducer | None = None
        self.handlers: dict[str, list[Callable[..., Coroutine]]] = defaultdict(list)

        super().__init__(app, **kwargs)

    def setup(self, app: Application, **options) -> bool:
        setup_result = super().setup(app, **options)

        @app.manage(name=f"{self.name}-healthcheck")
        async def healthcheck(max_lag=1000):
            """Run Kafka healthcheck.

            param max_lag: Maximum allowed lag for healthcheck.
            """
            if not await self.healthcheck(max_lag):
                app.logger.error("Kafka healthcheck failed")
                raise SystemExit(1)

        @app.manage(name=f"{self.name}-listen", lifespan=True)
        async def listen(*topics: str, group_id: str | None = None, monitor: bool = True):
            """Start listening to Kafka topics.

            If no topics are specified, all topics with registered handlers will be listened to.
            """
            # If the plugin is not started yet, we need to start it before listening
            await self.listen(*topics, group_id=group_id, monitor=monitor)

            # Wait until the plugin is stopped to exit the function
            await gather(*self.tasks)

        return setup_result

    async def startup(self):
        """Start the plugin by initializing producer and/or consumers based on configuration."""
        logger = self.app.logger
        logger.info("Kafka: Starting plugin: %s", self.name)

        if self.cfg.produce:
            logger.info("Kafka: Setup producer")
            self.producer = AIOKafkaProducer(**self.get_params())
            await self.producer.start()

        if self.cfg.listen:
            logger.info("Kafka: Setup listeners")
            await self.listen()

    async def shutdown(self):
        """Stop the plugin by cancelling tasks and stopping producer and consumers."""
        self.app.logger.info("Stopping Kafka plugin: %s", self.name)
        for task in self.tasks:
            task.cancel()
        await gather(*self.tasks, return_exceptions=True)

        if self.producer:
            await self.producer.stop()

        if self.cfg.listen:
            await gather(*[consumer.commit() for consumer in self.consumers])
            await gather(*[consumer.stop() for consumer in self.consumers])

    def init_consumer(self, *topics: str, **params: Any):
        """Initialize a consumer for the given topics and add it to the cluster."""
        cfg = self.cfg
        params.setdefault("group_id", cfg.group_id)
        params.setdefault("max_poll_records", cfg.max_poll_records)
        params.setdefault("auto_offset_reset", cfg.auto_offset_reset)
        params.setdefault("enable_auto_commit", cfg.enable_auto_commit)
        consumer = AIOKafkaConsumer(*topics, **self.get_params(**params))
        self.consumers.append(consumer)
        return consumer

    def get_params(self, **params: Any) -> dict[str, Any]:
        """Get Kafka connection parameters by merging plugin configuration
        with provided parameters."""
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

    async def listen(self, *only: str, monitor: bool | None = None, **params: Any):
        """Start listening to Kafka topics.

        If no topics are specified, all topics with registered handlers will be listened to.
        """
        topics_to_listen = set(only) if only else set(self.handlers)
        missing = topics_to_listen.copy()
        for consumer in self.consumers:
            missing -= set(consumer._client._topics)

        # If there are any topics that don't have a consumer yet, initialize a new consumer for them
        if missing:
            self.init_consumer(*missing, **params)

        topics = set(only) if only else set(self.handlers)
        consumers = [c for c in self.consumers if topics.intersection(c._client._topics)]
        if not consumers:
            self.app.logger.warning("Kafka: No consumers found for topics: %s", topics)
            return

        await gather(*[consumer.start() for consumer in consumers])

        for consumer in consumers:
            task = create_task(self.__process__(consumer))
            task.add_done_callback(self._log_task_errors)
            self.tasks.append(task)

        monitor = self.cfg.monitor if monitor is None else monitor
        if monitor:
            self.app.logger.info("Kafka: Setup monitor")
            monitor_task = create_task(self.__monitor__())
            monitor_task.add_done_callback(self._log_task_errors)
            self.tasks.append(monitor_task)

    async def send(self, topic: str, value: Any, key=None, **params):
        """Send a value to Kafka topic."""
        if not self.cfg.produce:
            raise PluginError("Kafka: Producer is not enabled")

        if self.producer is None:
            raise PluginError("Kafka: Producer is not initialized")

        if key and isinstance(key, str):
            key = key.encode("utf-8")

        if not isinstance(value, bytes):
            value = value.encode("utf-8") if isinstance(value, str) else json_dumps(value)

        return await self.producer.send(topic, value, key=key, **params)

    def handle_topics(self, *topics: str) -> Callable[[TCallable], TCallable]:
        """Register a handler for Kafka messages."""

        def wrapper(fn):
            for topic in topics:
                self.handlers[topic].append(fn)

            return fn

        return wrapper

    def handle_error(self, fn: TErrCallable) -> TErrCallable:
        """Register a handler for Kafka errors."""

        self.error_handler = fn
        return fn

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
                for fn in self.handlers[msg.topic]:
                    try:
                        await fn(msg)
                    except Exception as exc:
                        logger.exception("Kafka: Error while processing message: %r", msg)
                        if self.error_handler:
                            await self.error_handler(exc)
        except Exception:
            logger.exception("Kafka: Error while listening messages")

    async def __monitor__(self, interval: int | None = None):
        logger = self.app.logger
        interval = self.cfg.monitor_interval if interval is None else interval

        while interval:
            now = int(time() * 1000)
            for consumer in self.consumers:
                assigned = consumer.assignment()
                if not assigned:
                    continue

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
                                "[Monitor] %s-%d | pos: %d | committed: %d "
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

                    except Exception as e:  # noqa: BLE001
                        logger.warning(f"[Kafka Monitor] Failed to fetch info for {tp}: {e}")

            await sleep(interval)

    async def healthcheck(self, max_lag: int = 100) -> bool:
        """Check consumer health by analyzing lag."""
        for consumer in self.consumers:
            assigned = consumer.assignment()
            if not assigned:
                continue

            end_offsets = await consumer.end_offsets(assigned)

            for tp in assigned:
                committed = await consumer.committed(tp) or 0
                lag = end_offsets[tp] - committed
                if lag > max_lag:
                    self.app.logger.warning(
                        f"Consumer has lag {lag} on topic {tp.topic} partition {tp.partition}",
                    )
                    return False

        return True


# ruff: noqa: PERF203
