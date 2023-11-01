from asyncio import Task, gather
from collections import defaultdict
from collections.abc import Awaitable
from typing import Any, Callable, ClassVar, Coroutine, List, Tuple, TypedDict, cast

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, helpers
from aiokafka.client import create_task
from asgi_tools._compat import json_dumps
from muffin.plugins import BasePlugin

TCallable = Callable[..., Awaitable[Any]]


class Options(TypedDict):
    auto_offset_reset: str
    bootstrap_servers: str
    client_id: str
    enable_auto_commit: bool
    group_id: str | None
    max_poll_records: int | None
    request_timeout_ms: int
    retry_backoff_ms: int
    sasl_mechanism: str
    sasl_plain_password: str | None
    sasl_plain_username: str | None
    security_protocol: str
    ssl_cafile: str | None
    produce: bool
    auto_connect: bool


class KafkaPlugin(BasePlugin):
    name = "kafka"
    defaults: ClassVar[Options] = {
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
        "security_protocol": "SASL_PLAINTEXT",
        "ssl_cafile": None,
        "produce": False,
        "auto_connect": True,
    }

    producer: AIOKafkaProducer
    consumers: ClassVar[List[AIOKafkaConsumer]] = []
    handlers: ClassVar[List[Tuple[Tuple[str, ...], Callable[..., Coroutine]]]] = []

    def __init__(self, *args, **kwargs):
        self.map = defaultdict(list)
        self.tasks: List[Task] = []
        self.error_handler = None
        super().__init__(*args, **kwargs)

    async def startup(self):
        logger = self.app.logger
        logger.info("Kafka: Starting plugin")

        if self.cfg.auto_connect:
            await self.__connect__()

    async def shutdown(self):
        self.app.logger.info("Stopping Kafka plugin")
        for task in self.tasks:
            task.cancel()

        if self.cfg.auto_connect:
            await gather(*[consumer.stop() for consumer in self.consumers])

    async def send(self, topic: str, value: Any):
        """Send a message to Kafka."""
        if not self.cfg.produce:
            return False

        if not isinstance(value, str):
            value = json_dumps(value)

        fut = await self.producer.send(topic, value)
        return fut

    def handle_topics(self, *topics: str) -> Callable[[TCallable], TCallable]:
        """Register a handler for Kafka messages."""

        def wrapper(fn):
            self.handlers.append((topics, fn))
            return fn

        return wrapper

    def handle_error(self, err: BaseException) -> Callable[[TCallable], TCallable]:
        """Register a handler for Kafka errors."""

        def wrapper(fn):
            self.error_handler = fn
            return fn

        return wrapper

    async def __connect__(self):
        cfg = self.cfg
        params = {
            "auto_offset_reset": cfg.auto_offset_reset,
            "bootstrap_servers": cfg.bootstrap_servers,
            "client_id": cfg.client_id,
            "enable_auto_commit": cfg.enable_auto_commit,
            "group_id": cfg.group_id,
            "max_poll_records": cfg.max_poll_records,
            "request_timeout_ms": cfg.request_timeout_ms,
            "retry_backoff_ms": cfg.retry_backoff_ms,
            "sasl_mechanism": cfg.sasl_mechanism,
            "sasl_plain_password": cfg.sasl_plain_password,
            "sasl_plain_username": cfg.sasl_plain_username,
            "security_protocol": cfg.security_protocol,
        }
        if cfg.ssl_cafile:
            params["ssl_context"] = helpers.create_ssl_context(cafile=cfg.ssl_cafile)

        logger = self.app.logger
        logger.info("Kafka: Connecting to %s", self.cfg.bootstrap_servers)
        logger.info("Kafka: Params %r", params)

        consumers = {}

        for topics, fn in self.handlers:
            logger.info("Kafka: Listen to %r", topics)
            for topic in topics:
                if topic not in consumers:
                    consumer = consumers[topic] = AIOKafkaConsumer(topic, **params)
                    self.consumers.append(consumer)
                    await consumer.start()

                for topic in topics:
                    self.map[topic].append(fn)

        self.tasks = [create_task(self.__process__(consumer)) for consumer in self.consumers]

    async def __process__(self, consumer):
        logger = self.app.logger
        logger.info("Start listening Kafka messages")
        async for msg in consumer:
            logger.info("Kafka msg: %s-%s-%s", msg.topic, msg.partition, msg.offset)
            for fn in self.map[msg.topic]:
                try:
                    await fn(msg)
                except Exception as exc:  # noqa: BLE001
                    logger.error("Kafka: Error while processing message: %s", exc)
                    error_handler = cast(TCallable, self.error_handler)
                    if error_handler:
                        await error_handler(exc)
