from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Mapping,
    Optional,
)

from aiokafka import AIOKafkaProducer, helpers
from asgi_tools._compat import json_dumps
from muffin.plugins import BasePlugin, PluginError

from muffin_kafka.consumers import (
    ConsumerHandlers,
    ConsumerPool,
    ConsumerPoolHealthcheck,
    TCallable,
    TErrCallable,
)
from muffin_kafka.consumers.runner import BatchPoolRunner, PoolRunner, SinglePoolRunner

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
        "batch_size": None,  # Read messages in batches via getmany()
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
        self.runner: PoolRunner | None = None
        self.producer: AIOKafkaProducer | None = None
        self.handlers = ConsumerHandlers()
        self.consumer_pool = ConsumerPool()

        super().__init__(app, **kwargs)

    def setup(self, app: Application, **options) -> bool:
        """Setup the plugin by registering management commands."""
        if super().setup(app, **options):
            self.setup_commands(app)

        cfg = self.cfg
        consumer_params = self.get_common_params()
        consumer_params.setdefault("max_poll_records", cfg.max_poll_records)
        consumer_params.setdefault("auto_offset_reset", cfg.auto_offset_reset)
        consumer_params.setdefault("enable_auto_commit", cfg.enable_auto_commit)
        consumer_params.setdefault("group_id", cfg.group_id)
        self.consumer_pool.setup(**consumer_params)

        return True

    async def startup(self):
        """Start the plugin by initializing producer and/or consumers based on configuration."""
        logger = self.app.logger
        logger.info("Kafka: Starting plugin: %s", self.name)
        cfg = self.cfg

        if cfg.produce:
            logger.info("Kafka: Setup producer")
            self.producer = AIOKafkaProducer(**self.get_common_params())
            await self.producer.start()

        if cfg.listen:
            logger.info("Kafka: Setup listeners")
            await self.listen()

    async def shutdown(self):
        """Stop the plugin by cancelling tasks and stopping producer and consumers."""
        self.app.logger.info("Stopping Kafka plugin: %s", self.name)

        if self.runner:
            await self.runner.stop(commit=True)

        if self.producer:
            await self.producer.stop()

    def setup_commands(self, app: Application):
        @app.manage(name=f"{self.name}-healthcheck")
        async def healthcheck(*only: str, max_lag=1000):
            """Run Kafka healthcheck.

            param topics: Optional list of topics to check.
            param max_lag: Maximum allowed lag for healthcheck.
            """
            topics_to_listen = only or self.handlers.get_topics()
            self.consumer_pool.init(*topics_to_listen)
            healthcheck = ConsumerPoolHealthcheck(self.consumer_pool, max_lag=max_lag)
            await healthcheck.process()
            if not healthcheck:
                app.logger.error("Kafka healthcheck failed")
                raise SystemExit(1)

        @app.manage(name=f"{self.name}-listen", lifespan=True)
        async def listen(
            *topics: str,
            group_id: str | None = None,
            monitor: bool = True,
            batch_size: int | None = None,
        ):
            """Start listening to Kafka topics.

            If no topics are specified, all topics with registered handlers will be listened to.
            """
            # If the plugin is not started yet, we need to start it before listening
            await self.listen(*topics, group_id=group_id, monitor=monitor, batch_size=batch_size)

            # Wait until the plugin is stopped to exit the function
            assert self.runner
            await self.runner

    def get_common_params(self, **params: Any) -> dict[str, Any]:
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

    async def listen(
        self,
        *only: str,
        monitor: bool | None = None,
        batch_size: int | None = None,
        **params: Any,
    ):
        """Start listening to Kafka topics.

        If no topics are specified, all topics with registered handlers will be listened to.
        """
        topics_to_listen = only or self.handlers.get_topics()
        self.consumer_pool.init(*topics_to_listen, **params)
        batch_size = batch_size or self.cfg.batch_size
        self.runner = (
            BatchPoolRunner(self.consumer_pool, self.handlers, batch_size=batch_size)
            if batch_size
            else SinglePoolRunner(self.consumer_pool, self.handlers)
        )
        monitor = self.cfg.monitor if monitor is None else monitor
        await self.runner.start(monitor)

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
            self.handlers.set_handler(fn, *topics)
            return fn

        return wrapper

    def handle_error(self, fn: TErrCallable) -> TErrCallable:
        """Register a handler for Kafka errors."""

        self.handlers.set_error_handler(fn)
        return fn
