"""Muffin plugin wiring an aiokafka producer and consumer pool into the app lifecycle."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Mapping
from unittest import mock

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, helpers
from asgi_tools._compat import json_dumps
from muffin.plugins import BasePlugin, PluginError

from muffin_kafka import logger
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
    """Kafka integration for Muffin.

    Options live in ``cfg`` (see ``defaults``). ``setup()`` registers the
    management commands and copies the Kafka options into ``consumer_pool``.
    ``startup()`` creates the producer when ``produce`` is enabled and starts the
    consumers when ``listen`` is enabled; ``shutdown()`` stops the runner
    (committing offsets) and the producer.

    Runtime state: ``producer`` (only with ``produce``), ``handlers`` filled by
    the decorators below, ``consumer_pool`` (one consumer per listened topic) and
    ``runner`` (``None`` until ``listen()`` is called).
    """

    name = "kafka"
    defaults: ClassVar[Mapping[str, Any]] = {
        # Plugin options
        "produce": False,  # Enable producer functionality
        "listen": True,  # Auto start consumers for registered handlers
        "monitor": False,  # Enable consumer monitoring task
        "monitor_interval": 60,
        "batch_size": None,  # Read messages in batches via getmany()
        "setup_commands": True,  # Register management commands
        #
        # Kafka connection parameters
        "bootstrap_servers": ["localhost:9092"],
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

    def __init__(
        self,
        app: Application | None = None,
        *,
        context: Callable[[], AbstractAsyncContextManager] | None = None,
        **kwargs,
    ):
        self.runner: PoolRunner | None = None
        self.producer: AIOKafkaProducer | None = None
        self.handlers = ConsumerHandlers()
        self.consumer_pool = ConsumerPool()
        self.context = context

        super().__init__(app, **kwargs)

    def setup(self, app: Application, **options) -> bool:
        """Bind the app, register management commands and configure the consumer pool.

        Returns ``False`` without doing anything when the plugin is disabled.
        """
        if not super().setup(app, **options):
            return False

        self.setup_commands(app)
        self.setup_pool()

        return True

    async def startup(self):
        """Start the producer (``produce``) and the consumers (``listen``) with the app."""
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
        """Stop the runner with a final offset commit, then stop the producer.

        The consumer tasks are cancelled after the commit, so a message that is
        still being processed may be committed before its handler finishes.
        """
        self.app.logger.info("Stopping Kafka plugin: %s", self.name)

        if self.runner:
            await self.runner.stop(commit=True)

        if self.producer:
            await self.producer.stop()

    def setup_commands(self, app: Application):
        """Register ``kafka-healthcheck`` and ``kafka-listen`` unless ``setup_commands`` is off.

        ``kafka-healthcheck`` starts the pool itself (or reuses a running one),
        ``kafka-listen`` runs inside the app lifespan so the plugin (and its
        automatic ``listen``) starts before the command body runs.
        """
        if not self.cfg.setup_commands:
            return

        @app.manage(name=f"{self.name}-healthcheck")
        async def healthcheck(*only: str, max_lag=1000):
            """Run Kafka healthcheck.

            param only: Optional list of topics to check (defaults to registered handlers).
            param max_lag: Maximum allowed lag for healthcheck.

            Starts the consumers when the pool is not running yet (a separate pool,
            so the plugin's own consumers are untouched) and stops them afterwards
            with ``commit=False``; a running pool is reused. Exits with status 1
            when the check cannot be performed (no assigned partitions) or when any
            partition lag exceeds ``max_lag``.
            """
            topics_to_listen = only or self.handlers.get_topics()
            pool = self.consumer_pool
            started_here = not pool.is_started
            if started_here:
                pool = ConsumerPool()
                pool.setup(self.consumer_pool.group_id, **self.consumer_pool.params)
                pool.init(*topics_to_listen)
                await pool.start()

            try:
                checker = ConsumerPoolHealthcheck(pool, max_lag=max_lag)
                performed = await checker.process()
            finally:
                if started_here:
                    await pool.stop(commit=False)

            if not performed or not checker:
                app.logger.error("Kafka healthcheck failed")
                raise SystemExit(1)

        @app.manage(name=f"{self.name}-listen", lifespan=True)
        async def listen(
            *topics: str,
            group_id: str | None = None,
            monitor: bool = True,
            batch_size: int | str = 0,
        ):
            """Start listening to Kafka topics.

            param topics: Topics to listen (defaults to all registered handlers).
            param group_id: Override the consumer group ID for this run.
            param monitor: Start the consumer monitor log task (use --no-monitor to disable).
            param batch_size: Read messages in batches of this size (0 keeps single mode).
            """
            # Idempotent: reuses the runner started by plugin.startup() (listen=True)
            await self.listen(
                *topics,
                monitor=monitor,
                group_id=group_id,
                context=self.context,
                batch_size=int(batch_size),
            )

            # Wait until the plugin is stopped to exit the function
            assert self.runner
            await self.runner

    def setup_pool(self):
        """Pass the consumer options from ``cfg`` to ``consumer_pool.setup()``."""
        cfg = self.cfg
        consumer_params = self.get_common_params()
        consumer_params.setdefault("max_poll_records", cfg.max_poll_records)
        consumer_params.setdefault("auto_offset_reset", cfg.auto_offset_reset)
        consumer_params.setdefault("enable_auto_commit", cfg.enable_auto_commit)
        consumer_params.setdefault("group_id", cfg.group_id)
        self.consumer_pool.setup(**consumer_params)

    def get_common_params(self, **params: Any) -> dict[str, Any]:
        """Build ``AIOKafka`` connection options from the plugin configuration.

        ``params`` are merged over the configured values. ``ssl_cafile`` becomes
        an ``ssl_context``, which is what aiokafka expects.
        """
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
        context: Callable[[], AbstractAsyncContextManager] | None = None,
        **params: Any,
    ) -> PoolRunner:
        """Start consuming messages from ``only`` topics (all registered topics if empty).

        Creates one consumer per topic (``consumer_pool.init``) and starts a
        ``BatchPoolRunner`` when ``batch_size`` (argument or ``cfg.batch_size``)
        is set, a ``SinglePoolRunner`` otherwise. ``monitor=None`` falls back to
        ``cfg.monitor``; the interval is always ``cfg.monitor_interval``.
        ``context`` is a callable returning an async context manager wrapped
        around each per-consumer loop; ``**params`` go to ``AIOKafkaConsumer``.

        Idempotent: when the consumers are already running, the existing runner
        is returned and the arguments are ignored. Call ``stop()`` first to
        restart with different options.
        """
        if self.runner is not None and self.consumer_pool.is_started:
            logger.info("Kafka: Consumers are already running, reusing the runner")
            return self.runner

        topics_to_listen = only or self.handlers.get_topics()
        self.consumer_pool.init(*topics_to_listen, **params)
        batch_size = batch_size or self.cfg.batch_size
        runner = (
            BatchPoolRunner(
                self.consumer_pool,
                self.handlers,
                batch_size=batch_size,
                enable_auto_commit=self.cfg.enable_auto_commit,
            )
            if batch_size
            else SinglePoolRunner(
                self.consumer_pool,
                self.handlers,
                enable_auto_commit=self.cfg.enable_auto_commit,
            )
        )
        should_monitor = self.cfg.monitor if monitor is None else monitor
        monitor_interval = self.cfg.monitor_interval if should_monitor else None
        await runner.start(monitor_interval, context=context)

        self.runner = runner
        return runner

    async def send(self, topic: str, value: Any, key=None, **params):
        """Encode and publish a message, returning aiokafka's delivery future.

        ``str`` values are UTF-8 encoded, ``bytes`` are sent as is, anything else
        is JSON-encoded; a truthy ``str`` key is UTF-8 encoded. Extra ``params``
        go to ``AIOKafkaProducer.send()`` (``partition``, ``headers``, ...).
        Awaiting this method only queues the message, await the returned future
        to wait for delivery. Raises ``PluginError`` when ``produce`` is disabled
        or the producer was not started.
        """
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
        """Decorator: call ``fn`` for every message of ``topics``.

        Several handlers may be registered for one topic. They run sequentially
        in registration order, and a failure in one does not skip the rest.
        """

        def wrapper(fn):
            self.handlers.set_handler(fn, *topics)
            return fn

        return wrapper

    def handle_error(self, fn: TErrCallable) -> TErrCallable:
        """Decorator: register the single global error handler.

        It receives the exception raised by a topic handler. An exception raised
        by the error handler itself escapes into the consumer loop and stops that
        consumer task.
        """

        self.handlers.set_error_handler(fn)
        return fn

    @asynccontextmanager
    async def conftest(self):
        """Patch producer/consumer ``start``/``stop`` so tests need no broker."""

        async def _noop(self):
            pass

        with (
            mock.patch.object(AIOKafkaProducer, "start", _noop),
            mock.patch.object(AIOKafkaProducer, "stop", _noop),
            mock.patch.object(AIOKafkaConsumer, "start", _noop),
            mock.patch.object(AIOKafkaConsumer, "stop", _noop),
        ):
            yield self
