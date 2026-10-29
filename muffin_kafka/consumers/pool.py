"""Consumer pool: owns one ``AIOKafkaConsumer`` per listened topic."""

import asyncio
import dataclasses as dc

from aiokafka.consumer.consumer import AIOKafkaConsumer

from muffin_kafka import logger

from .utils import safe_commit


async def _commit(consumer: AIOKafkaConsumer) -> None:
    """Commit best-effort: a failing commit must not abort ``stop()``."""
    try:
        await safe_commit(consumer)
    except Exception:  # noqa: BLE001
        logger.exception("Kafka: Offset commit failed on stop")


@dc.dataclass
class ConsumerPool:
    """Consumers used by a runner, one per topic.

    Lifecycle: ``setup()`` stores the connection options, ``init()`` (or
    ``init_consumer()``) creates a consumer for every topic that is not covered
    yet, ``start()`` connects them and ``stop()`` commits and closes them.
    ``KafkaPlugin.setup_pool()`` calls ``setup()`` with the plugin configuration,
    so ``plugin.cfg`` is the source of the connection and consumer options.
    """

    consumers: list[AIOKafkaConsumer] = dc.field(default_factory=list)
    topics: set[str] = dc.field(default_factory=set)
    group_id: str | None = None
    params: dict = dc.field(default_factory=dict)
    is_started: bool = False

    def setup(self, group_id: str | None, **params):
        """Store the default consumer options and the fallback ``group_id``."""
        self.params = params
        self.group_id = group_id

    def init(self, *topics: str, **params):
        """Create a consumer for every topic in ``topics`` that the pool does not cover.

        The consumers are created but not started. ``params`` are merged over the
        options from ``setup()`` for the new consumers only.
        """
        for topic in set(topics) - self.topics:
            self.consumers.append(self.init_consumer(topic, **params))

    def init_consumer(self, *topics: str, **params) -> AIOKafkaConsumer:
        """Create an ``AIOKafkaConsumer`` for ``topics`` from the stored options.

        ``params`` win over the stored options; ``group_id`` falls back to the
        pool's value. A consumer without a ``group_id`` cannot commit offsets.
        """
        merged = dict(self.params, **params)
        merged["group_id"] = merged.get("group_id") or self.group_id
        consumer = AIOKafkaConsumer(*topics, **merged)
        self.topics.update(topics)
        return consumer

    async def start(self):
        """Connect every consumer to the cluster and mark the pool as started.

        Idempotent: a pool that is already started is left untouched. A failure
        raises ``ExceptionGroup`` and leaves ``is_started`` false.
        """
        if self.is_started:
            return

        async with asyncio.TaskGroup() as tg:
            for consumer in self.consumers:
                tg.create_task(consumer.start())
        self.is_started = True

    async def stop(self, *, commit: bool = True):
        """Commit offsets (unless ``commit=False``) and close every consumer.

        Commit failures are logged and never abort the shutdown; the consumers are
        always closed. Uncommitted records are delivered again by the broker.
        """
        if commit:
            async with asyncio.TaskGroup() as tg:
                for consumer in self.consumers:
                    tg.create_task(_commit(consumer))

        async with asyncio.TaskGroup() as tg:
            for consumer in self.consumers:
                tg.create_task(consumer.stop())
        self.is_started = False

    def __iter__(self):
        """Iterate over the pool's consumers."""
        return iter(self.consumers)
