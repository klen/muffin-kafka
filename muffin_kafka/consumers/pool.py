"""Consumer pool: owns one ``AIOKafkaConsumer`` per listened topic."""

import asyncio
import dataclasses as dc

from aiokafka.consumer.consumer import AIOKafkaConsumer

from .utils import safe_commit


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
    group_id: str | None = None
    params: dict = dc.field(default_factory=dict)
    is_started: bool = False

    def setup(self, group_id: str, **params):
        """Store the default consumer options and the fallback ``group_id``."""
        self.params = params
        self.group_id = group_id

    def init(self, *topics: str, **params):
        """Create a consumer for every topic that no existing consumer subscribes to.

        The consumers are created but not started. ``params`` are merged over the
        options from ``setup()`` for the new consumers only.
        """
        topics_to_listen = set(topics)
        missing = topics_to_listen.copy()
        for consumer in self.consumers:
            missing -= set(consumer._client._topics)

        for topic in missing:
            consumer = self.init_consumer(topic, **params)
            self.consumers.append(consumer)

    def init_consumer(self, *topics: str, **params) -> AIOKafkaConsumer:
        """Create an ``AIOKafkaConsumer`` for ``topics`` from the stored options.

        ``params`` win over the stored options; ``group_id`` falls back to the
        pool's value. A consumer without a ``group_id`` cannot commit offsets.
        """
        merged = dict(self.params, **params)
        merged["group_id"] = merged.get("group_id") or self.group_id
        return AIOKafkaConsumer(*topics, **merged)

    async def start(self):
        """Connect every consumer to the cluster and mark the pool as started.

        A failure raises ``ExceptionGroup`` and leaves ``is_started`` false.
        Starting the same pool twice starts the same consumers again, which
        aiokafka rejects with an ``AssertionError``.
        """
        async with asyncio.TaskGroup() as tg:
            for consumer in self.consumers:
                tg.create_task(consumer.start())
        self.is_started = True

    async def stop(self, *, commit: bool = True):
        """Commit offsets (unless ``commit=False``) and close every consumer.

        ``safe_commit`` swallows rebalance-related commit failures; other errors
        propagate and may prevent the consumers from being stopped.
        """
        if commit:
            async with asyncio.TaskGroup() as tg:
                for consumer in self.consumers:
                    tg.create_task(safe_commit(consumer))

        async with asyncio.TaskGroup() as tg:
            for consumer in self.consumers:
                tg.create_task(consumer.stop())
        self.is_started = False

    def __iter__(self):
        """Iterate over the pool's consumers."""
        return iter(self.consumers)
