"""Observers that periodically read consumer offsets for logging or health checks."""

import abc
import dataclasses as dc
from asyncio import sleep
from time import time
from typing import Any

from aiokafka.consumer.consumer import AIOKafkaConsumer
from aiokafka.structs import TopicPartition

from muffin_kafka import logger
from muffin_kafka.consumers.pool import ConsumerPool


@dc.dataclass
class ConsumerPoolMonitor(abc.ABC):
    """Base class for consumer pool observers.

    ``process()`` is the entry point: it does nothing when the pool is not
    started, otherwise it calls ``process_consumer()`` for every consumer, which
    resolves the assignment and calls ``process_tp()`` for every assigned
    partition. Subclasses only have to implement ``process_tp()``; override
    ``process_consumer()`` or ``process()`` for another granularity.

    The plugin creates only ``ConsumerPoolLogger`` (``monitor`` option). A custom
    monitor is instantiated and run by the user (see the README).
    """

    pool: ConsumerPool

    async def process(self):
        """Run one monitoring pass over the pool; skip it if the pool is not started."""
        if not self.pool.is_started:
            logger.warning(
                "[Kafka Processor] Consumer pool is not started. Processor will not run.",
            )
            return

        for consumer in self.pool:
            await self.process_consumer(consumer)

    async def process_consumer(self, consumer: AIOKafkaConsumer):
        """Process each partition assigned to ``consumer`` (unassigned consumers are skipped)."""
        assigned = consumer.assignment()
        if not assigned:
            return

        end_offsets = await consumer.end_offsets(assigned)
        for tp in assigned:
            await self.process_tp(consumer, tp, end_offsets.get(tp, 0))

    @abc.abstractmethod
    async def process_tp(
        self,
        consumer: AIOKafkaConsumer,
        tp: TopicPartition,
        end_offset: int,
    ) -> Any:
        """Handle one partition; ``end_offset`` is the broker's latest offset.

        The return value is ignored by ``process()``.
        """
        raise NotImplementedError("Override process_tp to implement custom processing logic")


@dc.dataclass
class ConsumerPoolLogger(ConsumerPoolMonitor):
    """Log position, committed offset, end offset, lag and poll delay per partition.

    ``interval`` is the delay between passes, in seconds, used by ``__call__``:
    that coroutine loops ``process()`` and ``sleep(interval)`` until cancelled and
    is what the runner starts when monitoring is enabled. ``interval=0`` makes
    the loop exit immediately.
    """

    interval: int

    async def __call__(self, interval: int | None = None):
        """Process/sleep loop meant to run as a task; ``interval`` overrides ``self.interval``."""
        interval = self.interval if interval is None else interval

        while interval:
            await self.process()
            await sleep(interval)

    async def process_tp(self, consumer: AIOKafkaConsumer, tp: TopicPartition, end_offset: int):
        """Log one line of metrics for a partition; read errors are logged and ignored."""
        now = int(time() * 1000)
        try:
            pos = await consumer.position(tp)
            committed = await consumer.committed(tp) or 0
            last_poll = consumer.last_poll_timestamp(tp)
            lag = end_offset - committed
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
                end_offset,
                lag,
                poll_delay,
            )

        except Exception as e:  # noqa: BLE001
            logger.warning(f"[Kafka Monitor] Failed to fetch info for {tp}: {e}")


@dc.dataclass
class ConsumerPoolHealthcheck(ConsumerPoolMonitor):
    """Mark the pool unhealthy when a partition lag exceeds ``max_lag``.

    Lag is ``end_offset - committed``, with a missing committed offset counting
    as 0, and the comparison is strict (``lag > max_lag``). ``is_healthy`` starts
    as true and is never reset; a pool that is not started, or has no assigned
    partitions, reports healthy. The check therefore fails open: truth-test the
    instance after ``process()`` to get the result.
    """

    max_lag: int = 100
    is_healthy: bool = True

    async def process_tp(self, consumer: AIOKafkaConsumer, tp: TopicPartition, end_offset: int):
        """Update ``is_healthy`` for one partition and log its lag."""
        committed = await consumer.committed(tp) or 0
        lag = end_offset - committed
        logger.info(
            f"[Healthcheck] {tp.topic}-{tp.partition} | committed: {committed} | end: {end_offset} | lag: {lag}",  # noqa: E501
        )
        if lag > self.max_lag:
            logger.warning(
                f"Consumer has lag {lag} on topic {tp.topic} partition {tp.partition}",
            )
            self.is_healthy = False

    def __bool__(self):
        """Return ``is_healthy`` so ``if healthcheck:`` reads naturally."""
        return self.is_healthy
