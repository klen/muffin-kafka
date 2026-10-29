"""Consume loops: one asyncio task per consumer, feeding records to the handlers."""

import asyncio
import dataclasses as dc
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, nullcontext
from typing import Coroutine

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import (
    ConsumerStoppedError,
    IllegalGenerationError,
    IllegalStateError,
    KafkaConnectionError,
    KafkaTimeoutError,
    RebalanceInProgressError,
    UnknownMemberIdError,
)
from aiokafka.util import create_task

from muffin_kafka import logger
from muffin_kafka.consumers import ConsumerPool, ConsumerPoolLogger, safe_commit
from muffin_kafka.consumers.handlers import ConsumerHandlers

# Polling errors that only mean "the group moved" or "the broker hiccupped":
# log, wait a moment and poll again instead of killing the consumer task.
# Configuration errors (for example `IllegalOperation`) stay fatal.
RETRY_ERRORS = (
    KafkaConnectionError,
    KafkaTimeoutError,
    RebalanceInProgressError,
    UnknownMemberIdError,
    IllegalGenerationError,
    IllegalStateError,
)


@dc.dataclass
class PoolRunner:
    """Start the consumer pool and run one consume loop per consumer.

    ``start()`` starts the consumers, registers one task per consumer and,
    optionally, a ``ConsumerPoolLogger`` task. ``stop()`` cancels the loops,
    then optionally commits offsets and closes the consumers. A truthy
    ``batch_size`` reads with ``getmany()``, otherwise with ``getone()``.
    """

    pool: ConsumerPool
    handlers: ConsumerHandlers
    enable_auto_commit: bool = True
    batch_size: int | None = None
    tasks: list = dc.field(default_factory=list)

    def __post_init__(self):
        self._stop_event: asyncio.Event = asyncio.Event()
        self._context_factory: Callable[[], AbstractAsyncContextManager] = nullcontext

    async def start(
        self,
        monitor: int | None = None,
        context: Callable[[], AbstractAsyncContextManager] | None = None,
    ):
        """Start the pool and spawn the consumer tasks.

        ``monitor`` is the interval in seconds of the built-in
        ``ConsumerPoolLogger`` (``None`` or ``0`` disables it). ``context``
        overrides the factory of the async context manager wrapped around each
        consumer loop.
        """
        if context is not None:
            self._context_factory = context

        await self.pool.start()

        for consumer in self.pool:
            self.register_task(self.run_consumer(consumer))

        if monitor:
            logger.info("Starting Kafka consumer pool monitor with interval %s seconds", monitor)
            pool_logger = ConsumerPoolLogger(pool=self.pool, interval=monitor)
            self.register_task(pool_logger())

    async def stop(self, *, commit: bool = False):
        """Cancel the consumer tasks, then optionally commit and close the pool.

        The loops are cancelled first: a message that is still being handled is
        interrupted and not committed, so it is delivered again on restart
        (at-least-once). With ``enable_auto_commit=False`` the loop already
        committed after every successful record or batch, so the default
        ``commit=False`` loses nothing and avoids committing in-flight work.
        """
        self._stop_event.set()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()

        await self.pool.stop(commit=commit)

    def register_task(self, coro: Coroutine):
        """Run ``coro`` as a task and log its exception instead of losing it."""
        task = create_task(coro)
        task.add_done_callback(self._handle_task_exception)
        self.tasks.append(task)

    def _handle_task_exception(self, task):
        """Log task crashes; normal stop and cancellation are expected."""
        if task.cancelled():
            return

        exc = task.exception()
        if exc is None or isinstance(exc, ConsumerStoppedError):
            return

        logger.error("Kafka task crashed: %s", exc)

    async def run_consumer(self, consumer):
        """Run one consumer loop inside the configured context manager."""
        async with self._context_factory():
            await self._run_consumer(consumer)

    async def _run_consumer(self, consumer: AIOKafkaConsumer):
        """Consume from ``consumer`` until the runner stops.

        With ``enable_auto_commit`` off the offset is committed only when every
        record of the last poll succeeded. A failed record gets no commit, but the
        next successful commit sends the consumer's current position, which is
        already past the failed record: it is not retried while this consumer runs.
        """
        stop = self._stop_event
        handlers = self.handlers
        batch_size = self.batch_size

        while not stop.is_set():
            try:
                if batch_size:
                    batches = await consumer.getmany(timeout_ms=100, max_records=batch_size)
                    messages = [msg for batch in batches.values() for msg in batch]
                else:
                    messages = [await consumer.getone()]

            except ConsumerStoppedError:
                break

            except RETRY_ERRORS as exc:
                logger.warning("Kafka: %s, retrying in 1s", exc)
                await asyncio.sleep(1)
                continue

            success = True
            for msg in messages:
                if not await handlers(msg):
                    success = False

            if messages and success and not self.enable_auto_commit:
                await safe_commit(consumer)

    def __await__(self):
        """Wait for all runner tasks, keeping ``kafka-listen`` alive."""
        return asyncio.gather(*self.tasks).__await__()


# One runner now serves both modes; the old names stay for existing imports.
SinglePoolRunner = PoolRunner
BatchPoolRunner = PoolRunner
