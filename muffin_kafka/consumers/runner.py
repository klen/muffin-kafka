"""Consume loops: one asyncio task per consumer, feeding records to the handlers."""

import abc
import asyncio
import dataclasses as dc
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, nullcontext
from typing import Coroutine

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import ConsumerStoppedError
from aiokafka.util import create_task

from muffin_kafka import logger
from muffin_kafka.consumers import ConsumerPool, ConsumerPoolLogger, safe_commit
from muffin_kafka.consumers.handlers import ConsumerHandlers


@dc.dataclass
class PoolRunner(abc.ABC):
    """Start the consumer pool and run one consume loop per consumer.

    ``start()`` starts the consumers, registers one task per consumer and,
    optionally, a ``ConsumerPoolLogger`` task. ``stop()`` signals the loops,
    commits offsets and cancels the tasks. Subclasses implement
    ``_run_consumer``, which decides how records are fetched and when offsets are
    committed.
    """

    pool: ConsumerPool
    handlers: ConsumerHandlers
    enable_auto_commit: bool = True
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

    async def stop(self, *, commit: bool = True):
        """Signal the loops, commit offsets, then cancel and join the tasks.

        The pool is committed and stopped before the tasks are cancelled, so a
        message that is still being handled at this moment may be committed
        before its handler returns.
        """
        self._stop_event.set()

        await self.pool.stop(commit=commit)
        for task in self.tasks:
            task.cancel()

        await asyncio.gather(*self.tasks, return_exceptions=True)

    def register_task(self, coro: Coroutine):
        """Run ``coro`` as a task and log its exception instead of losing it."""
        task = create_task(coro)
        task.add_done_callback(self._handle_task_exception)
        self.tasks.append(task)

    def _handle_task_exception(self, task):
        """Log task crashes; normal stop and cancellation are expected."""
        try:
            exc = task.exception()
            if exc is None or isinstance(exc, (asyncio.CancelledError, ConsumerStoppedError)):
                return
            logger.error("Kafka task crashed: %s", exc)
        except Exception:  # noqa: BLE001
            logger.exception("Kafka task error")

    async def run_consumer(self, consumer):
        """Run one consumer loop inside the configured context manager."""
        async with self._context_factory():
            await self._run_consumer(consumer)

    @abc.abstractmethod
    async def _run_consumer(self, consumer):
        """Consume from ``consumer`` until the runner stops; implemented by subclasses."""
        raise NotImplementedError("Override run_consumer to implement custom processing logic")

    def __await__(self):
        """Wait for all runner tasks, keeping ``kafka-listen`` alive."""
        return asyncio.gather(*self.tasks).__await__()


class SinglePoolRunner(PoolRunner):
    """Consume one record at a time, committing after every handled record.

    With ``enable_auto_commit`` off, the offset is committed only when all
    handlers of the record succeeded. A failed record gets no commit, but the
    next successful commit sends the consumer's current position, which is
    already past the failed record: it is not retried while this consumer runs.
    """

    async def _run_consumer(self, consumer: AIOKafkaConsumer):
        stop = self._stop_event
        handlers = self.handlers

        while not stop.is_set():
            try:
                msg = await consumer.getone()
            except ConsumerStoppedError:
                break

            success = await handlers(msg)
            if success and not self.enable_auto_commit:
                await safe_commit(consumer)


@dc.dataclass
class BatchPoolRunner(PoolRunner):
    """Consume in batches with ``getmany()``; handlers still get one record each.

    ``batch_size`` is the ``max_records`` of each ``getmany()`` call (which waits
    up to 100 ms for data), and ``enable_auto_commit`` off means one commit per
    batch when every record in it was handled successfully. Handlers are not
    called with a list: the batch is unpacked and each record is dispatched
    through ``ConsumerHandlers`` individually. A later successful batch commits
    the position past a record that failed earlier.
    """

    batch_size: int = dc.field(default=100)

    async def _run_consumer(self, consumer: AIOKafkaConsumer):
        stop = self._stop_event
        handlers = self.handlers

        while not stop.is_set():
            try:
                data = await consumer.getmany(timeout_ms=100, max_records=self.batch_size)
            except ConsumerStoppedError:
                break

            success = True
            for messages in data.values():
                for msg in messages:
                    if not await handlers(msg):
                        success = False

            if success and not self.enable_auto_commit:
                await safe_commit(consumer)
