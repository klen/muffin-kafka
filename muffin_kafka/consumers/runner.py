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
from muffin_kafka.consumers import ConsumerPool, ConsumerPoolLogger
from muffin_kafka.consumers.handlers import ConsumerHandlers


@dc.dataclass
class PoolRunner(abc.ABC):
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
        self._stop_event.set()

        await self.pool.stop(commit=commit)
        for task in self.tasks:
            task.cancel()

        await asyncio.gather(*self.tasks, return_exceptions=True)

    def register_task(self, coro: Coroutine):
        task = create_task(coro)
        task.add_done_callback(self._handle_task_exception)
        self.tasks.append(task)

    def _handle_task_exception(self, task):
        try:
            exc = task.exception()
            if exc is None or isinstance(exc, (asyncio.CancelledError, ConsumerStoppedError)):
                return
            logger.error("Kafka task crashed: %s", exc)
        except Exception:  # noqa: BLE001
            logger.exception("Kafka task error")

    async def run_consumer(self, consumer):
        async with self._context_factory():
            await self._run_consumer(consumer)

    @abc.abstractmethod
    async def _run_consumer(self, consumer):
        raise NotImplementedError("Override run_consumer to implement custom processing logic")

    def __await__(self):
        return asyncio.gather(*self.tasks).__await__()


class SinglePoolRunner(PoolRunner):
    async def _run_consumer(self, consumer: AIOKafkaConsumer):
        while not self._stop_event.is_set():
            try:
                msg = await consumer.getone()
            except ConsumerStoppedError:
                break
            await self.handlers(msg)
            if not self.enable_auto_commit:
                await consumer.commit()


@dc.dataclass
class BatchPoolRunner(PoolRunner):
    batch_size: int = dc.field(default=100)

    async def _run_consumer(self, consumer: AIOKafkaConsumer):
        while not self._stop_event.is_set():
            try:
                data = await consumer.getmany(timeout_ms=100, max_records=self.batch_size)
            except ConsumerStoppedError:
                break
            for messages in data.values():
                for msg in messages:
                    await self.handlers(msg)
            if not self.enable_auto_commit:
                await consumer.commit()
