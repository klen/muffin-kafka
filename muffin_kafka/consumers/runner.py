import abc
import dataclasses as dc
from asyncio import gather
from typing import Coroutine

from aiokafka import AIOKafkaConsumer
from aiokafka.util import create_task

from muffin_kafka import logger
from muffin_kafka.consumers import ConsumerPool, ConsumerPoolLogger
from muffin_kafka.consumers.handlers import ConsumerHandlers


@dc.dataclass
class PoolRunner(abc.ABC):
    pool: ConsumerPool
    handlers: ConsumerHandlers
    tasks: list = dc.field(default_factory=list)

    async def start(self, monitor: int | None = None):
        for consumer in self.pool:
            self.register_task(self.run_consumer(consumer))

        if monitor:
            logger.info("Starting Kafka consumer pool monitor with interval %s seconds", monitor)
            pool_logger = ConsumerPoolLogger(pool=self.pool, interval=monitor)
            self.register_task(pool_logger())

        await self.pool.start()

    async def stop(self, *, commit: bool = True):
        await self.pool.stop(commit=commit)
        for task in self.tasks:
            task.cancel()

        await gather(*self.tasks, return_exceptions=True)

    def register_task(self, coro: Coroutine):
        task = create_task(coro)
        task.add_done_callback(self._handle_task_exception)
        self.tasks.append(task)

    def _handle_task_exception(self, task):
        try:
            exc = task.exception()
            if exc:
                logger.error("Kafka task crashed: %s", exc)
        except Exception:  # noqa: BLE001
            logger.exception("Kafka task error")

    @abc.abstractmethod
    async def run_consumer(self, consumer):
        raise NotImplementedError("Override run_consumer to implement custom processing logic")

    def __await__(self):
        return gather(*self.tasks).__await__()


class SinglePoolRunner(PoolRunner):
    async def run_consumer(self, consumer: AIOKafkaConsumer):
        while True:
            msg = await consumer.getone()
            await self.handlers(msg)


@dc.dataclass
class BatchPoolRunner(PoolRunner):
    batch_size: int = dc.field(default=100)

    async def run_consumer(self, consumer: AIOKafkaConsumer):
        while True:
            data = await consumer.getmany(timeout_ms=100, max_records=self.batch_size)
            for messages in data.values():
                for msg in messages:
                    await self.handlers(msg)
