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
    pool: ConsumerPool

    async def process(self):
        if not self.pool.is_started:
            logger.warning(
                "[Kafka Processor] Consumer pool is not started. Processor will not run.",
            )
            return

        for consumer in self.pool:
            await self.process_consumer(consumer)

    async def process_consumer(self, consumer: AIOKafkaConsumer):
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
        raise NotImplementedError("Override process_tp to implement custom processing logic")


@dc.dataclass
class ConsumerPoolLogger(ConsumerPoolMonitor):
    interval: int

    async def __call__(self, interval: int | None = None):
        interval = self.interval if interval is None else interval

        while interval:
            await self.process()
            await sleep(interval)

    async def process_tp(self, consumer: AIOKafkaConsumer, tp: TopicPartition, end_offset: int):
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
    max_lag: int = 100
    is_healthy: bool = True

    async def process_tp(self, consumer: AIOKafkaConsumer, tp: TopicPartition, end_offset: int):
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
        return self.is_healthy
