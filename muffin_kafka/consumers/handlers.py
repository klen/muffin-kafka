import dataclasses as dc
from collections import defaultdict
from typing import Any, Awaitable, Callable

from aiokafka.structs import ConsumerRecord

from muffin_kafka import logger

TCallable = Callable[..., Awaitable[Any]]
TErrCallable = Callable[[BaseException], Awaitable[Any]]


@dc.dataclass
class ConsumerHandlers:
    handlers: dict[str, list[TCallable]] = dc.field(
        default_factory=lambda: defaultdict(list),
    )
    error_handler: TErrCallable | None = None

    def get_topics(self) -> set[str]:
        return set(self.handlers.keys())

    def set_handler(self, handler: TCallable, *topics: str):
        for topic in topics:
            self.handlers[topic].append(handler)

    def set_error_handler(self, handler: TErrCallable):
        self.error_handler = handler

    async def __call__(self, msg: ConsumerRecord):
        logger.debug("Kafka msg: %s-%s-%s", msg.topic, msg.partition, msg.offset)
        for fn in self.handlers[msg.topic]:
            try:
                await fn(msg)
            except Exception as exc:  # noqa: PERF203, BLE001
                logger.exception("Kafka: Error while processing message: %r", msg)
                if self.error_handler:
                    await self.error_handler(exc)
