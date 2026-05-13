import dataclasses as dc
from asyncio import gather

from aiokafka.consumer.consumer import AIOKafkaConsumer


@dc.dataclass
class ConsumerPool:
    consumers: list[AIOKafkaConsumer] = dc.field(default_factory=list)
    group_id: str | None = None
    params: dict = dc.field(default_factory=dict)
    is_started: bool = False

    def setup(self, group_id: str, **params):
        self.params = params
        self.group_id = group_id

    def init(self, *topics: str, **params):
        topics_to_listen = set(topics)
        missing = topics_to_listen.copy()
        for consumer in self.consumers:
            missing -= set(consumer._client._topics)

        if missing:
            consumer = self.init_consumer(*missing, **params)
            self.consumers.append(consumer)

    def init_consumer(self, *topics: str, **params) -> AIOKafkaConsumer:
        merged = dict(self.params, **params)
        merged["group_id"] = merged.get("group_id") or self.group_id
        return AIOKafkaConsumer(*topics, **merged)

    async def start(self):
        await gather(*[consumer.start() for consumer in self.consumers])
        self.is_started = True

    async def stop(self, *, commit: bool = True):
        if commit:
            await gather(*[consumer.commit() for consumer in self.consumers])

        await gather(*[consumer.stop() for consumer in self.consumers])
        self.is_started = False

    def __iter__(self):
        return iter(self.consumers)
