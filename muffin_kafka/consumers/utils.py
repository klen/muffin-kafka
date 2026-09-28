from aiokafka.consumer.consumer import AIOKafkaConsumer
from aiokafka.errors import CommitFailedError

from muffin_kafka import logger


async def safe_commit(consumer: AIOKafkaConsumer) -> None:
    """Commit consumed offsets, tolerating a concurrent group rebalance.

    ``CommitFailedError`` means the consumer group has rebalanced and the
    current generation is no longer valid. The consumer will rejoin the group
    and the uncommitted messages will be reprocessed, so a failed commit must
    not break the consuming loop.
    """
    try:
        await consumer.commit()
    except CommitFailedError as exc:
        logger.warning("Kafka: Offset commit skipped (group rebalanced): %s", exc)
