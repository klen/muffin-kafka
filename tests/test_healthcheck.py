from unittest.mock import AsyncMock, MagicMock

import pytest

from muffin_kafka.consumers import ConsumerPool, ConsumerPoolHealthcheck


def create_topic_partition(topic: str = "test", partition: int = 0):
    tp = MagicMock()
    tp.topic = topic
    tp.partition = partition
    return tp


class TestConsumerPoolHealthcheck:
    """Tests for the ConsumerPoolHealthcheck class."""

    @pytest.fixture
    def started_pool(self, mock_consumer):
        pool = ConsumerPool()
        pool.consumers = [mock_consumer]
        pool.is_started = True
        return pool

    async def test_returns_true_when_lag_within_threshold(self, started_pool, mock_consumer):
        tp = create_topic_partition()
        mock_consumer.assignment.return_value = [tp]
        mock_consumer.end_offsets = AsyncMock(return_value={tp: 100})
        mock_consumer.committed = AsyncMock(return_value=90)

        healthcheck = ConsumerPoolHealthcheck(max_lag=20, pool=started_pool)
        await healthcheck.process()

        assert healthcheck.is_healthy is True
        assert bool(healthcheck) is True

    async def test_returns_false_when_lag_exceeds_threshold(self, started_pool, mock_consumer):
        tp = create_topic_partition()
        mock_consumer.assignment.return_value = [tp]
        mock_consumer.end_offsets = AsyncMock(return_value={tp: 100})
        mock_consumer.committed = AsyncMock(return_value=50)

        healthcheck = ConsumerPoolHealthcheck(max_lag=20, pool=started_pool)
        await healthcheck.process()

        assert healthcheck.is_healthy is False
        assert bool(healthcheck) is False

    async def test_skips_unassigned_consumer(self, mock_consumer):
        mock_consumer.assignment.return_value = []
        mock_consumer.end_offsets = AsyncMock()
        pool = ConsumerPool()
        pool.consumers = [mock_consumer]
        pool.is_started = True

        healthcheck = ConsumerPoolHealthcheck(pool=pool)
        await healthcheck.process()

        assert healthcheck.is_healthy is True
        mock_consumer.end_offsets.assert_not_awaited()

    async def test_skips_when_pool_not_started(self, mock_consumer):
        pool = ConsumerPool()
        pool.consumers = [mock_consumer]
        pool.is_started = False
        healthcheck = ConsumerPoolHealthcheck(pool=pool)

        await healthcheck.process()

        assert healthcheck.is_healthy is True
        mock_consumer.assignment.assert_not_called()

    async def test_is_healthy_when_pool_has_no_consumers(self):
        pool = ConsumerPool()
        pool.is_started = True
        healthcheck = ConsumerPoolHealthcheck(pool=pool)

        await healthcheck.process()

        assert healthcheck.is_healthy is True
