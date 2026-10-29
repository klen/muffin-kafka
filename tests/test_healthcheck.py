from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka.errors import KafkaConnectionError

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
        performed = await healthcheck.process()

        assert performed is True
        assert healthcheck.is_healthy is True
        assert bool(healthcheck) is True

    async def test_returns_false_when_lag_exceeds_threshold(self, started_pool, mock_consumer):
        tp = create_topic_partition()
        mock_consumer.assignment.return_value = [tp]
        mock_consumer.end_offsets = AsyncMock(return_value={tp: 100})
        mock_consumer.committed = AsyncMock(return_value=50)

        healthcheck = ConsumerPoolHealthcheck(max_lag=20, pool=started_pool)
        performed = await healthcheck.process()

        assert performed is True
        assert healthcheck.is_healthy is False
        assert bool(healthcheck) is False

    async def test_skips_unassigned_consumer(self, mock_consumer):
        mock_consumer.assignment.return_value = []
        pool = ConsumerPool()
        pool.consumers = [mock_consumer]
        pool.is_started = True

        healthcheck = ConsumerPoolHealthcheck(pool=pool)
        performed = await healthcheck.process()

        assert performed is False
        assert healthcheck.is_healthy is True
        mock_consumer.end_offsets.assert_not_awaited()

    async def test_skips_when_pool_not_started(self, mock_consumer):
        pool = ConsumerPool()
        pool.consumers = [mock_consumer]
        pool.is_started = False
        healthcheck = ConsumerPoolHealthcheck(pool=pool)

        performed = await healthcheck.process()

        assert performed is False
        assert healthcheck.is_healthy is True
        mock_consumer.assignment.assert_not_called()

    async def test_is_healthy_when_pool_has_no_consumers(self):
        pool = ConsumerPool()
        pool.is_started = True
        healthcheck = ConsumerPoolHealthcheck(pool=pool)

        performed = await healthcheck.process()

        assert performed is False
        assert healthcheck.is_healthy is True

    async def test_read_error_marks_unhealthy(self, started_pool, mock_consumer):
        tp = create_topic_partition()
        mock_consumer.assignment.return_value = [tp]
        mock_consumer.end_offsets = AsyncMock(return_value={tp: 100})
        mock_consumer.committed = AsyncMock(side_effect=KafkaConnectionError())

        healthcheck = ConsumerPoolHealthcheck(max_lag=20, pool=started_pool)
        performed = await healthcheck.process()

        assert performed is True
        assert healthcheck.is_healthy is False

    async def test_end_offsets_error_counts_as_not_checked(self, started_pool, mock_consumer):
        mock_consumer.assignment.return_value = [create_topic_partition()]
        mock_consumer.end_offsets = AsyncMock(side_effect=KafkaConnectionError())

        healthcheck = ConsumerPoolHealthcheck(max_lag=20, pool=started_pool)
        performed = await healthcheck.process()

        assert performed is False
        assert healthcheck.is_healthy is True


class TestHealthcheckCommand:
    """Tests for the kafka-healthcheck management command."""

    async def test_command_fails_when_nothing_can_be_checked(self, kafka, mock_consumer):
        mock_consumer.assignment.return_value = []
        command = kafka.app.manage.commands["kafka-healthcheck"]

        with (
            patch("muffin_kafka.consumers.pool.AIOKafkaConsumer", return_value=mock_consumer),
            pytest.raises(SystemExit) as exc,
        ):
            await command("events", max_lag=1000)

        assert exc.value.code == 1
        mock_consumer.start.assert_awaited_once()
        mock_consumer.stop.assert_awaited_once()
        mock_consumer.commit.assert_not_awaited()
        # The check uses its own pool, the plugin's pool stays untouched
        assert kafka.consumer_pool.consumers == []
        assert kafka.consumer_pool.is_started is False

    async def test_command_passes_when_lag_is_within_limit(self, kafka, mock_consumer):
        tp = create_topic_partition()
        mock_consumer.assignment.return_value = [tp]
        mock_consumer.end_offsets = AsyncMock(return_value={tp: 100})
        mock_consumer.committed = AsyncMock(return_value=90)
        command = kafka.app.manage.commands["kafka-healthcheck"]

        with patch("muffin_kafka.consumers.pool.AIOKafkaConsumer", return_value=mock_consumer):
            await command("events", max_lag=1000)

        mock_consumer.stop.assert_awaited_once()
        mock_consumer.commit.assert_not_awaited()

    async def test_command_fails_when_lag_exceeds_limit(self, kafka, mock_consumer):
        tp = create_topic_partition()
        mock_consumer.assignment.return_value = [tp]
        mock_consumer.end_offsets = AsyncMock(return_value={tp: 100})
        mock_consumer.committed = AsyncMock(return_value=50)
        command = kafka.app.manage.commands["kafka-healthcheck"]

        with (
            patch("muffin_kafka.consumers.pool.AIOKafkaConsumer", return_value=mock_consumer),
            pytest.raises(SystemExit) as exc,
        ):
            await command("events", max_lag=20)

        assert exc.value.code == 1
        mock_consumer.stop.assert_awaited_once()

    async def test_command_reuses_started_pool(self, kafka, mock_consumer):
        tp = create_topic_partition()
        mock_consumer.assignment.return_value = [tp]
        mock_consumer.end_offsets = AsyncMock(return_value={tp: 100})
        mock_consumer.committed = AsyncMock(return_value=90)
        kafka.consumer_pool.consumers = [mock_consumer]
        kafka.consumer_pool.is_started = True
        command = kafka.app.manage.commands["kafka-healthcheck"]

        await command("events", max_lag=1000)

        mock_consumer.start.assert_not_awaited()
        mock_consumer.stop.assert_not_awaited()
