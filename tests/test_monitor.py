from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from muffin_kafka.consumers import ConsumerPool, ConsumerPoolLogger


class TestConsumerPoolLogger:
    """Tests for the ConsumerPoolLogger class."""

    @pytest.fixture
    def started_pool(self, mock_consumer):
        pool = ConsumerPool()
        pool.consumers = [mock_consumer]
        pool.is_started = True
        return pool

    @pytest.fixture
    def monitor(self, started_pool):
        return ConsumerPoolLogger(interval=0, pool=started_pool)

    async def test_process_logs_warning_when_pool_not_started(self):
        pool = ConsumerPool()
        pool.is_started = False
        monitor = ConsumerPoolLogger(interval=1, pool=pool)

        await monitor.process()

    async def test_calls_process_tp_for_assigned_partitions(self, mock_consumer):
        tp = MagicMock()
        tp.topic = "events"
        tp.partition = 0
        mock_consumer.assignment.return_value = [tp]

        pool = ConsumerPool()
        pool.consumers = [mock_consumer]
        pool.is_started = True
        monitor = ConsumerPoolLogger(interval=1, pool=pool)

        with (
            patch.object(monitor, "process_tp", new_callable=AsyncMock) as mock_process_tp,
            patch("muffin_kafka.consumers.monitors.sleep", side_effect=RuntimeError("stop")),
            pytest.raises(RuntimeError, match="stop"),
        ):
            await monitor()

        mock_process_tp.assert_awaited_once_with(mock_consumer, tp, 0)

    async def test_process_tp_outputs_metrics(self, monitor, mock_consumer):
        tp = MagicMock()
        tp.topic = "events"
        tp.partition = 0
        mock_consumer.assignment.return_value = [tp]
        mock_consumer.end_offsets = AsyncMock(return_value={tp: 100})
        mock_consumer.position = AsyncMock(return_value=10)
        mock_consumer.committed = AsyncMock(return_value=90)
        mock_consumer.last_poll_timestamp.return_value = 1_000_000

        with patch("muffin_kafka.consumers.monitors.time", return_value=1000):
            await monitor.process_tp(mock_consumer, tp, 100)

    async def test_process_tp_handles_per_partition_errors(self, monitor, mock_consumer):
        tp = MagicMock()
        tp.topic = "events"
        tp.partition = 0
        mock_consumer.assignment.return_value = [tp]
        mock_consumer.end_offsets = AsyncMock(return_value={tp: 100})
        mock_consumer.position = AsyncMock(side_effect=RuntimeError("kafka error"))

        await monitor.process_tp(mock_consumer, tp, 100)

    async def test_skips_consumer_with_no_assignments(self, monitor, mock_consumer):
        mock_consumer.assignment.return_value = []

        await monitor.process_consumer(mock_consumer)

        mock_consumer.end_offsets.assert_not_awaited()
