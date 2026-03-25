from unittest.mock import AsyncMock, MagicMock

from muffin_kafka.plugin import KafkaPlugin


async def awaitable(value):
    return value


async def test_healthcheck_ok(kafka: KafkaPlugin):
    mock_consumer = MagicMock()
    mock_consumer.stop = AsyncMock()

    tp = MagicMock(topic="test", partition=0)
    mock_consumer.assignment.return_value = [tp]
    mock_consumer.end_offsets.return_value = awaitable({tp: 100})
    mock_consumer.committed.return_value = awaitable(90)

    kafka.consumers = [mock_consumer]
    result = await kafka.healthcheck(max_lag=20)
    assert result is True


async def test_healthcheck_fail(kafka: KafkaPlugin):
    mock_consumer = MagicMock()
    mock_consumer.stop = AsyncMock()

    tp = MagicMock(topic="test", partition=0)
    mock_consumer.assignment.return_value = [tp]
    mock_consumer.end_offsets.return_value = awaitable({tp: 100})
    mock_consumer.committed.return_value = awaitable(50)

    kafka.consumers = [mock_consumer]
    result = await kafka.healthcheck(max_lag=20)
    assert result is False


async def test_healthcheck_skips_unassigned_consumer(kafka: KafkaPlugin):
    mock_consumer = MagicMock()
    mock_consumer.assignment.return_value = []
    mock_consumer.end_offsets = AsyncMock()

    kafka.consumers = [mock_consumer]
    result = await kafka.healthcheck()

    assert result is True
    mock_consumer.end_offsets.assert_not_called()
