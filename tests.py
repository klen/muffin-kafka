from unittest.mock import AsyncMock, MagicMock, patch

import muffin
import pytest

from muffin_kafka import Kafka


async def awaitable(value):
    """Helper to create an awaitable object."""
    return value


@pytest.fixture
def app():
    return muffin.Application()


@pytest.fixture
def options():
    return {}


@pytest.fixture
def kafka(app, options):
    import muffin_kafka

    return muffin_kafka.Plugin(app, **options)


async def test_bind_error_handler(kafka: Kafka):
    @kafka.handle_error
    async def handle_error(err):
        pass

    assert kafka.error_handler == handle_error


async def test_bind_topics_handler(kafka: Kafka):
    @kafka.handle_topics("test", "test2")
    async def handle(message):
        pass

    assert kafka.handlers
    assert kafka.handlers["test"] == [handle]
    assert kafka.handlers["test2"] == [handle]


@pytest.mark.parametrize("options", [{"produce": True}])
async def test_connect_sets_up_producer(kafka: Kafka):
    kafka.producer = AsyncMock()
    with patch("muffin_kafka.plugin.AIOKafkaProducer") as MockProducer:
        producer = AsyncMock()
        MockProducer.return_value = producer

        await kafka.connect(produce=True)

        producer.start.assert_awaited_once()
        assert kafka.producer == producer


@pytest.mark.parametrize("options", [{"produce": False}])
async def test_send_without_producer(kafka: Kafka):
    with pytest.raises(Exception, match="Producer is not enabled"):
        await kafka.send("test", "value")


@pytest.mark.parametrize("options", [{"produce": True}])
async def test_send_and_wait(kafka: Kafka):
    kafka.producer = AsyncMock()

    async def send():
        return "result"

    kafka.producer.send.return_value = send()

    result = await kafka.send_and_wait("test", {"foo": "bar"}, key="k1")
    assert result == "result"

    kafka.producer.send.assert_called_once()


async def test_healthcheck_ok(kafka: Kafka):
    mock_consumer = MagicMock()
    mock_consumer.stop = AsyncMock()

    tp = MagicMock(topic="test", partition=0)
    mock_consumer.assignment.return_value = [tp]
    mock_consumer.end_offsets.return_value = awaitable({tp: 100})
    mock_consumer.committed.return_value = awaitable(90)

    kafka.consumers = {"test": mock_consumer}
    kafka.init_consumers = lambda: awaitable(None)  # type: ignore[]
    result = await kafka.healthcheck(max_lag=20)
    assert result is True


async def test_healthcheck_fail(kafka: Kafka):
    mock_consumer = MagicMock()
    mock_consumer.stop = AsyncMock()

    tp = MagicMock(topic="test", partition=0)
    mock_consumer.assignment.return_value = [tp]
    mock_consumer.end_offsets.return_value = awaitable({tp: 100})
    mock_consumer.committed.return_value = awaitable(50)

    kafka.consumers = {"test": mock_consumer}
    kafka.init_consumers = lambda: awaitable(None)  # type: ignore[]
    result = await kafka.healthcheck(max_lag=20)
    assert result is False
