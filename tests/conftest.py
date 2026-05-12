from unittest.mock import AsyncMock, MagicMock

import muffin
import pytest

from muffin_kafka.plugin import KafkaPlugin


@pytest.fixture
def aiolib():
    return "asyncio"


@pytest.fixture
def app():
    return muffin.Application()


@pytest.fixture
def options():
    return {}


@pytest.fixture
def kafka(app, options):
    return KafkaPlugin(app, **options)


@pytest.fixture
def mock_consumer():
    """Create a mock Kafka consumer with common attributes pre-configured."""
    consumer = MagicMock()
    consumer._client._topics = set()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    consumer.commit = AsyncMock()
    consumer.assignment = MagicMock(return_value=[])
    consumer.end_offsets = AsyncMock(return_value={})
    consumer.committed = AsyncMock(return_value=0)
    return consumer


@pytest.fixture
def mock_producer():
    """Create a mock Kafka producer."""
    producer = AsyncMock()
    return producer
