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
