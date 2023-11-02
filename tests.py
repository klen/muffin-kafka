import muffin
import pytest

from muffin_kafka import Kafka


@pytest.fixture()
def app():
    return muffin.Application()


@pytest.fixture()
def kafka(app):
    import muffin_kafka

    return muffin_kafka.Plugin(app)


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
    assert kafka.handlers[0][0] == ("test", "test2")


# ruff: noqa: TRY002, ARG001
