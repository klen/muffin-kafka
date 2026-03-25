from unittest.mock import AsyncMock, patch

import pytest

from muffin_kafka.plugin import KafkaPlugin


@pytest.mark.parametrize("options", [{"produce": True, "listen": False}])
async def test_connect_sets_up_producer(kafka: KafkaPlugin):
    kafka.producer = AsyncMock()
    with patch("muffin_kafka.plugin.AIOKafkaProducer") as mock_producer:
        producer = AsyncMock()
        mock_producer.return_value = producer

        await kafka.startup()

    producer.start.assert_awaited_once()
    assert kafka.producer == producer


@pytest.mark.parametrize("options", [{"produce": False}])
async def test_send_without_producer(kafka: KafkaPlugin):
    with pytest.raises(Exception, match="Producer is not enabled"):
        await kafka.send("test", "value")


async def test_send_encodes_value_and_key(app):
    kafka = KafkaPlugin(app, produce=True)
    kafka.producer = AsyncMock()

    await kafka.send("events", {"ok": True}, key="user-1", partition=1)

    kafka.producer.send.assert_awaited_once()
    topic, value = kafka.producer.send.await_args.args
    key = kafka.producer.send.await_args.kwargs["key"]
    partition = kafka.producer.send.await_args.kwargs["partition"]

    assert topic == "events"
    assert isinstance(value, bytes)
    assert b'"ok":true' in value
    assert key == b"user-1"
    assert partition == 1
