from unittest.mock import AsyncMock, patch

import pytest

from muffin_kafka.plugin import KafkaPlugin


class TestProducerStartup:
    """Tests for producer initialization during startup."""

    @pytest.mark.parametrize("options", [{"produce": True, "listen": False}])
    async def test_starts_producer_when_enabled(self, kafka: KafkaPlugin):
        """When produce=True, startup should create and start a producer."""
        with patch("muffin_kafka.plugin.AIOKafkaProducer") as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer

            await kafka.startup()

        mock_producer_class.assert_called_once()
        mock_producer.start.assert_awaited_once()
        assert kafka.producer == mock_producer


class TestSend:
    """Tests for the send() method."""

    @pytest.mark.parametrize("options", [{"produce": False}])
    async def test_raises_when_producer_not_enabled(self, kafka: KafkaPlugin):
        """Should raise PluginError when trying to send but producer is not enabled."""
        with pytest.raises(Exception, match="Producer is not enabled"):
            await kafka.send("test", "value")

    async def test_raises_when_producer_not_initialized(self, app):
        """Should raise PluginError when producer is enabled but not initialized."""
        kafka = KafkaPlugin(app, produce=True)
        # producer is None because startup() wasn't called

        with pytest.raises(Exception, match="Producer is not initialized"):
            await kafka.send("test", "value")

    async def test_encodes_dict_to_json_bytes(self, app):
        """Should encode dict values to JSON bytes."""
        kafka = KafkaPlugin(app, produce=True)
        kafka.producer = AsyncMock()

        await kafka.send("events", {"ok": True})

        _topic, value = kafka.producer.send.await_args.args
        assert isinstance(value, bytes)
        assert b'"ok":true' in value

    async def test_encodes_string_key_to_bytes(self, app):
        """Should encode string keys to UTF-8 bytes."""
        kafka = KafkaPlugin(app, produce=True)
        kafka.producer = AsyncMock()

        await kafka.send("events", {"data": "test"}, key="user-1")

        key = kafka.producer.send.await_args.kwargs["key"]
        assert key == b"user-1"

    async def test_passes_through_additional_params(self, app):
        """Should pass additional parameters (partition, headers, etc.) to the producer."""
        kafka = KafkaPlugin(app, produce=True)
        kafka.producer = AsyncMock()

        await kafka.send("events", {"data": "test"}, partition=1, headers={"x-trace": "123"})

        kwargs = kafka.producer.send.await_args.kwargs
        assert kwargs["partition"] == 1
        assert kwargs["headers"] == {"x-trace": "123"}
