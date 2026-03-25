from unittest.mock import patch

import pytest

from muffin_kafka.plugin import KafkaPlugin


async def test_base():
    plugin = KafkaPlugin()
    assert plugin.name == "kafka"
    assert plugin.cfg
    assert plugin.__app__ is None


async def test_bind_error_handler(kafka: KafkaPlugin):
    @kafka.handle_error
    async def handle_error(err):
        del err

    assert kafka.error_handler == handle_error


async def test_bind_topics_handler(kafka: KafkaPlugin):
    @kafka.handle_topics("test", "test2")
    async def handle(message):
        del message

    assert kafka.handlers
    assert kafka.handlers["test"] == [handle]
    assert kafka.handlers["test2"] == [handle]


@pytest.mark.parametrize("options", [{"ssl_cafile": "/etc/ssl/certs/ca-certificates.crt"}])
async def test_get_params_adds_ssl_context(kafka: KafkaPlugin):
    with patch("muffin_kafka.plugin.helpers.create_ssl_context") as mock_ssl_ctx:
        mock_ssl_ctx.return_value = "ssl-context"

        params = kafka.get_params()

    assert params.get("ssl_context") == "ssl-context"
    mock_ssl_ctx.assert_called_once_with(cafile="/etc/ssl/certs/ca-certificates.crt")
