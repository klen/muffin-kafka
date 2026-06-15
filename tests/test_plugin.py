from unittest.mock import patch

from muffin_kafka.plugin import KafkaPlugin


class TestPluginInitialization:
    """Tests for plugin creation and basic configuration."""

    async def test_has_default_name_and_config(self):
        """Plugin should have default name 'kafka' and configuration."""
        plugin = KafkaPlugin()

        assert plugin.name == "kafka"
        assert plugin.cfg is not None
        assert plugin.__app__ is None


class TestErrorHandlerBinding:
    """Tests for the @handle_error decorator."""

    async def test_registers_error_handler_function(self, kafka: KafkaPlugin):
        """The @handle_error decorator should register the decorated function as error handler."""

        @kafka.handle_error
        async def handle_error(err):
            del err

        assert kafka.handlers.error_handler == handle_error


class TestTopicsHandlerBinding:
    """Tests for the @handle_topics decorator."""

    async def test_registers_handler_for_multiple_topics(self, kafka: KafkaPlugin):
        """The @handle_topics decorator should register handler for all specified topics."""

        @kafka.handle_topics("test", "test2")
        async def handle(message):
            del message

        assert kafka.handlers.handlers["test"] == [handle]
        assert kafka.handlers.handlers["test2"] == [handle]


class TestCommonParams:
    """Tests for the get_common_params() method."""

    async def test_creates_ssl_context_when_cafile_configured(self, app):
        """Should create SSL context when ssl_cafile is configured."""
        with patch("muffin_kafka.plugin.helpers.create_ssl_context") as mock_ssl_ctx:
            mock_ssl_ctx.return_value = "ssl-context"
            kafka = KafkaPlugin(app, ssl_cafile="/etc/ssl/certs/ca-certificates.crt")

            params = kafka.get_common_params()

        assert params.get("ssl_context") == "ssl-context"
        mock_ssl_ctx.assert_called_with(cafile="/etc/ssl/certs/ca-certificates.crt")

    async def test_does_not_include_ssl_context_without_cafile(self, app):
        """Should not include ssl_context when ssl_cafile is not set."""
        kafka = KafkaPlugin(app)

        params = kafka.get_common_params()

        assert "ssl_context" not in params


class TestSetupCommands:
    """Tests for the setup_commands configuration option."""

    async def test_setup_commands_defaults_to_true(self):
        """setup_commands should default to True."""
        plugin = KafkaPlugin()

        assert plugin.defaults["setup_commands"] is True

    async def test_registers_commands_when_setup_commands_is_true(self, app):
        """Management commands should be registered when setup_commands is True."""
        kafka = KafkaPlugin(app, setup_commands=True)

        assert "kafka-healthcheck" in kafka.app.manage.commands
        assert "kafka-listen" in kafka.app.manage.commands

    async def test_skips_commands_when_setup_commands_is_false(self, app):
        """Management commands should NOT be registered when setup_commands is False."""
        kafka = KafkaPlugin(app, setup_commands=False)

        assert "kafka-healthcheck" not in kafka.app.manage.commands
        assert "kafka-listen" not in kafka.app.manage.commands
