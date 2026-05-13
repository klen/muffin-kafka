from asyncio import Future
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from muffin_kafka.consumers import ConsumerHandlers, ConsumerPool
from muffin_kafka.consumers.runner import BatchPoolRunner, SinglePoolRunner
from muffin_kafka.plugin import KafkaPlugin


class TestListen:
    """Tests for the listen() method."""

    async def test_creates_single_runner_when_batch_size_none(
        self,
        kafka: KafkaPlugin,
        mock_consumer,
    ):
        mock_consumer._client._topics = {"events"}
        mock_consumer.getone = AsyncMock(side_effect=RuntimeError("stop"))
        kafka.consumer_pool.consumers = [mock_consumer]

        with patch.object(kafka.consumer_pool, "start", new_callable=AsyncMock):
            await kafka.listen("events", monitor=False)
            await kafka.shutdown()

        assert isinstance(kafka.runner, SinglePoolRunner)
        assert kafka.runner.pool == kafka.consumer_pool
        assert kafka.runner.handlers == kafka.handlers

    @pytest.mark.parametrize("options", [{"batch_size": 10}])
    async def test_creates_batch_runner_when_batch_size_set(
        self,
        kafka: KafkaPlugin,
        mock_consumer,
    ):
        mock_consumer._client._topics = {"events"}

        async def getmany(**kwargs):
            del kwargs
            raise RuntimeError("stop")

        mock_consumer.getmany = getmany
        kafka.consumer_pool.consumers = [mock_consumer]

        with patch.object(kafka.consumer_pool, "start", new_callable=AsyncMock):
            await kafka.listen("events", monitor=False)
            await kafka.shutdown()

        assert isinstance(kafka.runner, BatchPoolRunner)
        assert kafka.runner.batch_size == 10

    async def test_passes_setup_params_to_consumer(self, kafka: KafkaPlugin):
        kafka.consumer_pool.setup(
            group_id="test-group",
            bootstrap_servers="kafka:9092",
            client_id="test-client",
        )
        consumer = MagicMock()

        with patch(
            "muffin_kafka.consumers.pool.AIOKafkaConsumer",
            return_value=consumer,
        ) as mock_class:
            kafka.consumer_pool.init_consumer("events")

        mock_class.assert_called_once()
        call_kwargs = mock_class.call_args.kwargs
        assert call_kwargs["group_id"] == "test-group"
        assert call_kwargs["bootstrap_servers"] == "kafka:9092"
        assert call_kwargs["client_id"] == "test-client"

    async def test_pool_starts_before_creating_tasks(self, mock_consumer):
        call_order = []
        pool = ConsumerPool()
        pool.consumers = [mock_consumer]
        pool.is_started = False
        handlers = ConsumerHandlers()
        runner = SinglePoolRunner(pool=pool, handlers=handlers)

        async def fake_pool_start():
            call_order.append("pool.start")
            pool.is_started = True

        pool.start = fake_pool_start

        def fake_register_task(coro):
            call_order.append("register_task")
            coro.close()

        runner.register_task = fake_register_task

        await runner.start()

        assert call_order == ["pool.start", "register_task"]

    async def test_starts_pool(self, kafka: KafkaPlugin, mock_consumer):
        mock_consumer._client._topics = {"events"}

        async def getone():
            raise RuntimeError("stop")

        mock_consumer.getone = getone
        kafka.consumer_pool.consumers = [mock_consumer]

        with patch.object(kafka.consumer_pool, "start", new_callable=AsyncMock) as mock_start:
            await kafka.listen("events", monitor=False)
            await kafka.shutdown()

        mock_start.assert_awaited_once()

    async def test_passes_batch_size_from_manage_command(self, kafka: KafkaPlugin):
        consumer = MagicMock()
        consumer._client._topics = {"events"}
        consumer.start = AsyncMock()

        done_future = Future()
        done_future.set_result(None)

        with (
            patch(
                "muffin_kafka.consumers.pool.AIOKafkaConsumer",
                return_value=consumer,
            ) as mock_consumer_class,
            patch("muffin_kafka.consumers.runner.create_task"),
            patch("muffin_kafka.consumers.runner.gather", return_value=done_future),
        ):
            await kafka.app.manage.commands["kafka-listen"]("events", monitor=False, batch_size=5)

        mock_consumer_class.assert_called_once()
        assert isinstance(kafka.runner, BatchPoolRunner)
        assert kafka.runner.batch_size == 5

    async def test_coerces_batch_size_from_string(self, kafka: KafkaPlugin):
        consumer = MagicMock()
        consumer._client._topics = {"events"}
        consumer.start = AsyncMock()

        done_future = Future()
        done_future.set_result(None)

        with (
            patch(
                "muffin_kafka.consumers.pool.AIOKafkaConsumer",
                return_value=consumer,
            ) as mock_consumer_class,
            patch("muffin_kafka.consumers.runner.create_task"),
            patch("muffin_kafka.consumers.runner.gather", return_value=done_future),
        ):
            await kafka.app.manage.commands["kafka-listen"]("events", monitor=False, batch_size="5")

        mock_consumer_class.assert_called_once()
        assert isinstance(kafka.runner, BatchPoolRunner)
        assert kafka.runner.batch_size == 5


class TestShutdown:
    """Tests for the shutdown() method."""

    @pytest.mark.parametrize("options", [{"listen": True}])
    async def test_stops_runner_and_producer(self, kafka: KafkaPlugin, mock_consumer):
        kafka.consumer_pool.consumers = [mock_consumer]
        kafka.runner = MagicMock()
        kafka.runner.stop = AsyncMock()

        await kafka.shutdown()

        kafka.runner.stop.assert_awaited_once()

    @pytest.mark.parametrize("options", [{"listen": False}])
    async def test_skips_runner_when_none(self, kafka: KafkaPlugin):
        kafka.runner = None

        await kafka.shutdown()


class TestRunner:
    """Tests for the consumer pool runners."""

    @pytest.fixture
    def mock_messages(self):
        msg1 = MagicMock(topic="events", partition=0, offset=0)
        msg2 = MagicMock(topic="events", partition=0, offset=1)
        return msg1, msg2

    async def register_handler(self, kafka: KafkaPlugin):
        handler_calls = []

        @kafka.handle_topics("events")
        async def handler(message):
            handler_calls.append(message)

        return handler_calls

    async def test_single_runner_calls_handler_per_message(self, kafka: KafkaPlugin, mock_messages):
        msg1, msg2 = mock_messages
        handler_calls = await self.register_handler(kafka)

        mock_consumer = AsyncMock()
        call_count = 0

        async def getone_side():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return msg1
            if call_count == 2:
                return msg2
            raise RuntimeError("stopped")

        mock_consumer.getone = AsyncMock(side_effect=getone_side)
        pool = ConsumerPool()
        pool.consumers = [mock_consumer]

        runner = SinglePoolRunner(pool=pool, handlers=kafka.handlers)

        with pytest.raises(RuntimeError, match="stopped"):
            await runner.run_consumer(mock_consumer)

        assert len(handler_calls) == 2
        assert handler_calls[0] == msg1
        assert handler_calls[1] == msg2

    @pytest.mark.filterwarnings("ignore:coroutine.*was never awaited:RuntimeWarning")
    @pytest.mark.parametrize("options", [{"batch_size": 10}])
    async def test_batch_runner_calls_handler_per_message(self, kafka: KafkaPlugin, mock_messages):
        msg1, msg2 = mock_messages
        handler_calls = await self.register_handler(kafka)

        mock_consumer = AsyncMock()
        tp = MagicMock()
        call_count = 0

        async def getmany_side(**kwargs):
            nonlocal call_count
            del kwargs
            call_count += 1
            if call_count == 1:
                return {tp: [msg1, msg2]}
            raise RuntimeError("stopped")

        mock_consumer.getmany = AsyncMock(side_effect=getmany_side)
        pool = ConsumerPool()
        pool.consumers = [mock_consumer]

        runner = BatchPoolRunner(pool=pool, handlers=kafka.handlers, batch_size=10)

        with pytest.raises(RuntimeError, match="stopped"):
            await runner.run_consumer(mock_consumer)

        assert len(handler_calls) == 2
        assert handler_calls[0] == msg1
        assert handler_calls[1] == msg2
        mock_consumer.getmany.assert_awaited()
        assert mock_consumer.getmany.await_args.kwargs.get("max_records") == 10
        assert mock_consumer.getmany.await_args.kwargs.get("timeout_ms") == 100

    async def test_single_runner_calls_error_handler_on_failure(self, kafka: KafkaPlugin):
        handler_calls = []
        error_calls = []

        @kafka.handle_topics("events")
        async def handler(message):
            handler_calls.append(message)
            raise RuntimeError("handler error")

        async def error_handler(exc):
            error_calls.append(exc)

        kafka.handlers.set_error_handler(error_handler)

        mock_consumer = AsyncMock()
        mock_consumer.getone = AsyncMock(
            side_effect=[MagicMock(topic="events"), RuntimeError("stop")],
        )
        pool = ConsumerPool()
        pool.consumers = [mock_consumer]

        runner = SinglePoolRunner(pool=pool, handlers=kafka.handlers)

        with pytest.raises(RuntimeError, match="stop"):
            await runner.run_consumer(mock_consumer)

        assert len(handler_calls) == 1
        assert len(error_calls) == 1
