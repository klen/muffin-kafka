from asyncio import get_running_loop
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from muffin_kafka.plugin import KafkaPlugin


async def test_listen_initializes_missing_consumer(kafka: KafkaPlugin):
    consumer = MagicMock()
    consumer._client._topics = {"events"}
    consumer.start = AsyncMock()

    def init_consumer(*topics, **params):
        del topics, params
        kafka.consumers.append(consumer)
        return consumer

    kafka.init_consumer = MagicMock(side_effect=init_consumer)  # type: ignore[method-assign]

    process_task = MagicMock()
    process_task.add_done_callback = MagicMock()

    def create_task_stub(coro):
        coro.close()
        return process_task

    with patch("muffin_kafka.plugin.create_task", side_effect=create_task_stub):
        await kafka.listen("events", monitor=False)

    kafka.init_consumer.assert_called_once_with("events")
    consumer.start.assert_awaited_once()
    assert process_task in kafka.tasks


async def test_listen_adds_monitor_task(kafka: KafkaPlugin):
    consumer = MagicMock()
    consumer._client._topics = {"events"}
    consumer.start = AsyncMock()
    kafka.consumers = [consumer]

    process_task = MagicMock()
    process_task.add_done_callback = MagicMock()
    monitor_task = MagicMock()
    monitor_task.add_done_callback = MagicMock()

    def create_task_stub(coro):
        if coro.cr_code.co_name == "__process__":
            coro.close()
            return process_task

        coro.close()
        return monitor_task

    with patch("muffin_kafka.plugin.create_task", side_effect=create_task_stub):
        await kafka.listen("events", monitor=True)

    assert process_task in kafka.tasks
    assert monitor_task in kafka.tasks


async def test_shutdown_commits_and_stops_consumers(kafka: KafkaPlugin):
    consumer = MagicMock()
    consumer.commit = AsyncMock()
    consumer.stop = AsyncMock()
    kafka.consumers = [consumer]

    await kafka.shutdown()

    consumer.commit.assert_awaited_once()
    consumer.stop.assert_awaited_once()


@pytest.mark.parametrize("options", [{"group_id": "workers", "listen": False}])
async def test_manage_listen_uses_default_group_id_when_not_passed(kafka: KafkaPlugin):
    consumer = MagicMock()
    consumer._client._topics = {"events"}
    consumer.start = AsyncMock()

    listen_task = get_running_loop().create_future()
    listen_task.set_result(None)

    def create_task_stub(coro):
        coro.close()
        return listen_task

    with (
        patch("muffin_kafka.plugin.AIOKafkaConsumer", return_value=consumer) as mock_consumer,
        patch("muffin_kafka.plugin.create_task", side_effect=create_task_stub),
    ):
        await kafka.app.manage.commands["kafka-listen"]("events", monitor=False)

    mock_consumer.assert_called_once()
    assert mock_consumer.call_args.kwargs["group_id"] == "workers"
