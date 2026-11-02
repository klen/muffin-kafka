import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka.errors import NodeNotReadyError
from aiokafka.structs import OffsetAndMetadata, TopicPartition

from muffin_kafka.consumers import ConsumerPool, ConsumerPoolHealthcheck
from muffin_kafka.plugin import KafkaPlugin


def make_pool(group_id="group", **params):
    pool = ConsumerPool()
    pool.setup(group_id=group_id, **params)
    return pool


def describe_topic(topic, partitions):
    return {
        "topic": topic,
        "error_code": 0,
        "partitions": [{"partition": partition} for partition in partitions],
    }


def make_admin(committed, described=None):
    admin = MagicMock()
    admin.start = AsyncMock()
    admin.list_consumer_group_offsets = AsyncMock(return_value=committed)
    admin.describe_topics = AsyncMock(return_value=described or [])
    admin.close = AsyncMock()
    return admin


def make_probe_consumer(end_offsets=None):
    consumer = MagicMock()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    consumer.assign = MagicMock()
    consumer.end_offsets = AsyncMock(return_value=end_offsets or {})
    return consumer


async def test_probe_never_subscribes_and_reads_lag_without_group_membership():
    pool = make_pool(bootstrap_servers="kafka:9092", retry_backoff_ms=1)
    tp = TopicPartition("events", 0)
    admin = make_admin(
        {tp: OffsetAndMetadata(90, "")},
        described=[describe_topic("events", [0])],
    )
    consumer = make_probe_consumer({tp: 100})

    with (
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaAdminClient",
            return_value=admin,
        ),
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaConsumer",
            return_value=consumer,
        ) as consumer_class,
    ):
        result = await ConsumerPoolHealthcheck(
            pool,
            topics=("events",),
            max_lag=20,
        ).process()

    assert result.performed is True
    assert result.healthy is True
    assert result.failed == []
    assert result.uncommitted == []
    # The probe consumer is created without topics or a group, so it can only
    # be manually assigned partitions, never subscribe or join the group.
    assert consumer_class.call_args.args == ()
    assert "group_id" not in consumer_class.call_args.kwargs
    consumer.subscribe.assert_not_called()
    consumer.assignment.assert_not_called()
    consumer.assign.assert_called_once_with([tp])
    admin.describe_topics.assert_awaited_once_with(["events"])
    admin.list_consumer_group_offsets.assert_awaited_once_with("group")


async def test_probe_consumer_forces_auto_commit_off():
    pool = make_pool(bootstrap_servers="kafka:9092")
    tp = TopicPartition("events", 0)
    admin = make_admin(
        {tp: OffsetAndMetadata(90, "")},
        described=[describe_topic("events", [0])],
    )
    consumer = make_probe_consumer({tp: 100})

    with (
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaAdminClient",
            return_value=admin,
        ),
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaConsumer",
            return_value=consumer,
        ) as consumer_class,
    ):
        await ConsumerPoolHealthcheck(
            pool,
            topics=("events",),
            max_lag=20,
        ).process()

    kwargs = consumer_class.call_args.kwargs
    assert kwargs["enable_auto_commit"] is False
    assert kwargs["auto_offset_reset"] == "latest"


async def test_missing_committed_offset_is_reported_as_uncommitted_by_default():
    pool = make_pool(bootstrap_servers="kafka:9092")
    tp = TopicPartition("events", 0)
    admin = make_admin({}, described=[describe_topic("events", [0])])
    consumer = make_probe_consumer({tp: 100})

    with (
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaAdminClient",
            return_value=admin,
        ),
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaConsumer",
            return_value=consumer,
        ),
    ):
        result = await ConsumerPoolHealthcheck(
            pool,
            topics=("events",),
            max_lag=1000,
            require_committed=True,
        ).process()

    assert result.performed is True
    assert result.healthy is False
    assert result.uncommitted == [tp]
    assert result.failed == [tp]


async def test_missing_committed_offset_is_allowed_when_not_required():
    pool = make_pool(bootstrap_servers="kafka:9092")
    tp = TopicPartition("events", 0)
    admin = make_admin({}, described=[describe_topic("events", [0])])
    consumer = make_probe_consumer({tp: 100})

    with (
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaAdminClient",
            return_value=admin,
        ),
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaConsumer",
            return_value=consumer,
        ),
    ):
        result = await ConsumerPoolHealthcheck(
            pool,
            topics=("events",),
            max_lag=1000,
            require_committed=False,
        ).process()

    assert result.performed is True
    assert result.healthy is True
    assert result.uncommitted == [tp]
    assert result.failed == []


async def test_end_offsets_node_not_ready_is_retried():
    pool = make_pool(bootstrap_servers="kafka:9092", retry_backoff_ms=1)
    tp = TopicPartition("events", 0)
    admin = make_admin(
        {tp: OffsetAndMetadata(90, "")},
        described=[describe_topic("events", [0])],
    )
    consumer = make_probe_consumer()
    consumer.end_offsets = AsyncMock(
        side_effect=[NodeNotReadyError(), {tp: 100}],
    )

    with (
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaAdminClient",
            return_value=admin,
        ),
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaConsumer",
            return_value=consumer,
        ),
    ):
        result = await ConsumerPoolHealthcheck(
            pool,
            topics=("events",),
            max_lag=20,
            timeout=5,
        ).process()

    assert result.performed is True
    assert result.healthy is True
    assert consumer.end_offsets.await_count == 2


async def test_healthcheck_times_out_instead_of_hanging():
    pool = make_pool(bootstrap_servers="kafka:9092", retry_backoff_ms=1)

    async def hang():
        await asyncio.sleep(10)

    admin = make_admin({})
    admin.start = AsyncMock(side_effect=hang)

    with (
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaAdminClient",
            return_value=admin,
        ),
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaConsumer",
            return_value=make_probe_consumer(),
        ),
    ):
        result = await ConsumerPoolHealthcheck(
            pool,
            topics=("events",),
            timeout=0.05,
        ).process()

    assert result.performed is False
    assert result.healthy is False
    assert "timed out" in result.errors[0]
    admin.close.assert_awaited_once()


async def test_command_passes_when_lag_is_within_limit(kafka):
    kafka.consumer_pool.group_id = "group"
    tp = TopicPartition("events", 0)
    admin = make_admin(
        {tp: OffsetAndMetadata(90, "")},
        described=[describe_topic("events", [0])],
    )
    consumer = make_probe_consumer({tp: 100})
    command = kafka.app.manage.commands["kafka-healthcheck"]

    with (
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaAdminClient",
            return_value=admin,
        ),
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaConsumer",
            return_value=consumer,
        ),
    ):
        await command("events", max_lag=1000)

    consumer.stop.assert_awaited_once()
    admin.close.assert_awaited_once()


async def test_command_fails_when_lag_exceeds_limit(kafka):
    kafka.consumer_pool.group_id = "group"
    tp = TopicPartition("events", 0)
    admin = make_admin(
        {tp: OffsetAndMetadata(50, "")},
        described=[describe_topic("events", [0])],
    )
    consumer = make_probe_consumer({tp: 100})
    command = kafka.app.manage.commands["kafka-healthcheck"]

    with (
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaAdminClient",
            return_value=admin,
        ),
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaConsumer",
            return_value=consumer,
        ),
        pytest.raises(SystemExit) as exc,
    ):
        await command("events", max_lag=20)

    assert exc.value.code == 1
    consumer.stop.assert_awaited_once()


async def test_command_fails_when_nothing_can_be_checked(kafka):
    command = kafka.app.manage.commands["kafka-healthcheck"]

    with (
        patch("muffin_kafka.consumers.monitors.AIOKafkaAdminClient") as admin_class,
        patch("muffin_kafka.consumers.monitors.AIOKafkaConsumer") as consumer_class,
        pytest.raises(SystemExit) as exc,
    ):
        await command(max_lag=1000)

    assert exc.value.code == 1
    admin_class.assert_not_called()
    consumer_class.assert_not_called()


async def test_command_names_broken_topic(kafka):
    kafka.consumer_pool.group_id = "group"
    tp = TopicPartition("events", 0)
    admin = make_admin(
        {tp: OffsetAndMetadata(90, "")},
        described=[describe_topic("events", [0])],
    )
    consumer = make_probe_consumer({tp: 100})
    command = kafka.app.manage.commands["kafka-healthcheck"]
    kafka.app.logger = MagicMock()

    with (
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaAdminClient",
            return_value=admin,
        ),
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaConsumer",
            return_value=consumer,
        ),
        pytest.raises(SystemExit) as exc,
    ):
        await command("events", "broken", max_lag=1000)

    assert exc.value.code == 1
    assert "broken" in str(kafka.app.logger.error.call_args)


async def test_command_accepts_require_committed_and_timeout(kafka):
    kafka.consumer_pool.group_id = "group"
    tp = TopicPartition("events", 0)
    admin = make_admin({}, described=[describe_topic("events", [0])])
    consumer = make_probe_consumer({tp: 100})
    command = kafka.app.manage.commands["kafka-healthcheck"]

    with (
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaAdminClient",
            return_value=admin,
        ),
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaConsumer",
            return_value=consumer,
        ),
        patch(
            "muffin_kafka.plugin.ConsumerPoolHealthcheck",
            wraps=ConsumerPoolHealthcheck,
        ) as healthcheck_class,
    ):
        await command("events", max_lag=1000, require_committed=False, timeout="12.5")

    kwargs = healthcheck_class.call_args.kwargs
    assert kwargs["require_committed"] is False
    assert kwargs["timeout"] == 12.5


async def test_command_returns_nonzero_on_timeout(app):
    kafka = KafkaPlugin(
        app,
        group_id="group",
        request_timeout_ms=50,
        retry_backoff_ms=1,
    )
    admin = make_admin({})

    async def hang():
        await asyncio.sleep(10)

    admin.start = AsyncMock(side_effect=hang)
    command = kafka.app.manage.commands["kafka-healthcheck"]

    with (
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaAdminClient",
            return_value=admin,
        ),
        patch(
            "muffin_kafka.consumers.monitors.AIOKafkaConsumer",
            return_value=make_probe_consumer(),
        ),
        pytest.raises(SystemExit) as exc,
    ):
        await command("events", max_lag=1000)

    assert exc.value.code == 1
    admin.close.assert_awaited_once()
