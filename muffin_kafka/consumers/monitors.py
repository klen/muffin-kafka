"""Observers that periodically read consumer offsets for logging or health checks."""

import abc
import asyncio
import dataclasses as dc
from asyncio import sleep
from collections.abc import Awaitable, Callable
from time import time
from typing import Any

from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.consumer.consumer import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from aiokafka.structs import TopicPartition

from muffin_kafka import logger
from muffin_kafka.consumers.pool import ConsumerPool

# ``AIOKafkaAdminClient`` accepts only these connection options; the pool
# ``params`` also carry consumer-only options (``group_id``, ``enable_auto_commit``, ...).
_ADMIN_PARAMS = frozenset(
    {
        "bootstrap_servers",
        "client_id",
        "connections_max_idle_ms",
        "metadata_max_age_ms",
        "request_timeout_ms",
        "retry_backoff_ms",
        "sasl_kerberos_domain_name",
        "sasl_kerberos_service_name",
        "sasl_mechanism",
        "sasl_oauth_token_provider",
        "sasl_plain_password",
        "sasl_plain_username",
        "security_protocol",
        "ssl_context",
    }
)


def _admin_params(pool: ConsumerPool) -> dict[str, Any]:
    """Return the pool options ``AIOKafkaAdminClient`` accepts."""
    return {key: value for key, value in pool.params.items() if key in _ADMIN_PARAMS}


async def _quiet(operation: Callable[[], Awaitable[Any]], error: str) -> None:
    """Stop/close a client; a failing close must not mask the pass result."""
    try:
        await operation()
    except Exception:  # noqa: BLE001
        logger.exception(error)


@dc.dataclass
class ConsumerPoolMonitor(abc.ABC):
    """Base class for consumer pool observers.

    ``process()`` is the entry point: it returns ``False`` when the pool is not
    started, otherwise it calls ``process_consumer()`` for every consumer, which
    resolves the assignment and calls ``process_tp()`` for every assigned
    partition. The result is ``True`` only when at least one partition was
    inspected. Subclasses only have to implement ``process_tp()``; override
    ``process_consumer()`` or ``process()`` for another granularity.

    The plugin creates only ``ConsumerPoolLogger`` (``monitor`` option). A custom
    monitor is instantiated and run by the user (see the README).
    """

    pool: ConsumerPool

    async def process(self):
        """Run one monitoring pass and report whether anything was inspected.

        Returns ``False`` when the pool is not started, when no consumer has an
        assignment, or when the pool has no consumers: the check could not be
        performed.
        """
        if not self.pool.is_started:
            logger.warning(
                "[Kafka Processor] Consumer pool is not started. Processor will not run.",
            )
            return False

        performed = False
        for consumer in self.pool:
            if await self.process_consumer(consumer):
                performed = True
        return performed

    async def process_consumer(self, consumer: AIOKafkaConsumer) -> bool:
        """Process each partition assigned to ``consumer``.

        Returns ``False`` when the consumer has no assignment, so callers can
        tell "nothing to check" from "checked and fine".
        """
        assigned = consumer.assignment()
        if not assigned:
            return False

        try:
            end_offsets = await consumer.end_offsets(assigned)
        except KafkaError as exc:
            logger.warning("[Kafka Monitor] Failed to fetch end offsets: %s", exc)
            return False

        for tp in assigned:
            await self.process_tp(consumer, tp, end_offsets.get(tp, 0))
        return True

    @abc.abstractmethod
    async def process_tp(
        self,
        consumer: AIOKafkaConsumer,
        tp: TopicPartition,
        end_offset: int,
    ) -> Any:
        """Handle one partition; ``end_offset`` is the broker's latest offset.

        The return value is ignored by ``process()``.
        """
        raise NotImplementedError("Override process_tp to implement custom processing logic")


@dc.dataclass
class ConsumerPoolLogger(ConsumerPoolMonitor):
    """Log position, committed offset, end offset, lag and poll delay per partition.

    ``interval`` is the delay between passes, in seconds, used by ``__call__``:
    that coroutine loops ``process()`` and ``sleep(interval)`` until cancelled and
    is what the runner starts when monitoring is enabled. ``interval=0`` makes
    the loop exit immediately.
    """

    interval: int

    def __post_init__(self):
        self._committed: dict[TopicPartition, int] = {}

    async def process(self):
        """Fetch committed offsets once, then run the per-partition pass."""
        if self.pool.is_started:
            await self._fetch_committed_offsets()
        return await super().process()

    async def __call__(self, interval: int | None = None):
        """Process/sleep loop meant to run as a task; ``interval`` overrides ``self.interval``."""
        interval = self.interval if interval is None else interval

        while interval:
            try:
                await self.process()
            except Exception:  # noqa: BLE001
                logger.exception("[Kafka Monitor] Monitoring pass failed")
            await sleep(interval)

    async def _fetch_committed_offsets(self):
        """Read the group offsets once for the whole pool, with a timeout."""
        self._committed = {}
        group_id = self.pool.group_id
        if not group_id:
            return

        admin = AIOKafkaAdminClient(**_admin_params(self.pool))
        timeout = float(self.pool.params.get("request_timeout_ms") or 30000) / 1000
        try:
            async with asyncio.timeout(timeout):
                await admin.start()
                committed = await admin.list_consumer_group_offsets(group_id)
            self._committed = {tp: meta.offset for tp, meta in committed.items()}
        except TimeoutError:
            logger.warning("[Kafka Monitor] Timed out fetching committed offsets")
        except KafkaError as exc:
            logger.warning("[Kafka Monitor] Failed to fetch committed offsets: %s", exc)
        except Exception:  # noqa: BLE001
            logger.exception("[Kafka Monitor] Failed to fetch committed offsets")
        finally:
            await _quiet(admin.close, "[Kafka Monitor] Failed to close admin client")

    async def process_tp(self, consumer: AIOKafkaConsumer, tp: TopicPartition, end_offset: int):
        """Log one line of metrics for a partition; read errors are logged and ignored."""
        now = int(time() * 1000)
        try:
            pos = await consumer.position(tp)
            committed = self._committed.get(tp, 0)
            last_poll = consumer.last_poll_timestamp(tp)
            lag = end_offset - committed
            poll_delay = now - last_poll if last_poll else None

            logger.info(
                (
                    "[Monitor] %s-%d | pos: %d | committed: %d "
                    "| end: %d | lag: %d | poll_delay: %sms"
                ),
                tp.topic,
                tp.partition,
                pos,
                committed,
                end_offset,
                lag,
                poll_delay,
            )

        except Exception as e:  # noqa: BLE001
            logger.warning(f"[Kafka Monitor] Failed to fetch info for {tp}: {e}")


@dc.dataclass
class ConsumerHealthcheckResult:
    """Outcome of one ``ConsumerPoolHealthcheck.process()`` pass.

    ``performed`` is true when the probe reached a verdict (both offsets were
    fetched); ``healthy`` is false when any partition is lagging, uncommitted
    (with ``require_committed``) or could not be inspected. ``failed`` and
    ``uncommitted`` name the unhealthy partitions; ``errors`` carries
    per-topic/per-pass failures that cannot be attached to a partition.
    """

    performed: bool = False
    healthy: bool = True
    failed: list[TopicPartition] = dc.field(default_factory=list)
    uncommitted: list[TopicPartition] = dc.field(default_factory=list)
    errors: list[str] = dc.field(default_factory=list)

    def __str__(self) -> str:
        """Return a human-readable summary for the healthcheck command log."""
        problems = [
            f"{name} partitions: "
            + ", ".join(sorted(f"{tp.topic}-{tp.partition}" for tp in partitions))
            for name, partitions in (("failed", self.failed), ("uncommitted", self.uncommitted))
            if partitions
        ]
        if self.errors:
            problems.append("; ".join(self.errors))
        return "; ".join(problems)


@dc.dataclass
class ConsumerPoolHealthcheck:
    """Check consumer group lag without joining the consumer group.

    Committed offsets are read with ``AIOKafkaAdminClient`` and end offsets with
    a separate ``AIOKafkaConsumer`` created with no topics and no ``group_id``,
    so the probe never subscribes or triggers a rebalance. ``process()`` returns
    a :class:`ConsumerHealthcheckResult`; the instance itself stays truthy only
    while the last pass was healthy.
    """

    pool: ConsumerPool
    max_lag: int = 100
    require_committed: bool = True
    timeout: float = 30.0
    retry_backoff_ms: int = 1000
    topics: tuple[str, ...] | None = None
    is_healthy: bool = True

    async def process(self) -> ConsumerHealthcheckResult:
        """Run one pass and return its explicit result.

        The whole pass is bounded by ``asyncio.timeout``; retriable
        ``KafkaError``s are retried with ``retry_backoff_ms`` instead of being
        reported as "not performed".
        """
        result = ConsumerHealthcheckResult()
        topics = tuple(self.topics) if self.topics is not None else tuple(self.pool.topics)
        if not topics:
            result.errors.append("no topics to check")
            result.healthy = False
            self.is_healthy = False
            return result

        group_id = self.pool.group_id
        if not group_id:
            result.errors.append("consumer group_id is not configured")
            result.healthy = False
            self.is_healthy = False
            return result

        admin = AIOKafkaAdminClient(**_admin_params(self.pool))
        fetched = await self._collect(admin, group_id, topics, result)
        await _quiet(admin.close, "[Healthcheck] Failed to close admin client")
        if fetched is not None:
            self._evaluate(*fetched, result)
        self.is_healthy = result.healthy
        return result

    async def _collect(
        self,
        admin: AIOKafkaAdminClient,
        group_id: str,
        topics: tuple[str, ...],
        result: ConsumerHealthcheckResult,
    ) -> tuple[dict[TopicPartition, Any], dict[TopicPartition, int], list[TopicPartition]] | None:
        """Fetch committed and end offsets; return ``None`` when the pass fails."""
        consumer = None
        try:
            async with asyncio.timeout(self.timeout):
                await admin.start()
                committed = await self._retry(
                    lambda: admin.list_consumer_group_offsets(group_id),
                    "list consumer group offsets",
                )
                partitions, missing = await self._describe_partitions(admin, topics)
                if missing:
                    result.healthy = False
                    result.errors.extend(
                        f"topic {topic!r} has no partitions or is unknown" for topic in missing
                    )

                end_offsets: dict[TopicPartition, int] = {}
                if partitions:
                    consumer = self._make_consumer()
                    await consumer.start()
                    consumer.assign(partitions)
                    end_offsets = await self._retry(
                        lambda: consumer.end_offsets(partitions),
                        "fetch end offsets",
                    )
                return committed, end_offsets, partitions
        except TimeoutError:
            result.errors.append(f"timed out after {self.timeout}s")
            result.healthy = False
            return None
        except KafkaError as exc:
            result.errors.append(str(exc))
            result.healthy = False
            return None
        except Exception as exc:  # noqa: BLE001
            logger.exception("[Healthcheck] Healthcheck pass failed")
            result.errors.append(str(exc))
            result.healthy = False
            return None
        finally:
            if consumer is not None:
                await _quiet(consumer.stop, "[Healthcheck] Failed to stop probe consumer")

    def _evaluate(
        self,
        committed: dict[TopicPartition, Any],
        end_offsets: dict[TopicPartition, int],
        partitions: list[TopicPartition],
        result: ConsumerHealthcheckResult,
    ) -> None:
        """Compare committed and end offsets and fill ``result``."""
        result.performed = True
        for tp in partitions:
            end = end_offsets.get(tp)
            if end is None:
                result.errors.append(f"{tp} is missing from end offsets")
                result.failed.append(tp)
                result.healthy = False
                logger.error("[Healthcheck] Missing end offset for %s", tp)
                continue

            offset = committed.get(tp)
            committed_offset = offset.offset if offset is not None else None
            if committed_offset is None or committed_offset < 0:
                result.uncommitted.append(tp)
                logger.info("[Healthcheck] %s has no committed offset", tp)
                if self.require_committed:
                    result.failed.append(tp)
                    result.healthy = False
                    continue
                committed_offset = 0

            lag = end - committed_offset
            logger.info(
                "[Healthcheck] %s-%s | committed: %s | end: %s | lag: %s",
                tp.topic,
                tp.partition,
                committed_offset,
                end,
                lag,
            )
            if lag > self.max_lag:
                result.failed.append(tp)
                result.healthy = False
                logger.warning(
                    "Consumer has lag %s on topic %s partition %s",
                    lag,
                    tp.topic,
                    tp.partition,
                )

    def _make_consumer(self) -> AIOKafkaConsumer:
        """Build a group-less, never-subscribing probe consumer."""
        params = {
            key: value
            for key, value in self.pool.params.items()
            if key not in {"group_id", "enable_auto_commit", "auto_offset_reset"}
        }
        params["enable_auto_commit"] = False
        params["auto_offset_reset"] = "latest"
        return AIOKafkaConsumer(**params)

    async def _describe_partitions(
        self,
        admin: AIOKafkaAdminClient,
        topics: tuple[str, ...],
    ) -> tuple[list[TopicPartition], list[str]]:
        """Return every partition of ``topics`` and the topics without metadata."""
        described = await self._retry(
            lambda: admin.describe_topics(list(topics)),
            "describe topics",
        )
        by_topic = {item["topic"]: item for item in described}
        partitions: list[TopicPartition] = []
        missing: list[str] = []
        for topic in topics:
            info = by_topic.get(topic)
            if not info or info.get("error_code") or not info.get("partitions"):
                missing.append(topic)
                continue
            partitions.extend(
                TopicPartition(topic, item["partition"]) for item in info["partitions"]
            )
        return partitions, missing

    async def _retry(self, operation: Callable[[], Awaitable[Any]], what: str) -> Any:
        """Retry ``operation`` on retriable Kafka errors until the pass times out."""
        while True:
            try:
                return await operation()
            except KafkaError as exc:
                if not exc.retriable:
                    raise
                logger.warning(
                    "[Healthcheck] %s failed: %s; retrying in %s ms",
                    what,
                    exc,
                    self.retry_backoff_ms,
                )
                await asyncio.sleep(self.retry_backoff_ms / 1000)

    def __bool__(self):
        """Return ``is_healthy`` so ``if healthcheck:`` reads naturally."""
        return self.is_healthy
