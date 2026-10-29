"""Topic-aware dispatch of consumed messages to user handlers.

The consumer pool fetches records, the runners pass every record to
``ConsumerHandlers.__call__``, which calls the handlers registered for its topic.
The return value tells the runner whether the message may be committed.
"""

import dataclasses as dc
from collections import defaultdict
from typing import Any, Awaitable, Callable

from aiokafka.structs import ConsumerRecord

from muffin_kafka import logger

TCallable = Callable[..., Awaitable[Any]]
TErrCallable = Callable[[BaseException], Awaitable[Any]]


@dc.dataclass
class ConsumerHandlers:
    """Registry of message handlers grouped by topic and the global error hook.

    ``handlers`` maps a topic name to the list of handlers called for it, in
    registration order. A record whose topic has no handlers is considered
    handled successfully (and can therefore be committed without being seen by
    user code). ``error_handler`` is called for every handler exception.
    """

    handlers: dict[str, list[TCallable]] = dc.field(
        default_factory=lambda: defaultdict(list),
    )
    error_handler: TErrCallable | None = None

    def get_topics(self) -> set[str]:
        """Return the set of topics that have at least one handler."""
        return set(self.handlers.keys())

    def set_handler(self, handler: TCallable, *topics: str):
        """Append ``handler`` to every topic in ``topics`` (order is kept)."""
        for topic in topics:
            self.handlers[topic].append(handler)

    def set_error_handler(self, handler: TErrCallable):
        """Set the single error handler, replacing any previous one."""
        self.error_handler = handler

    async def __call__(self, msg: ConsumerRecord) -> bool:
        """Call every handler registered for ``msg.topic`` and report success.

        Returns ``False`` when at least one handler raised. Exceptions are logged
        and forwarded to ``error_handler``; the remaining handlers still run, and
        exceptions raised by ``error_handler`` propagate to the caller (the
        consumer loop).
        """
        logger.debug("Kafka msg: %s-%s-%s", msg.topic, msg.partition, msg.offset)
        success = True

        for fn in self.handlers.get(msg.topic, ()):
            try:
                await fn(msg)

            except Exception as exc:  # noqa: BLE001
                success = False
                logger.exception("Kafka: Error while processing message: %r", msg)
                if self.error_handler:
                    await self.error_handler(exc)

        return success
