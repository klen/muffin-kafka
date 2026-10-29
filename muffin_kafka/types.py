"""TypedDicts describing the Kafka options accepted by ``KafkaPlugin``.

Documentation-only: the plugin reads the options listed in
``KafkaPlugin.defaults`` at runtime, these types exist for annotations.
"""

from typing import TypedDict


class TBaseParams(TypedDict, total=False):
    """Connection and authentication options shared by producer and consumers."""

    bootstrap_servers: str
    client_id: str
    request_timeout_ms: int
    retry_backoff_ms: int
    sasl_mechanism: str
    sasl_plain_password: str
    sasl_plain_username: str
    security_protocol: str
    ssl_cafile: str


BASE_PARAMS = TBaseParams.__annotations__.keys()


class TParams(TBaseParams, total=False):
    """Base options plus the consumer-only options (group, offsets, commit)."""

    group_id: str
    max_poll_records: int
    auto_offset_reset: str
    enable_auto_commit: bool
