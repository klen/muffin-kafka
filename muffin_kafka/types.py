from typing import TypedDict


class TBaseParams(TypedDict, total=False):
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
    group_id: str
    max_poll_records: int
    auto_offset_reset: str
    enable_auto_commit: bool
