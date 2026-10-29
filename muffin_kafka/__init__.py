"""Muffin plugin for Apache Kafka: producer, consumer pool and management commands."""

import logging

logger = logging.getLogger("muffin_kafka")

from .plugin import KafkaPlugin  # noqa: E402

Kafka = KafkaPlugin
Plugin = KafkaPlugin
