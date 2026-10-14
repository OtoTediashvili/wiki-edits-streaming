"""End to end against a real Redpanda: `make up`, then `make test-integration`.

The recorded capture goes through the real producer, broker and schema
registry, and comes back out through a consumer. CI runs this against the
same docker-compose.yml.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import pytest
from confluent_kafka import DeserializingConsumer

# ConfigResource is defined in a private module and re-exported by
# confluent_kafka.admin without being declared, which strict mypy rejects.
# The public import path is still the right one to use.
from confluent_kafka.admin import AdminClient, ConfigResource  # type: ignore[attr-defined]
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from wikistream.__main__ import build_pipeline
from wikistream.config import Settings
from wikistream.producer import topic_config
from wikistream.source import replay

pytestmark = pytest.mark.integration

FIXTURE = Path(__file__).parent / "fixtures" / "recentchange_sample.sse"


@pytest.fixture
def live_settings(tmp_path: Path) -> Iterator[Settings]:
    """Settings with topics of their own, deleted afterwards, so tests never
    touch wiki.recentchange and leave nothing behind in Console."""
    run = uuid.uuid4().hex[:8]
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        topic=f"it.{run}.recentchange",
        dlq_topic=f"it.{run}.recentchange.dlq",
        state_path=tmp_path / "last_event_id",
    )
    yield settings
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    for future in admin.delete_topics([settings.topic, settings.dlq_topic]).values():
        future.result()


def consume_all(
    settings: Settings, expected: int, timeout: float = 60.0
) -> list[tuple[bytes, dict[str, object]]]:
    consumer = DeserializingConsumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": f"it-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "value.deserializer": AvroDeserializer(
                SchemaRegistryClient({"url": settings.schema_registry_url})
            ),
        }
    )
    consumer.subscribe([settings.topic])
    records: list[tuple[bytes, dict[str, object]]] = []
    deadline = time.monotonic() + timeout
    try:
        while len(records) < expected and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            assert message.error() is None, message.error()
            key, value = message.key(), message.value()
            assert isinstance(key, bytes)
            assert isinstance(value, dict)
            records.append((key, value))
    finally:
        consumer.close()
    return records


def test_the_capture_round_trips_through_redpanda(live_settings: Settings) -> None:
    stats = build_pipeline(live_settings, None).run(replay(FIXTURE))
    assert stats.produced == 95

    records = consume_all(live_settings, expected=95)
    assert len(records) == 95
    assert len({value["event_id"] for _, value in records}) == 95
    # Keyed by page, so every change to a page lands on the same partition.
    for key, value in records:
        assert key.decode() == f"{value['wiki']}:{value['title']}"
    # The schema registry round trip keeps the typed fields typed.
    assert all(isinstance(value["event_time"], datetime) for _, value in records)


def test_topics_get_their_retention_and_creation_is_repeatable(live_settings: Settings) -> None:
    build_pipeline(live_settings, None)
    build_pipeline(live_settings, None)  # second start: topics already exist

    admin = AdminClient({"bootstrap.servers": live_settings.bootstrap_servers})
    resource = ConfigResource(ConfigResource.Type.TOPIC, live_settings.topic)
    configs = admin.describe_configs([resource])[resource].result()
    for name, value in topic_config(live_settings).items():
        assert configs[name].value == value, name
    metadata = admin.list_topics(live_settings.topic, timeout=10)
    assert len(metadata.topics[live_settings.topic].partitions) == 3
