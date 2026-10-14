"""The producing pipeline: serialization, dead letters, delivery and checkpoints.

A fake producer stands in for the broker, so delivery confirmations and
failures can be controlled exactly. Serialization is real: Avro against an
in-memory schema registry.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from wikistream.config import Settings
from wikistream.model import WikiChange, schema_str
from wikistream.producer import (
    DeliveryError,
    Pipeline,
    ensure_topics,
    producer_config,
    topic_config,
)
from wikistream.sse import SseEvent
from wikistream.state import ResumePosition


@dataclass
class Sent:
    topic: str
    key: bytes | None
    value: bytes
    headers: Any


@dataclass
class FakeProducer:
    """Records what is produced. flush() confirms everything in flight,
    unless told to reject a message or to time out."""

    sent: list[Sent] = field(default_factory=list)
    inflight: list[Callable[[Any, Any], None]] = field(default_factory=list)
    reject_on_flush: bool = False
    stuck: int = 0
    flushes: int = 0

    def produce(
        self,
        topic: str,
        value: str | bytes | None = None,
        key: str | bytes | None = None,
        partition: int = -1,
        callback: Callable[[Any, Any], None] | None = None,
        on_delivery: Callable[[Any, Any], None] | None = None,
        timestamp: int = 0,
        headers: Any = None,
    ) -> None:
        assert isinstance(value, bytes)
        assert key is None or isinstance(key, bytes)
        self.sent.append(Sent(topic, key, value, headers))
        if on_delivery is not None:
            self.inflight.append(on_delivery)

    def poll(self, timeout: float = -1) -> int:
        return 0

    def flush(self, timeout: float = -1) -> int:
        self.flushes += 1
        for callback in self.inflight:
            callback("BROKER_REJECTED" if self.reject_on_flush else None, None)
        self.inflight.clear()
        return self.stuck


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def registry() -> SchemaRegistryClient:
    return SchemaRegistryClient.new_client({"url": "mock://wikistream-tests"})


def make_serializer(registry: SchemaRegistryClient, topic: str) -> Callable[[WikiChange], bytes]:
    serializer = AvroSerializer(registry, schema_str())
    context = SerializationContext(topic, MessageField.VALUE)

    def serialize(record: WikiChange) -> bytes:
        value: bytes | None = serializer(record, context)
        assert value is not None
        return value

    return serialize


def pipeline(
    settings: Settings,
    registry: SchemaRegistryClient,
    producer: FakeProducer,
    clock: Clock | None = None,
) -> Pipeline:
    return Pipeline(
        settings,
        producer,
        make_serializer(registry, settings.topic),
        ResumePosition(settings.state_path),
        clock=clock or Clock(),
    )


def message(data: str, event_id: str | None = None, event: str = "message") -> SseEvent:
    return SseEvent(event, data, event_id)


# ------------------------------------------------------------------ produce


def test_every_recorded_event_is_produced_as_avro(
    settings: Settings, registry: SchemaRegistryClient, recorded: list[SseEvent]
) -> None:
    producer = FakeProducer()
    stats = pipeline(settings, registry, producer).run(recorded)
    assert stats.produced == 95
    assert stats.dead_lettered == 0
    assert {s.topic for s in producer.sent} == {"wiki.recentchange"}

    deserialize = AvroDeserializer(registry)
    context = SerializationContext(settings.topic, MessageField.VALUE)
    first = producer.sent[0]
    assert first.value[0] == 0, "Confluent wire format: magic byte, then the schema id"
    record = deserialize(first.value, context)
    assert record is not None
    assert record["raw"] == recorded[0].data
    assert first.key == f"{record['wiki']}:{record['title']}".encode()


def test_the_resume_position_is_the_last_delivered_event(
    settings: Settings, registry: SchemaRegistryClient, recorded: list[SseEvent]
) -> None:
    pipeline(settings, registry, FakeProducer()).run(recorded)
    assert ResumePosition(settings.state_path).load() == recorded[-1].id


def test_checkpoints_follow_the_clock(
    settings: Settings, registry: SchemaRegistryClient, recorded: list[SseEvent]
) -> None:
    """Every checkpoint_seconds: flush, then save. Between checkpoints the
    position on disk stays at the last confirmed event."""
    producer, clock = FakeProducer(), Clock()
    p = pipeline(settings, registry, producer, clock)
    position = ResumePosition(settings.state_path)

    def ticking() -> Iterator[SseEvent]:
        for i, event in enumerate(recorded[:20]):
            clock.now = i * 1.0  # one event per second
            yield event
            if i == 4:
                assert position.load() is None, "nothing confirmed before the first checkpoint"
            if i == 6:
                # The checkpoint at t=5 s saved the event that arrived at t=5 s.
                assert position.load() == recorded[5].id

    p.run(ticking())
    assert producer.flushes == 3 + 1  # at t = 5, 10 and 15 s, plus the final one
    assert position.load() == recorded[19].id


# ------------------------------------------------------------- dead letters


def test_a_broken_event_goes_to_the_dead_letter_topic_and_the_stream_continues(
    settings: Settings, registry: SchemaRegistryClient, recorded: list[SseEvent]
) -> None:
    producer = FakeProducer()
    events = [recorded[0], message("{not json", "broken"), recorded[1]]
    stats = pipeline(settings, registry, producer).run(events)
    assert stats.produced == 2
    assert stats.dead_lettered == 1
    dead = [s for s in producer.sent if s.topic == settings.dlq_topic]
    assert len(dead) == 1
    assert dead[0].value == b"{not json"
    ((name, reason),) = dead[0].headers
    assert name == "error"
    assert reason.startswith(b"not JSON")
    assert ResumePosition(settings.state_path).load() == recorded[1].id


def test_an_event_that_does_not_fit_the_schema_is_dead_lettered(
    settings: Settings, registry: SchemaRegistryClient, recorded: list[SseEvent]
) -> None:
    event = json.loads(recorded[0].data)
    event["namespace"] = 2**40  # not an Avro int
    stats = pipeline(settings, registry, FakeProducer()).run([message(json.dumps(event), "x")])
    assert (stats.produced, stats.dead_lettered) == (0, 1)


def test_canary_events_are_dropped(
    settings: Settings, registry: SchemaRegistryClient, recorded: list[SseEvent]
) -> None:
    event = json.loads(recorded[0].data)
    event["meta"]["domain"] = "canary"
    producer = FakeProducer()
    stats = pipeline(settings, registry, producer).run([message(json.dumps(event), "c")])
    assert (stats.produced, stats.canaries, producer.sent) == (0, 1, [])


def test_other_event_types_are_ignored(settings: Settings, registry: SchemaRegistryClient) -> None:
    producer = FakeProducer()
    stats = pipeline(settings, registry, producer).run([message("{}", "1", event="error")])
    assert (stats.ignored, producer.sent) == (1, [])


# ------------------------------------------------------------ failures


def test_a_rejected_delivery_stops_without_moving_the_position(
    settings: Settings, registry: SchemaRegistryClient, recorded: list[SseEvent]
) -> None:
    """At-least-once: if the broker refuses a message, the position must not
    pass it, so a restart sends it again."""
    position = ResumePosition(settings.state_path)
    position.save("before")
    producer = FakeProducer(reject_on_flush=True)
    with pytest.raises(DeliveryError, match="BROKER_REJECTED"):
        pipeline(settings, registry, producer).run(recorded[:3])
    assert position.load() == "before"


def test_unconfirmed_messages_after_a_flush_stop_the_producer(
    settings: Settings, registry: SchemaRegistryClient, recorded: list[SseEvent]
) -> None:
    position = ResumePosition(settings.state_path)
    with pytest.raises(DeliveryError, match="unconfirmed"):
        pipeline(settings, registry, FakeProducer(stuck=2)).run(recorded[:3])
    assert position.load() is None


def test_stopping_still_confirms_and_saves_what_was_sent(
    settings: Settings, registry: SchemaRegistryClient, recorded: list[SseEvent]
) -> None:
    """Ctrl-C or docker stop (SIGTERM, turned into KeyboardInterrupt) mid-stream."""

    def interrupted() -> Iterable[SseEvent]:
        yield from recorded[:10]
        raise KeyboardInterrupt

    producer = FakeProducer()
    with pytest.raises(KeyboardInterrupt):
        pipeline(settings, registry, producer).run(interrupted())
    assert producer.inflight == []
    assert ResumePosition(settings.state_path).load() == recorded[9].id


# ------------------------------------------------------------ configuration


def test_the_producer_cannot_duplicate_or_reorder_on_retry(settings: Settings) -> None:
    config = producer_config(settings)
    assert config["enable.idempotence"] is True
    assert config["acks"] == "all"


def test_topics_keep_two_days_and_cap_their_size(settings: Settings) -> None:
    assert topic_config(settings) == {
        "cleanup.policy": "delete",
        "retention.ms": str(48 * 3600 * 1000),
        "retention.bytes": str(1024**3),
        "compression.type": "zstd",
    }


class FakeAdmin:
    def __init__(self, errors: dict[str, KafkaError] | None = None) -> None:
        self.errors = errors or {}
        self.created: list[Any] = []

    def create_topics(self, new_topics: list[Any], **kwargs: Any) -> dict[str, Any]:
        self.created = new_topics
        futures: dict[str, Future[None]] = {}
        for topic in new_topics:
            future: Future[None] = Future()
            if topic.topic in self.errors:
                future.set_exception(KafkaException(self.errors[topic.topic]))
            else:
                future.set_result(None)
            futures[topic.topic] = future
        return futures


def test_topics_are_created_with_their_settings(settings: Settings) -> None:
    admin = FakeAdmin()
    ensure_topics(admin, settings)
    main, dlq = admin.created
    assert (main.topic, main.num_partitions, main.replication_factor) == ("wiki.recentchange", 3, 1)
    assert main.config == topic_config(settings)
    assert (dlq.topic, dlq.num_partitions) == ("wiki.recentchange.dlq", 1)


def test_existing_topics_are_fine(settings: Settings) -> None:
    exists = KafkaError(KafkaError.TOPIC_ALREADY_EXISTS)
    ensure_topics(
        FakeAdmin({"wiki.recentchange": exists, "wiki.recentchange.dlq": exists}), settings
    )


def test_other_topic_errors_are_raised(settings: Settings) -> None:
    denied = KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)
    with pytest.raises(KafkaException):
        ensure_topics(FakeAdmin({"wiki.recentchange": denied}), settings)
