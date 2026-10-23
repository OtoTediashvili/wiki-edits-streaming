"""The topic side of a drill: where each partition ends, and every message
between two such points, decoded the way the producer encoded it."""

from __future__ import annotations

import io
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

import fastavro
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

from wikistream.model import schema_str

# Partition -> the offset the next message will get (the high watermark).
Offsets = dict[int, int]

_HEADER = 5  # Confluent wire format: magic byte 0, then a 4-byte schema id
_SCHEMA = fastavro.parse_schema(json.loads(schema_str()))


@dataclass(frozen=True)
class Sent:
    """One message in the topic. `event_id` is None when it isn't a
    WikiChange record at all."""

    partition: int
    offset: int
    event_id: str | None
    event_time: datetime | None


def read_message(partition: int, offset: int, value: bytes | None) -> Sent:
    if not value or len(value) < _HEADER or value[0] != 0:
        return Sent(partition, offset, None, None)
    try:
        record: Any = fastavro.schemaless_reader(io.BytesIO(value[_HEADER:]), _SCHEMA, None)
    except Exception:
        return Sent(partition, offset, None, None)
    if not isinstance(record, dict) or not record.get("event_id"):
        return Sent(partition, offset, None, None)
    when = record["event_time"]
    return Sent(partition, offset, record["event_id"], when.astimezone(UTC).replace(tzinfo=None))


class ConsumerLike(Protocol):
    """The slice of confluent_kafka.Consumer used here, so tests can fake it."""

    def list_topics(self, topic: str, timeout: float) -> Any: ...

    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float
    ) -> tuple[int, int]: ...

    def assign(self, partitions: list[TopicPartition]) -> None: ...

    def poll(self, timeout: float) -> Any: ...

    def position(self, partitions: list[TopicPartition]) -> list[TopicPartition]: ...

    def unassign(self) -> None: ...


def consumer(bootstrap_servers: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "wikistream-drills",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )


def end_offsets(client: ConsumerLike, topic: str) -> Offsets:
    partitions = client.list_topics(topic, timeout=10).topics[topic].partitions
    return {
        p: client.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
        for p in sorted(partitions)
    }


def read_window(
    client: ConsumerLike,
    topic: str,
    start: Offsets,
    end: Offsets,
    timeout_seconds: float = 120,
    clock: Callable[[], float] = time.monotonic,
) -> list[Sent]:
    """Every message from `start` (inclusive) to `end` (exclusive), per partition."""
    todo = {p: (start.get(p, 0), hi) for p, hi in end.items() if hi > start.get(p, 0)}
    if not todo:
        return []
    client.assign([TopicPartition(topic, p, lo) for p, (lo, _) in todo.items()])
    remaining = {p: hi for p, (_, hi) in todo.items()}
    sent: list[Sent] = []
    deadline = clock() + timeout_seconds
    try:
        while remaining:
            if clock() > deadline:
                raise TimeoutError(f"{topic}: still waiting for partitions {sorted(remaining)}")
            message = client.poll(1.0)
            if message is None:
                # Idle: a partition may already be past its end (a gap at
                # the end of the range, e.g. a control record).
                for tp in client.position([TopicPartition(topic, p) for p in remaining]):
                    if tp.offset >= remaining[tp.partition]:
                        del remaining[tp.partition]
                continue
            if message.error():
                raise KafkaException(message.error())
            partition, offset = message.partition(), message.offset()
            if partition not in remaining or offset >= remaining[partition]:
                continue
            sent.append(read_message(partition, offset, message.value()))
            if offset >= remaining[partition] - 1:
                del remaining[partition]
    finally:
        client.unassign()
    return sent


# Two kinds of message no WikiChange producer would send: plain bytes, and
# a valid-looking schema registry header in front of bytes that aren't Avro.
POISON = (b"this is not avro", b"\x00\x00\x00\x00\x01" + b"\xff" * 32)


def send_poison(bootstrap_servers: str, topic: str) -> list[tuple[int, int]]:
    """Write the POISON messages straight to the topic; their (partition, offset)."""
    producer = Producer({"bootstrap.servers": bootstrap_servers, "acks": "all"})
    delivered: list[tuple[int, int]] = []
    errors: list[str] = []

    def done(err: Any, msg: Any) -> None:
        if err is not None:
            errors.append(str(err))
        else:
            delivered.append((msg.partition(), msg.offset()))

    for value in POISON:
        producer.produce(topic, value=value, key=b"drill:poison", on_delivery=done)
    if producer.flush(30) or errors:
        raise RuntimeError(f"poison messages not delivered: {errors or 'timed out'}")
    return sorted(delivered)
