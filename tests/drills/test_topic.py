"""Reading the topic: the producer's encoding, and windows between offsets."""

from __future__ import annotations

import io
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import fastavro
import pytest
from confluent_kafka import TopicPartition

from wikistream.drills.topic import POISON, Sent, end_offsets, read_message, read_window
from wikistream.model import schema_str

SCHEMA = fastavro.parse_schema(json.loads(schema_str()))
WHEN = datetime(2026, 10, 22, 18, 0, 5)


def record(event_id: str) -> dict[str, Any]:
    """A valid WikiChange record: required fields filled, the rest null."""
    fields = json.loads(schema_str())["fields"]
    defaults: dict[str, Any] = {
        "string": "x",
        "long": 0,
        "int": 0,
        "boolean": False,
    }
    out: dict[str, Any] = {}
    for f in fields:
        kind = f["type"]
        if isinstance(kind, list):
            out[f["name"]] = None
        elif isinstance(kind, dict):
            out[f["name"]] = WHEN.replace(tzinfo=UTC)
        else:
            out[f["name"]] = defaults[kind]
    out["event_id"] = event_id
    return out


def encoded(event_id: str) -> bytes:
    """As the producer's AvroSerializer frames it: magic byte, schema id, Avro."""
    body = io.BytesIO()
    fastavro.schemaless_writer(body, SCHEMA, record(event_id))
    return b"\x00\x00\x00\x00\x07" + body.getvalue()


def test_a_message_decodes_to_its_event_id_and_naive_utc_time() -> None:
    assert read_message(2, 41, encoded("abc")) == Sent(2, 41, "abc", WHEN)


@pytest.mark.parametrize("value", [*POISON, b"", None, b"\x00\x00"])
def test_anything_else_is_not_a_wikichange(value: bytes | None) -> None:
    assert read_message(0, 1, value) == Sent(0, 1, None, None)


@dataclass
class Message:
    p: int
    o: int
    v: bytes | None
    err: Any = None

    def partition(self) -> int:
        return self.p

    def offset(self) -> int:
        return self.o

    def value(self) -> bytes | None:
        return self.v

    def error(self) -> Any:
        return self.err


CONTROL = b"<control record>"


class FakeConsumer:
    """Partitions as lists of values; assign() seeks, poll() reads in turn."""

    def __init__(self, partitions: dict[int, list[bytes | None]]) -> None:
        self.partitions = partitions
        self.positions: dict[int, int] = {}
        self.unassigned = False

    def list_topics(self, topic: str, timeout: float) -> Any:
        partitions = SimpleNamespace(partitions=dict.fromkeys(self.partitions))
        return SimpleNamespace(topics={topic: partitions})

    def get_watermark_offsets(self, tp: TopicPartition, timeout: float) -> tuple[int, int]:
        return 0, len(self.partitions[tp.partition])

    def assign(self, tps: list[TopicPartition]) -> None:
        self.positions = {tp.partition: tp.offset for tp in tps}

    def poll(self, timeout: float) -> Message | None:
        for p in sorted(self.positions):
            while self.positions[p] < len(self.partitions[p]):
                at = self.positions[p]
                self.positions[p] = at + 1
                value = self.partitions[p][at]
                if value is CONTROL:  # Kafka moves past it without delivering it
                    continue
                return Message(p, at, value)
        return None

    def position(self, tps: list[TopicPartition]) -> list[TopicPartition]:
        return [TopicPartition(tp.topic, tp.partition, self.positions[tp.partition]) for tp in tps]

    def unassign(self) -> None:
        self.unassigned = True


def test_the_end_of_each_partition() -> None:
    client = FakeConsumer({0: [b"a", b"b"], 1: [], 2: [b"c"]})
    assert end_offsets(client, "t") == {0: 2, 1: 0, 2: 1}


def test_a_window_holds_exactly_the_messages_between_the_offsets() -> None:
    client = FakeConsumer(
        {
            0: [encoded("before"), encoded("a"), encoded("b"), encoded("after")],
            1: [encoded("c"), POISON[0]],
            2: [encoded("untouched")],
        }
    )
    sent = read_window(client, "t", start={0: 1, 1: 0, 2: 1}, end={0: 3, 1: 2, 2: 1})
    assert sorted((s.partition, s.offset, s.event_id) for s in sent) == [
        (0, 1, "a"),
        (0, 2, "b"),
        (1, 0, "c"),
        (1, 1, None),
    ]
    assert client.unassigned


def test_a_partition_whose_last_offset_never_comes_still_ends() -> None:
    """The end can be a control record, never delivered: the position says
    the partition is done."""
    client = FakeConsumer({0: [encoded("a"), CONTROL]})
    sent = read_window(client, "t", start={0: 0}, end={0: 2}, timeout_seconds=5)
    assert [s.event_id for s in sent] == ["a"]


def test_a_message_past_the_end_is_left_out_even_after_a_gap() -> None:
    """The end's last offset is a control record; the next message the
    consumer hands over is already past the window."""
    client = FakeConsumer({0: [encoded("a"), CONTROL, encoded("after")]})
    sent = read_window(client, "t", start={0: 0}, end={0: 2}, timeout_seconds=5)
    assert [s.event_id for s in sent] == ["a"]


def test_an_empty_window_reads_nothing() -> None:
    assert read_window(FakeConsumer({0: [b"x"]}), "t", start={0: 1}, end={0: 1}) == []


def test_a_window_that_never_fills_times_out() -> None:
    class Stuck(FakeConsumer):
        def poll(self, timeout: float) -> Message | None:
            return None

        def position(self, tps: list[TopicPartition]) -> list[TopicPartition]:
            return [TopicPartition(tp.topic, tp.partition, 0) for tp in tps]

    ticks = iter(range(1000))
    with pytest.raises(TimeoutError, match="partitions \\[0\\]"):
        read_window(
            Stuck({0: []}), "t", start={0: 0}, end={0: 5}, timeout_seconds=3,
            clock=lambda: float(next(ticks)),
        )  # fmt: skip
