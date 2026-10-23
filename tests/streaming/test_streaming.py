"""The batch writer over several micro-batches: copies, replays, late events.

Most tests call the writer directly with batches, as Spark's foreachBatch
does; the last one runs it inside a real streaming query (a file source
standing in for Kafka) to check the wiring and the checkpoint.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from structlog.testing import capture_logs

from tests.streaming.frames import decoded_schema, undecodable
from tests.streaming.memory import MemoryTables, Ranges
from wikistream.streaming.job import BatchWriter, start_query
from wikistream.streaming.settings import LakeSettings


def batch(spark: SparkSession, rows: list[dict[str, Any]]) -> DataFrame:
    """A micro-batch as decode() hands it to the writer."""
    return spark.createDataFrame(rows, schema=decoded_schema())


def at(row: dict[str, Any], event_time: datetime, event_id: str) -> dict[str, Any]:
    return {**row, "event_time": event_time, "event_id": event_id}


def minute(t: datetime) -> datetime:
    return t.replace(second=0, microsecond=0)


def counts_by_hand(tables: MemoryTables) -> Counter[tuple[str, datetime]]:
    return Counter((r.wiki, minute(r.event_time)) for r in tables.changes)


def written_counts(tables: MemoryTables) -> Counter[tuple[str, datetime]]:
    return Counter({key: row.events for key, row in tables.minutes.items()})


@pytest.fixture
def tables(spark: SparkSession) -> MemoryTables:
    return MemoryTables(spark)


@pytest.fixture
def writer(tables: MemoryTables) -> BatchWriter:
    return BatchWriter(tables)


def test_copies_within_a_batch_are_written_once(
    spark: SparkSession, tables: MemoryTables, writer: BatchWriter, decoded: list[dict[str, Any]]
) -> None:
    result = writer.write(batch(spark, decoded + decoded[-10:]), 0)

    assert (result.received, result.distinct, result.written) == (105, 95, 95)
    assert sorted(tables.event_ids()) == sorted({r["event_id"] for r in decoded})
    assert written_counts(tables) == counts_by_hand(tables)


def test_events_an_earlier_batch_wrote_are_not_written_again(
    spark: SparkSession, tables: MemoryTables, writer: BatchWriter, decoded: list[dict[str, Any]]
) -> None:
    writer.write(batch(spark, decoded), 0)
    result = writer.write(batch(spark, decoded[-20:]), 1)

    assert result.written == 0
    assert len(tables.changes) == 95
    assert written_counts(tables) == counts_by_hand(tables)


def test_late_events_are_written_and_their_minutes_counted_again(
    spark: SparkSession, tables: MemoryTables, writer: BatchWriter, decoded: list[dict[str, Any]]
) -> None:
    """The incident that shaped this design: after a producer replay, events
    days older than ones already processed arrive. All of them must land."""
    writer.write(batch(spark, decoded), 0)
    first = decoded[0]
    before = tables.minute(first["wiki"], minute(first["event_time"]))["events"]

    three_days_earlier = at(first, first["event_time"] - timedelta(days=3), "late-by-days")
    same_minute = at(first, first["event_time"], "late-by-seconds")
    result = writer.write(batch(spark, [three_days_earlier, same_minute]), 1)

    assert result.written == 2
    assert {"late-by-days", "late-by-seconds"} <= set(tables.event_ids())
    assert tables.minute(first["wiki"], minute(first["event_time"]))["events"] == before + 1
    old_minute = minute(three_days_earlier["event_time"])
    assert tables.minute(first["wiki"], old_minute)["events"] == 1
    assert written_counts(tables) == counts_by_hand(tables)


def test_replaying_a_batch_changes_nothing(
    spark: SparkSession, tables: MemoryTables, writer: BatchWriter, decoded: list[dict[str, Any]]
) -> None:
    rows = batch(spark, decoded)
    writer.write(rows, 0)
    changes, minutes = list(tables.changes), dict(tables.minutes)

    replay = writer.write(rows, 0)

    assert replay.written == 0
    assert tables.changes == changes
    assert {k: v.asDict() for k, v in tables.minutes.items()} == {
        k: v.asDict() for k, v in minutes.items()
    }


class CrashBeforeCounting(MemoryTables):
    """Fails once, after the rows are written and before the minutes are."""

    def __init__(self, spark: SparkSession) -> None:
        super().__init__(spark)
        self.crash = True

    def upsert_minutes(self, rows: DataFrame, ranges: Ranges) -> None:
        if self.crash:
            self.crash = False
            raise RuntimeError("container killed")
        super().upsert_minutes(rows, ranges)


def test_a_crash_between_the_two_writes_is_repaired_by_the_replay(
    spark: SparkSession, decoded: list[dict[str, Any]]
) -> None:
    tables = CrashBeforeCounting(spark)
    writer = BatchWriter(tables)
    rows = batch(spark, decoded)

    with pytest.raises(RuntimeError, match="killed"):
        writer.write(rows, 0)
    assert len(tables.changes) == 95 and not tables.minutes

    replay = writer.write(rows, 0)  # Spark replays the unfinished batch

    assert replay.written == 0
    assert written_counts(tables) == counts_by_hand(tables)


def test_counts_match_the_rows_after_any_mix_of_batches(
    spark: SparkSession, tables: MemoryTables, writer: BatchWriter, decoded: list[dict[str, Any]]
) -> None:
    first = decoded[0]
    batches = [
        decoded[:50],
        decoded[40:] + decoded[:5],  # overlaps the first batch
        [at(first, first["event_time"] - timedelta(hours=6), "six-hours-late")],
        decoded[::7],  # nothing new
    ]
    for batch_id, rows in enumerate(batches):
        writer.write(batch(spark, rows), batch_id)

    assert len(tables.changes) == 96
    assert written_counts(tables) == counts_by_hand(tables)
    for row in tables.minutes.values():
        assert row.bot_events + row.human_events == row.events
        assert 1 <= row.editors <= row.events


def test_an_event_years_old_does_not_make_the_batch_read_the_years_between(
    spark: SparkSession, tables: MemoryTables, writer: BatchWriter, decoded: list[dict[str, Any]]
) -> None:
    """Wikipedia sends some events with timestamps years old. The batch must
    still read only the minutes it touched, not everything since 2009."""
    first = decoded[0]
    ancient = at(first, datetime(2009, 3, 9, 2, 8, 30), "from-2009")
    result = writer.write(batch(spark, [*decoded, ancient]), 0)

    assert result.written == 96 and result.ranges == 2
    assert max(hi - lo for lo, hi in tables.ranges_read) < timedelta(hours=1)
    assert tables.minute(first["wiki"], datetime(2009, 3, 9, 2, 8))["events"] == 1
    assert written_counts(tables) == counts_by_hand(tables)

    # Replayed, both ranges are checked for events already written.
    assert writer.write(batch(spark, [*decoded, ancient]), 0).written == 0
    assert len(tables.event_ids()) == 96


def test_a_message_that_does_not_decode_is_set_aside_and_the_batch_goes_on(
    spark: SparkSession, tables: MemoryTables, writer: BatchWriter, decoded: list[dict[str, Any]]
) -> None:
    """The first version stopped the job on such a message, for good: Spark
    retried the same batch after every restart."""
    poison = [
        undecodable(b"this is not avro", partition=1, offset=500),
        undecodable(b"\x00\x00\x00\x00\x01\xff\xff\xff", partition=2, offset=501),
        undecodable(None, partition=0, offset=502),
    ]
    with capture_logs() as logs:
        result = writer.write(batch(spark, [*decoded[:50], *poison, *decoded[50:]]), 0)

    assert (result.received, result.written, result.rejected) == (95, 95, 3)
    assert written_counts(tables) == counts_by_hand(tables)
    assert {(r.kafka_partition, r.kafka_offset): (r.reason, r.value) for r in tables.rejected} == {
        (1, 500): ("no schema registry header", b"this is not avro"),
        (2, 501): ("does not decode as a WikiChange record", poison[1]["raw_value"]),
        (0, 502): ("empty message", None),
    }
    [warning] = [e for e in logs if e["event"] == "batch.rejected"]
    assert warning["log_level"] == "warning" and warning["set_aside"] == 3

    # Replayed after a crash: nothing set aside twice, nothing written twice.
    again = writer.write(batch(spark, [*decoded, *poison]), 0)
    assert (again.written, again.rejected) == (0, 0)
    assert len(tables.rejected) == 3


def test_a_batch_of_nothing_but_rejects(
    spark: SparkSession, tables: MemoryTables, writer: BatchWriter
) -> None:
    result = writer.write(batch(spark, [undecodable(b"junk", partition=0, offset=7)]), 3)
    assert (result.received, result.written, result.rejected) == (0, 0, 1)
    assert tables.changes == [] and tables.minutes == {}


def test_the_streaming_query_writes_each_batch_and_resumes_from_its_checkpoint(
    spark: SparkSession, tmp_path: Path, decoded: list[dict[str, Any]]
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    settings = LakeSettings(_env_file=None, checkpoint_dir=tmp_path / "checkpoints")  # type: ignore[call-arg]
    tables = MemoryTables(spark)

    def deliver(name: str, rows: list[dict[str, Any]]) -> None:
        tmp = tmp_path / f"{name}.tmp"
        tmp.write_text(
            "\n".join(json.dumps(r, default=lambda v: v.isoformat() + "Z") for r in rows)
        )
        tmp.rename(source / f"{name}.json")

    def run_once() -> None:
        stream = spark.readStream.schema(decoded_schema()).json(str(source))
        start_query(stream, BatchWriter(tables), settings, once=True).awaitTermination()

    deliver("a", decoded + decoded[:10])
    run_once()
    assert len(tables.changes) == 95

    first = decoded[0]
    deliver("b", [at(first, first["event_time"] - timedelta(days=3), "late")])
    run_once()  # the checkpoint skips file a; only file b is new

    assert len(tables.changes) == 96
    assert written_counts(tables) == counts_by_hand(tables)
