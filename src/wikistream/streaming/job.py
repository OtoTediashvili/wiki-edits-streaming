"""One streaming query: Redpanda into the two Iceberg tables, batch by batch.

Each micro-batch of decoded changes is written in two steps:

1. `changes`: the batch's events that aren't in the table yet, by event id,
   appended. Copies within the batch and events written by earlier batches
   are dropped; late events are not, however late.
2. `edits_per_minute`: every minute the batch has events for is counted
   again from `changes` and merged in (update or insert).

Both steps can run twice for the same batch without changing the result, and
that is what makes delivery exactly-once: after a crash Spark replays the
unfinished batch from its checkpoint, step 1 finds those events already
written, and step 2 counts the same minutes to the same numbers.

The first version used Spark's stateful operators with a 2-minute watermark.
On its first real day, a few live events got mixed into a producer replay of
three days of history (fixed since, see `source.resume_id`). The watermark
moved to them, and the job dropped the whole replay as late. This design has
no watermark, so lateness can't lose data.

Each batch reads `changes` only for the minutes it touched, merged into
ranges: some events carry timestamps years old, and one [oldest, newest]
range would read every file in between.
"""

from __future__ import annotations

import operator
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from functools import reduce
from typing import Any, Protocol, override

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.listener import StreamingQueryListener
from pyspark.sql.streaming.query import StreamingQuery

from wikistream.logging_config import get_logger
from wikistream.model import schema_str
from wikistream.streaming.settings import CHANGES, EDITS_PER_MINUTE, LakeSettings
from wikistream.streaming.tables import ensure_tables
from wikistream.streaming.transform import (
    decode,
    minute_of,
    minutes_touched,
    recount,
    time_ranges,
    to_changes,
    unseen,
)

log = get_logger(__name__)

QUERY_NAME = "lake"
_TS = "yyyy-MM-dd HH:mm:ss"


def read_kafka(spark: SparkSession, settings: LakeSettings) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.bootstrap_servers)
        .option("subscribe", settings.topic)
        # Only for the very first start; afterwards the checkpoint decides.
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", str(settings.max_offsets_per_trigger))
        .option("failOnDataLoss", str(settings.fail_on_data_loss).lower())
        .option("groupIdPrefix", "wikistream-spark")
        .load()
    )


Ranges = Sequence[tuple[str, str]]


class LakeTables(Protocol):
    """What the batch writer needs from storage. Iceberg in production
    (`IcebergTables`); an in-memory stand-in in the tests. Ranges are UTC
    strings, start inclusive, end exclusive (see `transform.time_ranges`)."""

    def existing_ids(self, ranges: Ranges) -> DataFrame: ...

    def append_changes(self, rows: DataFrame) -> None: ...

    def changes_in(self, ranges: Ranges) -> DataFrame: ...

    def upsert_minutes(self, rows: DataFrame, ranges: Ranges) -> None: ...


def _within(column: str, ranges: Ranges) -> Column:
    """Literal bounds, so Iceberg skips every file and partition outside them."""
    c = F.col(column)
    terms = [
        (c >= F.lit(start).cast("timestamp")) & (c < F.lit(end).cast("timestamp"))
        for start, end in ranges
    ]
    return reduce(operator.or_, terms)


def _within_sql(column: str, ranges: Ranges) -> str:
    return " OR ".join(
        f"({column} >= TIMESTAMP '{start}' AND {column} < TIMESTAMP '{end}')"
        for start, end in ranges
    )


class IcebergTables:
    def __init__(self, spark: SparkSession, settings: LakeSettings) -> None:
        self.spark = spark
        self.changes = settings.table(CHANGES)
        self.minutes = settings.table(EDITS_PER_MINUTE)

    def changes_in(self, ranges: Ranges) -> DataFrame:
        return self.spark.table(self.changes).where(_within("event_time", ranges))

    def existing_ids(self, ranges: Ranges) -> DataFrame:
        return self.changes_in(ranges).select("event_id")

    def append_changes(self, rows: DataFrame) -> None:
        rows.writeTo(self.changes).append()

    def upsert_minutes(self, rows: DataFrame, ranges: Ranges) -> None:
        rows.createOrReplaceTempView("minute_updates")
        rows.sparkSession.sql(
            f"""
            MERGE INTO {self.minutes} t
            USING minute_updates s
            ON t.wiki = s.wiki AND t.window_start = s.window_start
               AND ({_within_sql("t.window_start", ranges)})
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )


@dataclass(frozen=True)
class BatchResult:
    received: int
    distinct: int
    written: int
    minutes: int
    ranges: int


class BatchWriter:
    """The foreachBatch function: one micro-batch of `changes` rows into the lake."""

    def __init__(self, tables: LakeTables) -> None:
        self.tables = tables

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        self.write(batch, batch_id)

    def write(self, batch: DataFrame, batch_id: int) -> BatchResult:
        batch = batch.persist()
        try:
            return self._write(batch, batch_id)
        finally:
            batch.unpersist()

    def _write(self, batch: DataFrame, batch_id: int) -> BatchResult:
        stats = batch.agg(
            F.count(F.lit(1)).alias("received"),
            F.count_distinct("event_id").alias("distinct"),
        ).first()
        if stats is None or stats.received == 0:
            return BatchResult(0, 0, 0, 0, 0)
        minute_rows = batch.select(
            F.date_format(minute_of(F.col("event_time")), _TS).alias("minute")
        ).distinct()
        ranges = time_ranges([r.minute for r in minute_rows.collect()])

        new = unseen(batch, self.tables.existing_ids(ranges)).persist()
        try:
            written = new.count()
            if written:
                self.tables.append_changes(new)
        finally:
            new.unpersist()

        # Count again every minute this batch touched, including minutes whose
        # events were all written before: a replayed batch must leave the
        # counts right even if the crash came between the two steps.
        touched = minutes_touched(batch).persist()
        try:
            counts = recount(self.tables.changes_in(ranges), touched)
            self.tables.upsert_minutes(counts, ranges)
            minutes = touched.count()
        finally:
            touched.unpersist()

        result = BatchResult(stats.received, stats.distinct, written, minutes, len(ranges))
        log.info(
            "batch.written",
            batch=batch_id,
            received=result.received,
            duplicates=result.received - result.distinct,
            already_written=result.distinct - result.written,
            written=result.written,
            minutes_recounted=result.minutes,
            time_ranges=result.ranges,
            event_time_from=ranges[0][0],
            event_time_to=ranges[-1][1],
        )
        return result


def changes_stream(spark: SparkSession, settings: LakeSettings) -> DataFrame:
    return to_changes(decode(read_kafka(spark, settings), schema_str()))


def start_query(
    stream: DataFrame, writer: BatchWriter, settings: LakeSettings, once: bool
) -> StreamingQuery:
    builder = (
        stream.writeStream.queryName(QUERY_NAME)
        .option("checkpointLocation", str(settings.checkpoint_dir / QUERY_NAME))
        .foreachBatch(writer)
    )
    if once:
        builder = builder.trigger(availableNow=True)
    else:
        builder = builder.trigger(processingTime=f"{settings.trigger_seconds} seconds")
    return builder.start()


def start_queries(spark: SparkSession, settings: LakeSettings, once: bool) -> list[StreamingQuery]:
    ensure_tables(spark, settings)
    writer = BatchWriter(IcebergTables(spark, settings))
    return [start_query(changes_stream(spark, settings), writer, settings, once)]


class ProgressLog(StreamingQueryListener):
    """One log line per micro-batch from Spark's side: rows in, speed, and
    how far behind Redpanda the job is. `batch.written` has the rest."""

    @override
    def onQueryStarted(self, event: Any) -> None:
        log.info("query.started", query=event.name)

    @override
    def onQueryProgress(self, event: Any) -> None:
        p = event.progress
        behind = [s.metrics.get("maxOffsetsBehindLatest") for s in p.sources if s.metrics]
        log.info(
            "query.progress",
            query=p.name,
            batch=p.batchId,
            rows=p.numInputRows,
            rows_per_second=round(p.processedRowsPerSecond or 0.0, 1),
            batch_ms=p.durationMs.get("triggerExecution"),
            offsets_behind=behind[0] if behind else None,
        )

    @override
    def onQueryIdle(self, event: Any) -> None:
        pass

    @override
    def onQueryTerminated(self, event: Any) -> None:
        if event.exception:
            log.error("query.failed", id=str(event.id), error=event.exception.splitlines()[0])
        else:
            log.info("query.stopped", id=str(event.id))


def await_queries(
    queries: list[StreamingQuery],
    poll_seconds: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
    on_tick: Callable[[], None] | None = None,
) -> None:
    """Block until every query has finished. If one fails, stop the rest and
    raise, so the container exits and restarts from the checkpoint.
    `on_tick` runs once per poll while the queries run (table maintenance)."""
    try:
        while any(q.isActive for q in queries):
            if on_tick is not None:
                on_tick()
            for q in queries:
                if not q.isActive and q.exception() is not None:
                    raise RuntimeError(f"query {q.name} failed: {q.exception()}")
            sleep(poll_seconds)
        for q in queries:
            if q.exception() is not None:
                raise RuntimeError(f"query {q.name} failed: {q.exception()}")
    finally:
        for q in queries:
            if q.isActive:
                q.stop()
