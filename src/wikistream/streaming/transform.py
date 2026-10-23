"""The stream job's logic, as functions from DataFrame to DataFrame.

    Kafka rows ──decode──▶ WikiChange records ──to_changes──▶ table rows
                       └──▶ records that don't decode ──rejects──▶ rejected rows
    batch of rows ──unseen──▶ rows not yet in the table (each event once)
    rows ──minutes_touched / recount──▶ per-wiki counts for those minutes

None of it depends on event-time order: an event that arrives days late is
written like any other, and the minutes it belongs to are counted again.
The tests drive these functions with small local DataFrames and no Kafka.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import NamedTuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro


class ColumnSpec(NamedTuple):
    name: str
    sql_type: str
    comment: str


# The changes table, in order. The DDL and the rows written both come from
# this list, so they cannot drift apart. `raw` (the original JSON) is left
# out: Redpanda keeps it for 48 hours, and storing it here would make this
# table five times larger on a laptop disk.
CHANGE_COLUMNS: tuple[ColumnSpec, ...] = (
    ColumnSpec("event_id", "STRING", "Wikimedia meta.id: unique per event, the deduplication key"),
    ColumnSpec(
        "event_time", "TIMESTAMP", "When the change happened on the wiki (second precision)"
    ),
    ColumnSpec("stream_time", "TIMESTAMP", "When EventStreams published it (meta.dt)"),
    ColumnSpec("type", "STRING", "edit, new, log or categorize"),
    ColumnSpec("wiki", "STRING", "Database name, e.g. enwiki, commonswiki, wikidatawiki"),
    ColumnSpec("server_name", "STRING", "Host name, e.g. en.wikipedia.org"),
    ColumnSpec("namespace", "INT", "MediaWiki namespace: 0 articles, 14 categories, ..."),
    ColumnSpec("title", "STRING", "Page title"),
    ColumnSpec("user", "STRING", "User name; temporary accounts start with ~"),
    ColumnSpec("bot", "BOOLEAN", "Made by an account flagged as a bot"),
    ColumnSpec("minor", "BOOLEAN", "Marked as a minor edit (edits and new pages only)"),
    ColumnSpec("patrolled", "BOOLEAN", "Reviewed by a patroller, where the wiki tracks it"),
    ColumnSpec("rc_id", "BIGINT", "Recent-changes id; null for some log types"),
    ColumnSpec("length_old", "INT", "Page size in bytes before"),
    ColumnSpec("length_new", "INT", "Page size in bytes after"),
    ColumnSpec("length_delta", "INT", "length_new - length_old; null when either is missing"),
    ColumnSpec("revision_old", "BIGINT", "Revision id before"),
    ColumnSpec("revision_new", "BIGINT", "Revision id after"),
    ColumnSpec("log_type", "STRING", "Log entries only, e.g. abusefilter, patrol"),
    ColumnSpec("log_action", "STRING", "Log entries only, e.g. hit, patrol"),
    ColumnSpec("source_topic", "STRING", "Wikimedia's Kafka topic (one per datacenter)"),
    ColumnSpec("source_partition", "INT", "Wikimedia's Kafka partition"),
    ColumnSpec("source_offset", "BIGINT", "Wikimedia's Kafka offset"),
    ColumnSpec("kafka_partition", "INT", "Partition in our Redpanda topic"),
    ColumnSpec("kafka_offset", "BIGINT", "Offset in our Redpanda topic"),
    ColumnSpec("kafka_timestamp", "TIMESTAMP", "When the producer wrote it to Redpanda"),
)

# Messages that don't decode as a WikiChange, set aside instead of stopping
# the job. `kafka_partition` first: the partition column can't be first.
REJECTED_COLUMNS: tuple[ColumnSpec, ...] = (
    ColumnSpec("kafka_partition", "INT", "Partition in our Redpanda topic"),
    ColumnSpec("kafka_offset", "BIGINT", "Offset in our Redpanda topic"),
    ColumnSpec("kafka_timestamp", "TIMESTAMP", "When it was written to Redpanda"),
    ColumnSpec("reason", "STRING", "Why it was set aside"),
    ColumnSpec("value", "BINARY", "The message exactly as received; null if it was empty"),
)

# `wiki` comes first on purpose: the partition column must not be the first
# column (see tables.create_table_sql).
MINUTE_COLUMNS: tuple[ColumnSpec, ...] = (
    ColumnSpec("wiki", "STRING", "Database name"),
    ColumnSpec("window_start", "TIMESTAMP", "Start of the one-minute event-time window"),
    ColumnSpec("window_end", "TIMESTAMP", "End of the window (exclusive)"),
    ColumnSpec("events", "BIGINT", "All changes"),
    ColumnSpec("edits", "BIGINT", "Edits to existing pages"),
    ColumnSpec("new_pages", "BIGINT", "Pages created"),
    ColumnSpec("log_events", "BIGINT", "Log entries"),
    ColumnSpec("categorize_events", "BIGINT", "Pages added to or removed from categories"),
    ColumnSpec("bot_events", "BIGINT", "Changes by bot accounts"),
    ColumnSpec("human_events", "BIGINT", "Changes by everyone else"),
    ColumnSpec("bytes_added", "BIGINT", "Sum of positive size changes"),
    ColumnSpec("bytes_removed", "BIGINT", "Sum of negative size changes, as a positive number"),
    ColumnSpec("editors", "BIGINT", "Distinct user names"),
)

# Confluent wire format: one magic byte (0) and a four-byte schema id, then
# the Avro body. Spark's from_avro wants only the body.
_CONFLUENT_HEADER_BYTES = 5


def _typed(columns: tuple[ColumnSpec, ...]) -> list[Column]:
    """Select in table order, cast to the table's types."""
    return [F.col(c.name).cast(c.sql_type).alias(c.name) for c in columns]


def decode(kafka_rows: DataFrame, avro_schema: str) -> DataFrame:
    """Kafka records (key, value, partition, offset, timestamp, ...) to one
    column per WikiChange field, plus where each record sat in Redpanda,
    `decoded`, and `raw_value`.

    A record that doesn't decode doesn't stop the job. It comes out with
    `decoded` false and its bytes in `raw_value`, and the batch writer sets it
    aside in the rejected table. The first version failed the query instead
    (from_avro's FAILFAST mode): the producer validates every record, so a bad
    one "means a bug". True, but one such message then stopped the whole lake
    until someone edited the job's checkpoint by hand.

    In PERMISSIVE mode from_avro returns a struct of nulls for bytes it can't
    read, and null for an empty message; every real record has an event_id.
    """
    body = F.expr(f"substring(value, {_CONFLUENT_HEADER_BYTES + 1})")
    decoded = F.col("change.event_id").isNotNull()
    return kafka_rows.select(
        from_avro(body, avro_schema, {"mode": "PERMISSIVE"}).alias("change"),
        F.col("value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select(
        "change.*",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        decoded.alias("decoded"),
        F.when(~decoded, F.col("value")).alias("raw_value"),
    )


def to_changes(decoded: DataFrame) -> DataFrame:
    """Decoded records to rows of the changes table. Only records that
    decoded: the others go through `rejects`."""
    good = decoded.where(F.col("decoded"))
    with_delta = good.withColumn("length_delta", F.col("length_new") - F.col("length_old"))
    return with_delta.select(*_typed(CHANGE_COLUMNS))


_REASON_SQL = f"""CASE
    WHEN raw_value IS NULL OR length(raw_value) = 0 THEN 'empty message'
    WHEN length(raw_value) < {_CONFLUENT_HEADER_BYTES} OR substring(raw_value, 1, 1) != X'00'
        THEN 'no schema registry header'
    ELSE 'does not decode as a WikiChange record'
END"""


def rejects(decoded: DataFrame) -> DataFrame:
    """The records that didn't decode, as rows of the rejected table."""
    bad = decoded.where(~F.col("decoded"))
    return bad.select(
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
        F.expr(_REASON_SQL).alias("reason"),
        F.col("raw_value").alias("value"),
    ).select(*_typed(REJECTED_COLUMNS))


def unseen(batch: DataFrame, existing_ids: DataFrame) -> DataFrame:
    """The rows of a batch that aren't in the changes table yet, each once.

    The producer delivers at least once, and after a long outage it replays
    history, so a batch can hold copies of the same event and events already
    written by an earlier batch. Both are removed here by event id, whatever
    their age. `existing_ids` holds the event ids already written, at least
    for the batch's event-time range.
    """
    return batch.dropDuplicates(["event_id"]).join(
        existing_ids.select("event_id"), on="event_id", how="left_anti"
    )


def minute_of(timestamp: Column) -> Column:
    return F.date_trunc("minute", timestamp)


MINUTE_FORMAT = "%Y-%m-%d %H:%M:%S"


def time_ranges(minutes: list[str], gap_minutes: int = 10) -> list[tuple[str, str]]:
    """Minutes a batch touched, as a few [start, end) ranges to read.

    Most events in a batch fall in one stretch of minutes, but Wikipedia
    sends some with timestamps years old. A single [oldest, newest] range
    would then cover years and make every batch read the whole table; ranges
    around the minutes actually touched read only those. Minutes closer than
    `gap_minutes` share a range, which keeps the list short. Times are UTC
    strings ("yyyy-MM-dd HH:mm:ss"), minute-aligned.
    """
    starts = sorted({datetime.strptime(m, MINUTE_FORMAT) for m in minutes})
    ranges: list[tuple[datetime, datetime]] = []
    for start in starts:
        end = start + timedelta(minutes=1)
        if ranges and start - ranges[-1][1] < timedelta(minutes=gap_minutes):
            ranges[-1] = (ranges[-1][0], end)
        else:
            ranges.append((start, end))
    return [(a.strftime(MINUTE_FORMAT), b.strftime(MINUTE_FORMAT)) for a, b in ranges]


def minutes_touched(batch: DataFrame) -> DataFrame:
    """The event-time minutes a batch has rows for: the minutes to count again."""
    return batch.select(minute_of(F.col("event_time")).alias("minute")).distinct()


def recount(changes: DataFrame, minutes: DataFrame) -> DataFrame:
    """Per-wiki counts for the given minutes, from the rows of the changes table.

    Counting again from the table, rather than adding a batch's rows to the
    old totals, makes the result right however often a batch is replayed.
    """
    scoped = changes.join(
        F.broadcast(minutes), minute_of(changes["event_time"]) == minutes["minute"], "left_semi"
    )
    return per_minute(scoped)


def _count_if(condition: Column) -> Column:
    return F.sum(F.when(condition, 1).otherwise(0))


def per_minute(changes: DataFrame) -> DataFrame:
    """Changes per wiki per event-time minute, for rows of the changes table."""
    delta = F.col("length_delta")
    counted = changes.groupBy(F.window("event_time", "1 minute"), "wiki").agg(
        F.count(F.lit(1)).alias("events"),
        _count_if(F.col("type") == "edit").alias("edits"),
        _count_if(F.col("type") == "new").alias("new_pages"),
        _count_if(F.col("type") == "log").alias("log_events"),
        _count_if(F.col("type") == "categorize").alias("categorize_events"),
        _count_if(F.col("bot")).alias("bot_events"),
        _count_if(~F.col("bot")).alias("human_events"),
        F.coalesce(F.sum(F.when(delta > 0, delta)), F.lit(0)).alias("bytes_added"),
        F.coalesce(F.sum(F.when(delta < 0, -delta)), F.lit(0)).alias("bytes_removed"),
        F.count_distinct("user").alias("editors"),
    )
    return counted.select(
        F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"), "*"
    ).select(*_typed(MINUTE_COLUMNS))
