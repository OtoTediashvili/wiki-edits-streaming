"""An in-memory stand-in for the Iceberg tables, for the batch writer's tests.

Same contract as `job.IcebergTables`: time ranges are UTC strings
("yyyy-MM-dd HH:mm:ss"), start inclusive, end exclusive. Rows live in Python
lists and come back as Spark DataFrames, so the writer runs its real Spark
logic against them. Every range asked for is recorded, so tests can check
how much a batch reads.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, Row, SparkSession

from wikistream.streaming.transform import (
    CHANGE_COLUMNS,
    MINUTE_COLUMNS,
    REJECTED_COLUMNS,
    ColumnSpec,
)


def _ddl(columns: tuple[ColumnSpec, ...]) -> str:
    return ", ".join(f"`{c.name}` {c.sql_type}" for c in columns)


def _parse(bound: str) -> datetime:
    return datetime.strptime(bound, "%Y-%m-%d %H:%M:%S")


Ranges = Sequence[tuple[str, str]]


class MemoryTables:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.changes: list[Row] = []
        self.minutes: dict[tuple[str, datetime], Row] = {}
        self.ranges_read: list[tuple[datetime, datetime]] = []
        self.rejected: list[Row] = []

    def _within(self, ranges: Ranges) -> list[Row]:
        bounds = [(_parse(a), _parse(b)) for a, b in ranges]
        self.ranges_read.extend(bounds)
        return [r for r in self.changes if any(lo <= r.event_time < hi for lo, hi in bounds)]

    def changes_in(self, ranges: Ranges) -> DataFrame:
        return self.spark.createDataFrame(self._within(ranges), _ddl(CHANGE_COLUMNS))

    def existing_ids(self, ranges: Ranges) -> DataFrame:
        return self.changes_in(ranges).select("event_id")

    def append_changes(self, rows: DataFrame) -> None:
        self.changes.extend(rows.collect())

    def upsert_minutes(self, rows: DataFrame, ranges: Ranges) -> None:
        bounds = [(_parse(a), _parse(b)) for a, b in ranges]
        for row in rows.collect():
            assert any(lo <= row.window_start < hi for lo, hi in bounds), "recount outside ranges"
            self.minutes[(row.wiki, row.window_start)] = row

    def rejected_offsets(self) -> DataFrame:
        return self.spark.createDataFrame(self.rejected, _ddl(REJECTED_COLUMNS)).select(
            "kafka_partition", "kafka_offset"
        )

    def append_rejected(self, rows: DataFrame) -> None:
        self.rejected.extend(rows.collect())

    # --- for assertions ---------------------------------------------------

    def event_ids(self) -> list[str]:
        return [r.event_id for r in self.changes]

    def minute(self, wiki: str, start: datetime) -> dict[str, Any]:
        return self.minutes[(wiki, start)].asDict()

    def minutes_frame(self) -> DataFrame:
        return self.spark.createDataFrame(list(self.minutes.values()), _ddl(MINUTE_COLUMNS))
