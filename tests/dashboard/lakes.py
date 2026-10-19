"""Real Iceberg tables in a temporary folder, for the dashboard's tests.

A SQL catalog in a SQLite file stands in for SeaweedFS's REST catalog. The
tables get the stream job's columns (from its column specs) and its day
partitioning, PyIceberg writes the rows, and the dashboard reads them the
way it reads the real lake.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    BooleanType,
    IcebergType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from wikistream.dashboard.lake import Lake, naive_utc
from wikistream.streaming.settings import CHANGES, EDITS_PER_MINUTE
from wikistream.streaming.transform import CHANGE_COLUMNS, MINUTE_COLUMNS, ColumnSpec

NAMESPACE = "wiki"

# Spark's TIMESTAMP is an instant: Iceberg stores it as timestamptz.
_TYPES: dict[str, IcebergType] = {
    "STRING": StringType(),
    "TIMESTAMP": TimestamptzType(),
    "INT": IntegerType(),
    "BIGINT": LongType(),
    "BOOLEAN": BooleanType(),
}

# Calls to the page's `reconnect` hook, for the test that checks it.
RECONNECTS: list[str] = []


def schema(columns: tuple[ColumnSpec, ...]) -> Schema:
    fields = (
        NestedField(i + 1, c.name, _TYPES[c.sql_type], required=False)
        for i, c in enumerate(columns)
    )
    return Schema(*fields)


def _by_day(table_schema: Schema, column: str) -> PartitionSpec:
    source = table_schema.find_field(column).field_id
    return PartitionSpec(
        PartitionField(source_id=source, field_id=1000, transform=DayTransform(), name="day")
    )


def catalog(folder: Path) -> Catalog:
    return SqlCatalog("test", uri=f"sqlite:///{folder}/catalog.db", warehouse=f"file://{folder}")


def create_tables(folder: Path) -> Catalog:
    cat = catalog(folder)
    cat.create_namespace_if_not_exists(NAMESPACE)
    for name, columns, column in (
        (CHANGES, CHANGE_COLUMNS, "event_time"),
        (EDITS_PER_MINUTE, MINUTE_COLUMNS, "window_start"),
    ):
        table_schema = schema(columns)
        cat.create_table_if_not_exists(
            f"{NAMESPACE}.{name}", schema=table_schema, partition_spec=_by_day(table_schema, column)
        )
    return cat


def open_lake(folder: Path) -> Lake:
    return Lake(catalog(folder), NAMESPACE)


def _utc(value: Any) -> Any:
    return value.replace(tzinfo=UTC) if isinstance(value, datetime) else value


def arrow(columns: tuple[ColumnSpec, ...], rows: list[dict[str, Any]]) -> pa.Table:
    """Rows as the lake holds them: every column, timestamps with a zone."""
    full = [{c.name: _utc(row.get(c.name)) for c in columns} for row in rows]
    return pa.Table.from_pylist(full, schema=schema(columns).as_arrow())


def read_back(columns: tuple[ColumnSpec, ...], rows: list[dict[str, Any]]) -> pa.Table:
    """Rows as `Lake` returns them: naive UTC timestamps."""
    return naive_utc(arrow(columns, rows))


def append_minutes(cat: Catalog, rows: list[dict[str, Any]]) -> None:
    cat.load_table(f"{NAMESPACE}.{EDITS_PER_MINUTE}").append(arrow(MINUTE_COLUMNS, rows))


def append_changes(cat: Catalog, rows: list[dict[str, Any]]) -> None:
    cat.load_table(f"{NAMESPACE}.{CHANGES}").append(arrow(CHANGE_COLUMNS, rows))


def minute(
    wiki: str,
    start: datetime,
    *,
    humans: int = 0,
    bots: int = 0,
    new_pages: int = 0,
    bytes_added: int = 0,
    bytes_removed: int = 0,
    editors: int = 1,
) -> dict[str, Any]:
    """One row of edits_per_minute; every change an edit unless it's a new page."""
    events = humans + bots
    return {
        "wiki": wiki,
        "window_start": start,
        "window_end": start + timedelta(minutes=1),
        "events": events,
        "edits": events - new_pages,
        "new_pages": new_pages,
        "log_events": 0,
        "categorize_events": 0,
        "bot_events": bots,
        "human_events": humans,
        "bytes_added": bytes_added,
        "bytes_removed": bytes_removed,
        "editors": editors,
    }


def change(
    when: datetime,
    *,
    wiki: str = "enwiki",
    title: str = "Main Page",
    user: str | None = "Alice",
    bot: bool = False,
    type: str = "edit",
    namespace: int = 0,
    length_new: int = 1000,
    server_name: str | None = "en.wikipedia.org",
) -> dict[str, Any]:
    return {
        "event_id": f"{wiki}-{title}-{user}-{when.isoformat()}-{type}",
        "event_time": when,
        "type": type,
        "wiki": wiki,
        "server_name": server_name,
        "namespace": namespace,
        "title": title,
        "user": user,
        "bot": bot,
        "length_new": length_new,
    }
