"""What the dashboard reads from the lake, with PyIceberg.

Each refresh makes three small reads, all bounded:

- `state`: the newest minute counted and when the job last wrote. Taken from
  Iceberg's metadata (per-file column statistics, the snapshot log), so it
  opens no data file.
- `minutes`: the per-minute counts in the chosen window.
- `changes`: single changes, for at most the last hour.

Iceberg keeps min/max statistics for every data file, and a scan with a time
filter opens only the files whose range overlaps it. So a read costs about
the same on day one as on day thirty.

Times are naive UTC datetimes throughout, like the tables themselves (the
Spark job runs in UTC).
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

from wikistream.streaming.settings import CHANGES, EDITS_PER_MINUTE

# The columns of `changes` the dashboard uses; the other 17 stay unread.
CHANGE_FIELDS = (
    "event_time",
    "type",
    "wiki",
    "server_name",
    "namespace",
    "title",
    "user",
    "bot",
    "length_new",
)

# Iceberg's `content` code for a data file (1 and 2 are delete files).
_DATA_FILE = 0


@dataclass(frozen=True)
class LakeState:
    newest_minute: datetime | None  # start of the newest minute with a count
    last_write: datetime | None  # when the job last committed to the counts


class Lake:
    def __init__(self, catalog: Catalog, namespace: str) -> None:
        self.catalog = catalog
        self.namespace = namespace
        # Streamlit caches reads by their arguments; this keeps two lakes in
        # one process (the tests run several) from sharing cache entries.
        self.key = f"{namespace}/{uuid.uuid4().hex}"

    def _table(self, name: str) -> Table:
        return self.catalog.load_table(f"{self.namespace}.{name}")

    def state(self) -> LakeState:
        table = self._table(EDITS_PER_MINUTE)
        snapshot = table.current_snapshot()
        if snapshot is None:
            return LakeState(None, None)
        last_write = _naive(datetime.fromtimestamp(snapshot.timestamp_ms / 1000, UTC))
        files = table.inspect.files()
        data = files.filter(pc.equal(files["content"], _DATA_FILE))
        bounds = pc.struct_field(data["readable_metrics"], ["window_start", "upper_bound"])
        newest = pc.max(bounds).as_py()
        return LakeState(_naive(newest) if newest is not None else None, last_write)

    def minutes(self, start: datetime, end: datetime) -> pa.Table:
        """Per-wiki counts for the minutes starting in [start, end)."""
        return _scan(self._table(EDITS_PER_MINUTE), "window_start", start, end)

    def changes(self, start: datetime, end: datetime) -> pa.Table:
        """Changes with event_time in [start, end), `CHANGE_FIELDS` only."""
        return _scan(self._table(CHANGES), "event_time", start, end, CHANGE_FIELDS)


def _scan(
    table: Table, column: str, start: datetime, end: datetime, fields: Sequence[str] = ("*",)
) -> pa.Table:
    row_filter = f"{column} >= '{_iso_utc(start)}' AND {column} < '{_iso_utc(end)}'"
    rows = table.scan(row_filter=row_filter, selected_fields=tuple(fields)).to_arrow()
    return naive_utc(rows)


def _iso_utc(when: datetime) -> str:
    aware = when.replace(tzinfo=UTC) if when.tzinfo is None else when.astimezone(UTC)
    return aware.isoformat()


def _naive(when: datetime) -> datetime:
    return when.astimezone(UTC).replace(tzinfo=None) if when.tzinfo else when


def naive_utc(table: pa.Table) -> pa.Table:
    """Timestamps with a time zone become naive UTC (same instant, no zone).

    Iceberg's `timestamptz` arrives as timestamp[us, tz=UTC]. Dropping the
    zone keeps every query and chart in UTC without depending on the
    machine's or the browser's time zone.
    """
    for i, field in enumerate(table.schema):
        if pa.types.is_timestamp(field.type) and field.type.tz is not None:
            naive = pa.timestamp(field.type.unit)
            table = table.set_column(i, field.with_type(naive), table.column(i).cast(naive))
    return table
