"""The lake side of a drill: the rows the audit compares with the topic."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

import pyarrow as pa
from pyiceberg.catalog import Catalog

from wikistream.dashboard.lake import naive_utc
from wikistream.streaming.settings import CHANGES, EDITS_PER_MINUTE, REJECTED

Ranges = Sequence[tuple[str, str]]  # "yyyy-MM-dd HH:mm:ss" UTC, start inclusive


def _iso(bound: str) -> str:
    return datetime.strptime(bound, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC).isoformat()


def _within(column: str, ranges: Ranges) -> str:
    return " OR ".join(f"({column} >= '{_iso(a)}' AND {column} < '{_iso(b)}')" for a, b in ranges)


class LakeView:
    def __init__(self, catalog: Catalog, namespace: str) -> None:
        self.catalog = catalog
        self.namespace = namespace

    def _scan(self, name: str, column: str, ranges: Ranges, fields: tuple[str, ...]) -> pa.Table:
        table = self.catalog.load_table(f"{self.namespace}.{name}")
        if not ranges:
            return naive_utc(table.scan(selected_fields=fields, limit=0).to_arrow())
        scan = table.scan(row_filter=_within(column, ranges), selected_fields=fields)
        return naive_utc(scan.to_arrow())

    def changes(self, ranges: Ranges) -> pa.Table:
        """event_id, wiki and event_time (in table order) of every change in the ranges."""
        return self._scan(CHANGES, "event_time", ranges, ("event_id", "wiki", "event_time"))

    def minutes(self, ranges: Ranges) -> pa.Table:
        """wiki, window_start, events of every counted minute in the ranges."""
        return self._scan(
            EDITS_PER_MINUTE, "window_start", ranges, ("wiki", "window_start", "events")
        )

    def rejected(self) -> set[tuple[int, int]]:
        """(partition, offset) of every message set aside. Empty until the job
        has created the table."""
        identifier = f"{self.namespace}.{REJECTED}"
        if not self.catalog.table_exists(identifier):
            return set()
        rows = (
            self.catalog.load_table(identifier)
            .scan(selected_fields=("kafka_partition", "kafka_offset"))
            .to_arrow()
        )
        return set(
            zip(rows["kafka_partition"].to_pylist(), rows["kafka_offset"].to_pylist(), strict=True)
        )
