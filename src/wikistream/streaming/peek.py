"""Look into the lake from this machine, without Spark: `make peek`.

Reads the tables through the same REST catalog with PyIceberg, like the
dashboard does.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta

import pyarrow as pa
from pyiceberg.catalog import Catalog

from wikistream.streaming.catalog import rest_catalog
from wikistream.streaming.settings import CHANGES, EDITS_PER_MINUTE, LakeSettings


def _table_summary(catalog: Catalog, identifier: str) -> str:
    table = catalog.load_table(identifier)
    snapshot = table.current_snapshot()
    if snapshot is None:
        return f"{identifier}: empty"
    summary = snapshot.summary.additional_properties if snapshot.summary else {}
    committed = datetime.fromtimestamp(snapshot.timestamp_ms / 1000, UTC)
    return (
        f"{identifier}: {int(summary.get('total-records', 0)):,} rows in "
        f"{int(summary.get('total-data-files', 0)):,} files, "
        f"{len(table.metadata.snapshots)} snapshots, last commit {committed:%H:%M:%S} UTC"
    )


def _newest_minutes(catalog: Catalog, identifier: str, minutes: int = 10) -> pa.Table:
    """Totals for the newest minutes in the table, whatever the clock says:
    while the job catches up on history, those are hours or days old."""
    table = catalog.load_table(identifier)
    starts = table.scan(selected_fields=("window_start",)).to_arrow()["window_start"]
    if len(starts) == 0:
        return pa.table({})
    newest = max(starts.to_pylist())
    since = newest - timedelta(minutes=minutes - 1)
    rows = table.scan(row_filter=f"window_start >= '{since.isoformat()}'").to_arrow()
    per_minute = rows.group_by("window_start").aggregate(
        [("events", "sum"), ("bot_events", "sum"), ("wiki", "count_distinct")]
    )
    return per_minute.sort_by([("window_start", "descending")])


def _behind(newest: datetime) -> str:
    seconds = (datetime.now(UTC) - newest).total_seconds()
    if seconds < 600:
        return "live"
    if seconds < 86_400:
        return f"{seconds / 3600:.1f} hours behind the clock: catching up"
    return f"{seconds / 86_400:.1f} days behind the clock: catching up"


def main() -> int:
    settings = LakeSettings()
    catalog = rest_catalog(settings)
    namespace = settings.lake_namespace
    for name in (CHANGES, EDITS_PER_MINUTE):
        print(_table_summary(catalog, f"{namespace}.{name}"))
    newest = _newest_minutes(catalog, f"{namespace}.{EDITS_PER_MINUTE}")
    if newest.num_rows == 0:
        print("\nNo minutes counted yet.")
        return 0
    starts = newest["window_start"].to_pylist()
    print(f"\nNewest minutes ({_behind(starts[0])}). The newest may still be filling.")
    print("minute (UTC)       events   bots   wikis")
    for start, events, bots, wikis in zip(
        starts,
        newest["events_sum"].to_pylist(),
        newest["bot_events_sum"].to_pylist(),
        newest["wiki_count_distinct"].to_pylist(),
        strict=True,
    ):
        share = bots / events if events else 0.0
        print(f"{start:%Y-%m-%d %H:%M}  {events:>9,}  {share:>5.0%}  {wikis:>6}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
