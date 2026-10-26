"""Look into the lake from this machine, without Spark: `make peek`.

Reads the tables through the same REST catalog with PyIceberg, like the
dashboard does. For each table: rows and files now, snapshots kept, and
what it takes on disk, which is what maintenance (`make maintain`) brings
down.
"""

from __future__ import annotations

import sys
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from urllib.parse import urlparse

import pyarrow.compute as pc
from pyarrow.fs import FileSelector, FileType
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.table import Table

from wikistream.dashboard.lake import Lake
from wikistream.streaming.catalog import rest_catalog
from wikistream.streaming.settings import CHANGES, EDITS_PER_MINUTE, REJECTED, LakeSettings


@dataclass(frozen=True)
class Footprint:
    """A table as it is (the current snapshot) and as it sits on disk (every
    object under its location: data files old snapshots still hold, files no
    snapshot holds, and metadata)."""

    rows: int
    data_files: int
    data_bytes: int
    snapshots: int
    oldest_snapshot: datetime | None
    disk_objects: int
    disk_bytes: int
    files_by_day: dict[date, int]  # current data files per day partition


def _files_by_day(table: Table) -> dict[date, int]:
    """From the manifests: which day partition each current data file is in."""
    files = table.inspect.files()
    if files.num_rows == 0 or files.schema.field("partition").type.num_fields == 0:
        return {}
    data = files.filter(pc.equal(files["content"], 0))
    return dict(Counter(data["partition"].combine_chunks().field(0).to_pylist()))


def footprint(table: Table) -> Footprint:
    snapshot = table.current_snapshot()
    totals = snapshot.summary.additional_properties if snapshot and snapshot.summary else {}
    snapshots = table.metadata.snapshots
    oldest = min((s.timestamp_ms for s in snapshots), default=None)
    location = urlparse(table.location())
    if not isinstance(table.io, PyArrowFileIO):
        raise TypeError(f"can't list {table.location()} with {type(table.io).__name__}")
    fs = table.io.fs_by_scheme(location.scheme, location.netloc)
    listing = fs.get_file_info(FileSelector(f"{location.netloc}{location.path}", recursive=True))
    objects = [info for info in listing if info.type == FileType.File]
    return Footprint(
        rows=int(totals.get("total-records", 0)),
        data_files=int(totals.get("total-data-files", 0)),
        data_bytes=int(totals.get("total-files-size", 0)),
        snapshots=len(snapshots),
        oldest_snapshot=datetime.fromtimestamp(oldest / 1000, UTC) if oldest else None,
        disk_objects=len(objects),
        disk_bytes=sum(info.size for info in objects),
        files_by_day=_files_by_day(table),
    )


def _size(size: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1000 or unit == "GB":
            return f"{size:,.0f} {unit}" if unit == "B" else f"{size:,.1f} {unit}"
        size /= 1000
    raise AssertionError("unreachable")


def _n(count: int, noun: str) -> str:
    return f"{count:,} {noun}" + ("" if count == 1 else "s")


def describe(identifier: str, table: Footprint, now: datetime) -> str:
    if table.snapshots == 0:
        return f"{identifier}: empty"
    average = table.data_bytes / table.data_files if table.data_files else 0
    oldest = table.oldest_snapshot
    age = f", the oldest {(now - oldest).total_seconds() / 3600:.1f} h old" if oldest else ""
    held = table.disk_bytes - table.data_bytes
    today = table.files_by_day.get(now.date(), 0)
    other_days = len(table.files_by_day) - (1 if today else 0)
    return (
        f"{identifier}: {_n(table.rows, 'row')} in {_n(table.data_files, 'file')} "
        f"({_size(table.data_bytes)}, {_size(average)} each on average)\n"
        f"  files: {today:,} today (UTC), {_n(table.data_files - today, 'file')} across "
        f"{_n(other_days, 'other day')}\n"
        f"  {_n(table.snapshots, 'snapshot')}{age}\n"
        f"  on disk: {_size(table.disk_bytes)} in {_n(table.disk_objects, 'object')}; "
        f"{_size(held)} of it is metadata and files only old snapshots hold"
    )


def set_aside(table: Table, identifier: str) -> str:
    """One line on the rejected table: how many, the latest, and why."""
    rows = table.scan(selected_fields=("kafka_timestamp", "reason")).to_arrow()
    if rows.num_rows == 0:
        return f"{identifier}: nothing set aside"
    latest = max(rows["kafka_timestamp"].to_pylist())
    reasons = Counter(rows["reason"].to_pylist())
    why = ", ".join(f"{count} {reason}" for reason, count in reasons.most_common())
    return (
        f"{identifier}: {_n(rows.num_rows, 'message')} set aside, "
        f"the latest from {latest:%Y-%m-%d %H:%M} UTC ({why})"
    )


def _behind(newest: datetime, now: datetime) -> str:
    seconds = (now - newest).total_seconds()
    if seconds < 600:
        return "live"
    if seconds < 86_400:
        return f"{seconds / 3600:.1f} hours behind the clock: catching up"
    return f"{seconds / 86_400:.1f} days behind the clock: catching up"


def newest_minutes(lake: Lake, now: datetime, minutes: int = 10) -> str:
    """Totals for the newest minutes counted, and how far behind the clock
    (`now`, naive UTC) they are: while the job catches up on history, hours
    or days. During a replay a few straggling changes can be counted hours
    ahead of the rest; the list then ends at the newest full minute, and a
    line says what is counted ahead of it."""
    position = lake.position()
    if position.newest_minute is None:
        return "No minutes counted yet."
    full = position.full_minute
    split = full is not None and position.straggling
    end = full if split and full is not None else position.newest_minute
    rows = lake.minutes(end - timedelta(minutes=minutes - 1), end + timedelta(minutes=1))
    per_minute = rows.group_by("window_start").aggregate(
        [("events", "sum"), ("bot_events", "sum"), ("wiki", "count_distinct")]
    )
    per_minute = per_minute.sort_by([("window_start", "descending")])
    if split:
        lines = [f"Newest full minutes ({_behind(end, now)})."]
    else:
        lines = [f"Newest minutes ({_behind(end, now)}). The newest may still be filling."]
    lines.append("minute (UTC)       events   bots   wikis")
    for start, events, bots, wikis in zip(
        per_minute["window_start"].to_pylist(),
        per_minute["events_sum"].to_pylist(),
        per_minute["bot_events_sum"].to_pylist(),
        per_minute["wiki_count_distinct"].to_pylist(),
        strict=True,
    ):
        share = bots / events if events else 0.0
        lines.append(f"{start:%Y-%m-%d %H:%M}  {events:>9,}  {share:>5.0%}  {wikis:>6}")
    if split:
        lines.append(
            f"Ahead of these: {_n(position.ahead, 'straggling change')}, up to "
            f"{position.newest_minute:%Y-%m-%d %H:%M} (live changes come mixed into a "
            "replay). The newest full minute is how far the job has got."
        )
    return "\n".join(lines)


def main() -> int:
    settings = LakeSettings()
    catalog = rest_catalog(settings)
    namespace = settings.lake_namespace
    now = datetime.now(UTC)
    for name in (CHANGES, EDITS_PER_MINUTE):
        identifier = f"{namespace}.{name}"
        print(describe(identifier, footprint(catalog.load_table(identifier)), now))
    rejected = f"{namespace}.{REJECTED}"
    if catalog.table_exists(rejected):
        print(set_aside(catalog.load_table(rejected), rejected))
    print()
    print(newest_minutes(Lake(catalog, namespace), now.replace(tzinfo=None)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
