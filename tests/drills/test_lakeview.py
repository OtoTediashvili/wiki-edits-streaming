"""Reading what the audit needs, from real Iceberg tables in a temp folder."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import Catalog

from tests.dashboard.lakes import (
    NAMESPACE,
    append_changes,
    append_minutes,
    change,
    create_tables,
    minute,
    schema,
)
from wikistream.drills.lakeview import LakeView
from wikistream.streaming.settings import REJECTED
from wikistream.streaming.transform import REJECTED_COLUMNS

T = datetime(2026, 10, 22, 18, 0)


def fill(cat: Catalog) -> None:
    times = [T - timedelta(days=4000), T, T + timedelta(minutes=1), T + timedelta(minutes=30)]
    append_changes(cat, [change(t, title=f"P{i}") for i, t in enumerate(times)])
    append_minutes(cat, [minute("enwiki", t.replace(second=0), humans=1) for t in times])


def test_only_the_rows_in_the_ranges(tmp_path: Path) -> None:
    cat = create_tables(tmp_path)
    fill(cat)
    view = LakeView(cat, NAMESPACE)
    old = (T - timedelta(days=4000)).strftime("%Y-%m-%d %H:%M:%S")
    ranges = [
        (old, (T - timedelta(days=4000, minutes=-1)).strftime("%Y-%m-%d %H:%M:%S")),
        ("2026-10-22 18:00:00", "2026-10-22 18:02:00"),
    ]
    rows = view.changes(ranges)
    assert set(rows.column_names) == {"event_id", "wiki", "event_time"}
    assert sorted(rows["event_time"].to_pylist()) == [
        T - timedelta(days=4000),
        T,
        T + timedelta(minutes=1),
    ]
    counted = view.minutes(ranges)
    assert set(counted.column_names) == {"wiki", "window_start", "events"}
    assert counted.num_rows == 3
    assert view.changes([]).num_rows == 0


def test_rejected_offsets_once_the_table_exists(tmp_path: Path) -> None:
    cat = create_tables(tmp_path)
    view = LakeView(cat, NAMESPACE)
    assert view.rejected() == set()

    table = cat.create_table(f"{NAMESPACE}.{REJECTED}", schema=schema(REJECTED_COLUMNS))
    stamp = datetime(2026, 10, 22, 18, 0, tzinfo=UTC)
    table.append(
        pa.Table.from_pylist(
            [
                {"kafka_partition": 1, "kafka_offset": 500, "kafka_timestamp": stamp,
                 "reason": "empty message", "value": None},
                {"kafka_partition": 2, "kafka_offset": 7, "kafka_timestamp": stamp,
                 "reason": "no schema registry header", "value": b"junk"},
            ],
            schema=schema(REJECTED_COLUMNS).as_arrow(),
        )
    )  # fmt: skip
    assert view.rejected() == {(1, 500), (2, 7)}
