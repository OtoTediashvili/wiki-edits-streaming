"""Reading the lake: real Iceberg tables in a temporary folder."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import DataScan, Table

from tests.dashboard.lakes import (
    NAMESPACE,
    append_changes,
    append_minutes,
    catalog,
    change,
    create_tables,
    minute,
    open_lake,
)
from wikistream.dashboard.lake import (
    CHANGE_FIELDS,
    Lake,
    LakeState,
    Position,
    naive_utc,
    position,
)

DAY = datetime(2026, 10, 19)


@pytest.fixture
def cat(tmp_path: Path) -> Catalog:
    return create_tables(tmp_path)


@pytest.fixture
def lake(tmp_path: Path, cat: Catalog) -> Lake:
    return open_lake(tmp_path)


@pytest.fixture
def files_opened(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[int]]:
    """How many data files each scan planned to open."""
    opened: list[int] = []
    to_arrow = DataScan.to_arrow

    def counting(self: DataScan) -> pa.Table:
        opened.append(len(list(self.plan_files())))
        return to_arrow(self)

    monkeypatch.setattr(DataScan, "to_arrow", counting)
    yield opened


def test_the_newest_minute_and_last_write_come_from_metadata_alone(
    cat: Catalog, lake: Lake, monkeypatch: pytest.MonkeyPatch
) -> None:
    before = datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    append_minutes(cat, [minute("enwiki", DAY + timedelta(hours=9), humans=1)])
    # One commit, one file, holding 10:30 to 10:42: its upper bound is the answer.
    append_minutes(
        cat,
        [minute("enwiki", DAY + timedelta(hours=10, minutes=m), humans=1) for m in (30, 42, 35)],
    )
    # A straggler from 2009 and a late minute land after the newest one.
    straggler = minute("enwiki", datetime(2009, 3, 9, 2, 8), humans=1)
    late = minute("dewiki", DAY + timedelta(hours=10, minutes=3), humans=1)
    append_minutes(cat, [straggler, late])

    def no_data_files(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("state() must not scan data files")

    monkeypatch.setattr(Table, "scan", no_data_files)
    state = lake.state()
    assert state.newest_minute == DAY + timedelta(hours=10, minutes=42)
    assert state.last_write is not None
    assert (
        before - timedelta(seconds=1) <= state.last_write <= datetime.now(UTC).replace(tzinfo=None)
    )


def test_empty_tables_have_no_state(lake: Lake) -> None:
    assert lake.state() == LakeState(None, None)


def test_missing_tables_say_so(tmp_path: Path) -> None:
    """Before the job's first start: a catalog, no tables."""
    with pytest.raises(NoSuchTableError):
        Lake(catalog(tmp_path), NAMESPACE).state()


def test_the_position_is_the_newest_full_minute_not_a_straggler_ahead(
    cat: Catalog, lake: Lake
) -> None:
    """A replay: full minutes up to 09:59, and the quiet datacenter's live
    changes, a couple a minute, already counted two hours ahead."""
    bulk = DAY + timedelta(hours=9)
    append_minutes(
        cat,
        [minute("enwiki", bulk + timedelta(minutes=m), humans=60, bots=20) for m in range(60)]
        # Two wikis that make a full minute only together.
        + [minute("dewiki", bulk + timedelta(minutes=60), humans=50)]
        + [minute("frwiki", bulk + timedelta(minutes=60), bots=50)],
    )
    ahead = DAY + timedelta(hours=12)
    append_minutes(cat, [minute("enwiki", ahead, humans=1), minute("commonswiki", ahead, bots=2)])
    append_minutes(cat, [minute("enwiki", ahead + timedelta(minutes=3), humans=1)])

    assert lake.position() == Position(
        full_minute=bulk + timedelta(minutes=60),
        newest_minute=ahead + timedelta(minutes=3),
        ahead=4,
    )
    # Looking back only an hour from 12:10: stragglers, no full minute.
    assert lake.position(since=ahead - timedelta(minutes=50)) == Position(
        None, ahead + timedelta(minutes=3), 4
    )


def test_the_position_of_a_lake_without_counts(lake: Lake) -> None:
    assert lake.position() == Position(None, None, 0)
    assert position(pa.table({"window_start": pa.array([], pa.timestamp("us"))})) == Position(
        None, None, 0
    )


def test_minutes_in_a_window_and_only_their_files(
    cat: Catalog, lake: Lake, files_opened: list[int]
) -> None:
    # Three commits, three files: 08:xx, 09:xx and 10:xx.
    for hour in (8, 9, 10):
        start = DAY + timedelta(hours=hour)
        append_minutes(
            cat, [minute("enwiki", start + timedelta(minutes=m), humans=m) for m in range(60)]
        )

    rows = lake.minutes(DAY + timedelta(hours=9, minutes=30), DAY + timedelta(hours=9, minutes=45))
    starts = rows["window_start"].to_pylist()
    assert starts == [DAY + timedelta(hours=9, minutes=m) for m in range(30, 45)]
    assert rows.schema.field("window_start").type == pa.timestamp("us")  # naive UTC
    assert files_opened == [1]  # the 08:xx and 10:xx files were never opened


def test_changes_in_a_window_with_only_the_columns_shown(
    cat: Catalog, lake: Lake, files_opened: list[int]
) -> None:
    times = [DAY + timedelta(hours=10, minutes=m, seconds=30) for m in range(5)]
    append_changes(cat, [change(t, title=f"Page {i}") for i, t in enumerate(times)])
    append_changes(cat, [change(DAY - timedelta(days=3), title="Three days ago")])

    rows = lake.changes(times[1].replace(second=0), times[4].replace(second=0))
    assert rows.column_names == list(CHANGE_FIELDS)
    assert rows["title"].to_pylist() == ["Page 1", "Page 2", "Page 3"]
    assert rows["event_time"].to_pylist() == times[1:4]
    assert files_opened == [1]


def test_aware_timestamps_become_naive_utc_and_nothing_else_changes() -> None:
    instant = datetime(2026, 10, 19, 10, 0, tzinfo=UTC)
    table = pa.table(
        {
            "aware": pa.array([instant], pa.timestamp("us", tz="UTC")),
            "tbilisi": pa.array([instant], pa.timestamp("ms", tz="Asia/Tbilisi")),
            "naive": pa.array([datetime(2026, 1, 1)], pa.timestamp("us")),
            "n": pa.array([1], pa.int64()),
        }
    )
    out = naive_utc(table)
    assert out.schema.field("aware").type == pa.timestamp("us")
    assert out.schema.field("tbilisi").type == pa.timestamp("ms")
    assert out["aware"].to_pylist() == out["tbilisi"].to_pylist() == [datetime(2026, 10, 19, 10)]
    assert out["naive"].to_pylist() == [datetime(2026, 1, 1)]
    assert out["n"].to_pylist() == [1]
