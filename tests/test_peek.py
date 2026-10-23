"""`make peek`'s numbers, on real Iceberg tables in a temporary folder."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from tests.dashboard.lakes import NAMESPACE, arrow, create_tables, minute, schema
from wikistream.streaming.peek import describe, footprint, set_aside
from wikistream.streaming.settings import EDITS_PER_MINUTE, REJECTED
from wikistream.streaming.transform import MINUTE_COLUMNS, REJECTED_COLUMNS

T0 = datetime(2026, 10, 19, 10, 0)


def test_old_snapshots_keep_superseded_files_on_disk(tmp_path: Path) -> None:
    table = create_tables(tmp_path).load_table(f"{NAMESPACE}.{EDITS_PER_MINUTE}")
    for m in range(4):
        table.append(arrow(MINUTE_COLUMNS, [minute("enwiki", T0 + timedelta(minutes=m), humans=1)]))
    # Like the job's copy-on-write MERGE: the current table is one new file.
    table.overwrite(arrow(MINUTE_COLUMNS, [minute("enwiki", T0, humans=9)]))

    now = datetime.now(UTC)
    stats = footprint(table)
    assert (stats.rows, stats.data_files) == (1, 1)
    assert stats.snapshots == 6  # 4 appends; PyIceberg overwrites in two commits
    assert stats.oldest_snapshot is not None
    assert now - timedelta(minutes=1) < stats.oldest_snapshot <= now
    # On disk: the four superseded data files too, and the metadata.
    parquet = list(Path(tmp_path).rglob("*.parquet"))
    assert len(parquet) == 5
    assert stats.disk_objects > len(parquet)
    assert stats.disk_bytes > stats.data_bytes > 0

    assert stats.files_by_day == {T0.date(): 1}
    text = describe("wiki.edits_per_minute", stats, now + timedelta(hours=2))
    assert text.startswith("wiki.edits_per_minute: 1 row in 1 file (")
    assert "6 snapshots, the oldest 2.0 h old" in text
    assert f"in {stats.disk_objects:,} objects" in text


def test_files_are_counted_by_day_so_compaction_shows(tmp_path: Path) -> None:
    table = create_tables(tmp_path).load_table(f"{NAMESPACE}.{EDITS_PER_MINUTE}")
    for day, files in ((0, 3), (1, 1), (9, 2)):
        for i in range(files):
            start = T0 - timedelta(days=day, minutes=i)
            table.append(arrow(MINUTE_COLUMNS, [minute("enwiki", start, humans=1)]))
    stats = footprint(table)
    assert stats.files_by_day == {
        T0.date(): 3,
        (T0 - timedelta(days=1)).date(): 1,
        (T0 - timedelta(days=9)).date(): 2,
    }
    text = describe("wiki.edits_per_minute", stats, datetime.combine(T0.date(), T0.time(), UTC))
    assert "files: 3 today (UTC), 3 files across 2 other days" in text


def test_an_empty_table(tmp_path: Path) -> None:
    table = create_tables(tmp_path).load_table(f"{NAMESPACE}.{EDITS_PER_MINUTE}")
    stats = footprint(table)
    assert (stats.rows, stats.data_files, stats.snapshots) == (0, 0, 0)
    assert stats.files_by_day == {}
    assert stats.oldest_snapshot is None
    assert describe("wiki.edits_per_minute", stats, datetime.now(UTC)).endswith(": empty")


def test_what_was_set_aside_and_why(tmp_path: Path) -> None:
    cat = create_tables(tmp_path)
    table = cat.create_table(f"{NAMESPACE}.{REJECTED}", schema=schema(REJECTED_COLUMNS))
    assert set_aside(table, "wiki.rejected") == "wiki.rejected: nothing set aside"
    rows = [
        (0, 1, T0, "no schema registry header"),
        (1, 2, T0 + timedelta(minutes=5), "no schema registry header"),
        (2, 3, T0 + timedelta(minutes=1), "empty message"),
    ]
    table.append(arrow(REJECTED_COLUMNS, [
        {"kafka_partition": p, "kafka_offset": o, "kafka_timestamp": t, "reason": r, "value": b"x"}
        for p, o, t, r in rows
    ]))  # fmt: skip
    assert set_aside(table, "wiki.rejected") == (
        "wiki.rejected: 3 messages set aside, the latest from 2026-10-19 10:05 UTC "
        "(2 no schema registry header, 1 empty message)"
    )
