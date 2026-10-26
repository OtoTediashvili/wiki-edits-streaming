"""`make peek`'s numbers, on real Iceberg tables in a temporary folder."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from tests.dashboard.lakes import (
    NAMESPACE,
    append_minutes,
    arrow,
    create_tables,
    minute,
    open_lake,
    schema,
)
from wikistream.streaming.peek import describe, footprint, newest_minutes, set_aside
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


def test_the_newest_minutes_of_a_live_job(tmp_path: Path) -> None:
    cat = create_tables(tmp_path)
    append_minutes(
        cat, [minute("enwiki", T0 + timedelta(minutes=m), humans=150) for m in range(12)]
    )
    # The newest minute is still filling: not full yet, and no straggler.
    last = T0 + timedelta(minutes=12)
    append_minutes(
        cat, [minute("enwiki", last, humans=30, bots=10), minute("dewiki", last, humans=2)]
    )

    text = newest_minutes(open_lake(tmp_path), last + timedelta(seconds=40))
    lines = text.splitlines()
    assert lines[0] == "Newest minutes (live). The newest may still be filling."
    assert lines[2] == "2026-10-19 10:12         42    24%       2"
    assert lines[-1] == "2026-10-19 10:03        150     0%       1"
    assert len(lines) == 2 + 10
    assert "straggling" not in text


def test_stragglers_ahead_of_a_replay_are_set_apart(tmp_path: Path) -> None:
    """Full minutes up to 10:11, and the quiet datacenter's live changes
    counted hours ahead: the job is nearly three hours behind, not live."""
    cat = create_tables(tmp_path)
    append_minutes(
        cat, [minute("enwiki", T0 + timedelta(minutes=m), humans=150) for m in range(12)]
    )
    ahead = T0 + timedelta(hours=3)
    append_minutes(cat, [minute("enwiki", ahead, humans=1), minute("frwiki", ahead, bots=2)])

    text = newest_minutes(open_lake(tmp_path), ahead + timedelta(seconds=30))
    lines = text.splitlines()
    assert lines[0] == "Newest full minutes (2.8 hours behind the clock: catching up)."
    assert lines[2].startswith("2026-10-19 10:11 ")
    assert lines[-1] == (
        "Ahead of these: 3 straggling changes, up to 2026-10-19 13:00 (live changes come "
        "mixed into a replay). The newest full minute is how far the job has got."
    )


def test_no_minutes_yet(tmp_path: Path) -> None:
    create_tables(tmp_path)
    assert newest_minutes(open_lake(tmp_path), T0) == "No minutes counted yet."
