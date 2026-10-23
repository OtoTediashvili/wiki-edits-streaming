"""The row mapping and the per-minute counts, on the recorded events."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from tests.streaming.frames import decoded_schema, undecodable
from wikistream.streaming.transform import (
    CHANGE_COLUMNS,
    MINUTE_COLUMNS,
    minutes_touched,
    per_minute,
    recount,
    rejects,
    time_ranges,
    to_changes,
    unseen,
)

BY_TYPE = {
    "edit": "edits",
    "new": "new_pages",
    "log": "log_events",
    "categorize": "categorize_events",
}


def frame(spark: SparkSession, rows: list[dict[str, Any]]) -> DataFrame:
    return spark.createDataFrame(rows, schema=decoded_schema())


def test_changes_have_the_table_columns_in_order_and_type(
    spark: SparkSession, decoded: list[dict[str, Any]]
) -> None:
    changes = to_changes(frame(spark, decoded))

    assert changes.columns == [c.name for c in CHANGE_COLUMNS]
    types = [f.dataType.simpleString() for f in changes.schema.fields]
    assert types == [c.sql_type.lower() for c in CHANGE_COLUMNS]
    assert "raw" not in changes.columns
    assert changes.count() == 95


def test_length_delta_is_the_size_change_or_null(
    spark: SparkSession, decoded: list[dict[str, Any]]
) -> None:
    rows = to_changes(frame(spark, decoded)).collect()

    for row in rows:
        if row.length_old is not None and row.length_new is not None:
            assert row.length_delta == row.length_new - row.length_old
        else:
            assert row.length_delta is None
    # The recording has both kinds: edits with sizes, log entries without.
    assert any(r.length_delta is not None for r in rows)
    assert any(r.length_delta is None for r in rows)


def test_per_minute_counts_match_a_count_by_hand(
    spark: SparkSession, decoded: list[dict[str, Any]]
) -> None:
    windows = per_minute(to_changes(frame(spark, decoded)))

    assert windows.columns == [c.name for c in MINUTE_COLUMNS]

    expected: dict[tuple[datetime, str], Counter[str]] = defaultdict(Counter)
    editors: dict[tuple[datetime, str], set[str]] = defaultdict(set)
    for r in decoded:
        minute = r["event_time"].replace(second=0, microsecond=0)
        c = expected[(minute, r["wiki"])]
        c["events"] += 1
        c[BY_TYPE[r["type"]]] += 1
        c["bot_events" if r["bot"] else "human_events"] += 1
        editors[(minute, r["wiki"])].add(r["user"])
        old, new = r["length_old"], r["length_new"]
        if old is not None and new is not None:
            c["bytes_added"] += max(new - old, 0)
            c["bytes_removed"] += max(old - new, 0)

    got = {(w.window_start, w.wiki): w for w in windows.collect()}
    assert set(got) == set(expected)
    for key, c in expected.items():
        w = got[key]
        for column in (
            "events",
            "edits",
            "new_pages",
            "log_events",
            "categorize_events",
            "bot_events",
            "human_events",
            "bytes_added",
            "bytes_removed",
        ):
            assert getattr(w, column) == c[column], (key, column)
        assert (w.window_end - w.window_start).total_seconds() == 60
        assert w.editors == len(editors[key])
    assert sum(w.events for w in got.values()) == 95


def test_unseen_drops_copies_and_events_already_written_whatever_their_age(
    spark: SparkSession, decoded: list[dict[str, Any]]
) -> None:
    rows = to_changes(frame(spark, decoded + decoded[:3]))
    already = rows.limit(10).select("event_id")

    new = unseen(rows, already)

    ids = [r.event_id for r in new.collect()]
    assert len(ids) == len(set(ids)) == 85
    assert not set(ids) & {r.event_id for r in already.collect()}


def test_recount_covers_only_the_minutes_asked_for(
    spark: SparkSession, decoded: list[dict[str, Any]]
) -> None:
    changes = to_changes(frame(spark, decoded))
    touched = minutes_touched(changes.limit(1))
    (asked,) = [r.minute for r in touched.collect()]

    counts = recount(changes, touched).collect()

    assert counts and {w.window_start for w in counts} == {asked}
    in_that_minute = [r for r in decoded if r["event_time"].replace(second=0) == asked]
    assert sum(w.events for w in counts) == len(in_that_minute)


def test_time_ranges_join_nearby_minutes_and_keep_far_ones_apart() -> None:
    minutes = [
        "2026-10-19 06:03:00",
        "2026-10-19 06:01:00",  # unsorted, as Spark returns them
        "2026-10-19 06:02:00",
        "2026-10-19 06:11:00",  # 8 minutes after 06:03 ends: same range
        "2009-03-09 02:08:00",  # years earlier: its own range
        "2026-10-19 06:40:00",  # 29 minutes later: its own range
        "2026-10-19 06:01:00",  # a repeat
    ]
    assert time_ranges(minutes) == [
        ("2009-03-09 02:08:00", "2009-03-09 02:09:00"),
        ("2026-10-19 06:01:00", "2026-10-19 06:12:00"),
        ("2026-10-19 06:40:00", "2026-10-19 06:41:00"),
    ]
    assert time_ranges([]) == []
    assert time_ranges(["2026-10-19 06:01:00", "2026-10-19 06:05:00"], gap_minutes=1) == [
        ("2026-10-19 06:01:00", "2026-10-19 06:02:00"),
        ("2026-10-19 06:05:00", "2026-10-19 06:06:00"),
    ]


def test_rejects_say_why_and_keep_the_bytes(spark: SparkSession) -> None:
    rows = [
        undecodable(b"plain text", partition=0, offset=1),
        undecodable(b"\x00\x01", partition=0, offset=2),  # too short for the header
        undecodable(b"\x00\x00\x00\x00\x07garbage", partition=1, offset=3),
        undecodable(b"", partition=1, offset=4),
        undecodable(None, partition=2, offset=5),
    ]
    out = rejects(spark.createDataFrame(rows, decoded_schema())).collect()
    assert [(r.kafka_offset, r.reason) for r in out] == [
        (1, "no schema registry header"),
        (2, "no schema registry header"),
        (3, "does not decode as a WikiChange record"),
        (4, "empty message"),
        (5, "empty message"),
    ]
    assert out[0].value == b"plain text" and out[4].value is None
    assert out[0].kafka_timestamp == datetime(2026, 10, 22, 18, 0)


def test_only_records_that_decoded_become_changes(
    spark: SparkSession, decoded: list[dict[str, Any]]
) -> None:
    rows = [*decoded[:5], undecodable(b"junk", partition=0, offset=99)]
    changes = to_changes(spark.createDataFrame(rows, decoded_schema()))
    assert changes.count() == 5
    assert rejects(spark.createDataFrame(rows, decoded_schema())).count() == 1
