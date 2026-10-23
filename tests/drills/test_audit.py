"""The audit's verdicts, one way of failing at a time."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pyarrow as pa

from wikistream.drills.audit import Miscount, audit, ranges_for
from wikistream.drills.topic import Sent

T = datetime(2026, 10, 22, 18, 0, 10)


def sent(*events: tuple[str | None, datetime | None], partition: int = 0) -> list[Sent]:
    return [Sent(partition, offset, eid, when) for offset, (eid, when) in enumerate(events)]


def changes(*rows: tuple[str, str, datetime]) -> pa.Table:
    ids, wikis, times = zip(*rows, strict=True) if rows else ((), (), ())
    return pa.table(
        {
            "event_id": pa.array(ids, pa.string()),
            "wiki": pa.array(wikis, pa.string()),
            "event_time": pa.array(times, pa.timestamp("us")),
        }
    )


def minutes(*rows: tuple[str, datetime, int]) -> pa.Table:
    wikis, starts, events = zip(*rows, strict=True) if rows else ((), (), ())
    return pa.table(
        {
            "wiki": pa.array(wikis, pa.string()),
            "window_start": pa.array(starts, pa.timestamp("us")),
            "events": pa.array(events, pa.int64()),
        }
    )


M = T.replace(second=0)
GOOD: dict[str, Any] = {
    # a sent twice (the producer's resend), b once; c was already in the lake
    "sent": sent(("a", T), ("b", T), ("a", T)),
    "changes": changes(("a", "enwiki", T), ("b", "enwiki", T), ("c", "dewiki", T)),
    "minutes": minutes(("enwiki", M, 2), ("dewiki", M, 1)),
    "rejected": set(),
}


def test_every_event_once_and_every_minute_right_passes() -> None:
    result = audit(**GOOD)
    assert result.ok
    assert (result.messages, result.events, result.resent, result.poison) == (3, 2, 1, 0)
    assert result.minutes == 2


def test_an_event_missing_from_the_lake_fails() -> None:
    result = audit(**{**GOOD, "sent": sent(("a", T), ("z", T))})
    assert not result.ok and result.missing == ("z",)


def test_an_event_written_twice_fails() -> None:
    twice = changes(("a", "enwiki", T), ("a", "enwiki", T), ("b", "enwiki", T), ("c", "dewiki", T))
    result = audit(
        **{**GOOD, "changes": twice, "minutes": minutes(("enwiki", M, 3), ("dewiki", M, 1))}
    )
    assert not result.ok and result.doubled == ("a",)


def test_a_minute_counted_wrong_fails() -> None:
    result = audit(**{**GOOD, "minutes": minutes(("enwiki", M, 1), ("dewiki", M, 1))})
    assert not result.ok
    assert result.miscounted == (Miscount("enwiki", M, counted=1, rows=2),)


def test_a_minute_never_counted_fails() -> None:
    result = audit(**{**GOOD, "minutes": minutes(("enwiki", M, 2))})
    assert result.miscounted == (Miscount("dewiki", M, counted=0, rows=1),)


def test_poison_must_be_set_aside() -> None:
    with_poison = [*GOOD["sent"], Sent(1, 9, None, None)]
    assert not audit(**{**GOOD, "sent": with_poison}).ok
    result = audit(**{**GOOD, "sent": with_poison, "rejected": {(1, 9)}})
    assert result.ok and result.poison == 1 and result.resent == 1


def test_ranges_cover_whole_minutes_and_skip_the_years_between() -> None:
    old = datetime(2009, 3, 9, 2, 8, 30)
    assert ranges_for(
        sent(("a", T), ("b", T + timedelta(minutes=3)), ("c", old), (None, None))
    ) == [
        ("2009-03-09 02:08:00", "2009-03-09 02:09:00"),
        ("2026-10-22 18:00:00", "2026-10-22 18:04:00"),
    ]


def test_a_count_with_no_rows_behind_it_fails() -> None:
    phantom = minutes(("enwiki", M, 2), ("dewiki", M, 1), ("frwiki", M, 4))
    result = audit(**{**GOOD, "minutes": phantom})
    assert result.miscounted == (Miscount("frwiki", M, counted=4, rows=0),)
