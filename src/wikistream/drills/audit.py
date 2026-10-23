"""The verdict of a drill: the messages that went through Redpanda during
it, compared with what the lake holds.

- Every event sent is in `changes` exactly once, however many times the
  producer sent it.
- Every message that isn't a WikiChange record is in `rejected`.
- Every minute those events fall in is counted right: `edits_per_minute`
  agrees with the rows in `changes`, wiki by wiki.

Pure functions over Python values and Arrow tables, so each verdict is
tested without a running stack.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa

from wikistream.drills.topic import Sent
from wikistream.streaming.transform import MINUTE_FORMAT, time_ranges

Ranges = list[tuple[str, str]]


def _minute(when: datetime) -> datetime:
    return when.replace(second=0, microsecond=0)


def ranges_for(sent: list[Sent]) -> Ranges:
    """The minutes the sent events fall in, merged into ranges the way the
    job reads them, so the audit sees whole minutes."""
    minutes = {_minute(s.event_time).strftime(MINUTE_FORMAT) for s in sent if s.event_time}
    return time_ranges(sorted(minutes))


@dataclass(frozen=True)
class Miscount:
    wiki: str
    minute: datetime
    counted: int  # in edits_per_minute
    rows: int  # in changes


@dataclass(frozen=True)
class Audit:
    messages: int
    events: int  # distinct event ids among the messages
    poison: int  # messages that aren't WikiChange records
    missing: tuple[str, ...]  # sent, not in changes
    doubled: tuple[str, ...]  # in changes more than once
    not_set_aside: tuple[tuple[int, int], ...]  # poison not in rejected
    minutes: int  # (wiki, minute) pairs compared
    miscounted: tuple[Miscount, ...]

    @property
    def resent(self) -> int:
        """Copies in the topic of an event sent before: at-least-once at work."""
        return self.messages - self.poison - self.events

    @property
    def ok(self) -> bool:
        return not (self.missing or self.doubled or self.not_set_aside or self.miscounted)


def audit(
    sent: list[Sent], changes: pa.Table, minutes: pa.Table, rejected: set[tuple[int, int]]
) -> Audit:
    """`changes` and `minutes` must cover `ranges_for(sent)` in full."""
    ids = {s.event_id for s in sent if s.event_id is not None}
    in_lake = Counter(changes["event_id"].to_pylist())
    poison = sorted((s.partition, s.offset) for s in sent if s.event_id is None)

    rows = Counter(
        (wiki, _minute(when))
        for wiki, when in zip(
            changes["wiki"].to_pylist(), changes["event_time"].to_pylist(), strict=True
        )
    )
    counted = dict(
        zip(
            zip(minutes["wiki"].to_pylist(), minutes["window_start"].to_pylist(), strict=True),
            minutes["events"].to_pylist(),
            strict=True,
        )
    )
    keys = sorted(set(rows) | set(counted))
    miscounted = tuple(
        Miscount(wiki, minute, counted.get((wiki, minute), 0), rows.get((wiki, minute), 0))
        for wiki, minute in keys
        if counted.get((wiki, minute), 0) != rows.get((wiki, minute), 0)
    )
    return Audit(
        messages=len(sent),
        events=len(ids),
        poison=len(poison),
        missing=tuple(sorted(i for i in ids if in_lake[i] == 0)),
        doubled=tuple(sorted(i for i in ids if in_lake[i] > 1)),
        not_set_aside=tuple(p for p in poison if p not in rejected),
        minutes=len(keys),
        miscounted=miscounted,
    )
