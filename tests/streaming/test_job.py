"""Supervising the two queries, and the progress log line. No Spark needed."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pyspark.sql.streaming.query import StreamingQuery
from structlog.testing import capture_logs

from wikistream.streaming.job import ProgressLog, await_queries


class Clock:
    """Counts polls: the supervisor's sleep advances it."""

    def __init__(self) -> None:
        self.ticks = 0

    def sleep(self, _seconds: float) -> None:
        self.ticks += 1


@dataclass
class FakeQuery:
    clock: Clock
    name: str
    ends_after: int | None = None  # polls until it finishes; None runs forever
    error: str | None = None
    stopped: bool = field(default=False)

    @property
    def isActive(self) -> bool:  # noqa: N802 (PySpark's name)
        finished = self.ends_after is not None and self.clock.ticks >= self.ends_after
        return not (self.stopped or finished)

    def exception(self) -> str | None:
        return None if self.isActive else self.error

    def stop(self) -> None:
        self.stopped = True


def supervise(*queries: FakeQuery, sleep: Any) -> None:
    await_queries(cast(list[StreamingQuery], list(queries)), poll_seconds=0, sleep=sleep)


def test_returns_when_both_queries_finish() -> None:
    clock = Clock()
    a = FakeQuery(clock, "changes", ends_after=1)
    b = FakeQuery(clock, "edits_per_minute", ends_after=3)
    supervise(a, b, sleep=clock.sleep)
    assert clock.ticks == 3
    assert not a.stopped and not b.stopped


def test_one_failure_stops_the_other_and_raises() -> None:
    clock = Clock()
    failed = FakeQuery(clock, "changes", ends_after=2, error="S3 unreachable")
    healthy = FakeQuery(clock, "edits_per_minute")
    with pytest.raises(RuntimeError, match="changes failed: S3 unreachable"):
        supervise(failed, healthy, sleep=clock.sleep)
    assert healthy.stopped


def test_ctrl_c_stops_both() -> None:
    clock = Clock()
    a, b = FakeQuery(clock, "changes"), FakeQuery(clock, "edits_per_minute")

    def interrupted(_seconds: float) -> None:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        supervise(a, b, sleep=interrupted)
    assert a.stopped and b.stopped


def test_progress_line_reports_rows_rate_and_lag() -> None:
    progress = SimpleNamespace(
        name="lake",
        batchId=7,
        numInputRows=1800,
        processedRowsPerSecond=950.25,
        durationMs={"triggerExecution": 1894},
        sources=[SimpleNamespace(metrics={"maxOffsetsBehindLatest": "12"})],
    )
    with capture_logs() as logs:
        ProgressLog().onQueryProgress(SimpleNamespace(progress=progress))
    assert logs == [
        {
            "event": "query.progress",
            "log_level": "info",
            "query": "lake",
            "batch": 7,
            "rows": 1800,
            "rows_per_second": 950.2,
            "batch_ms": 1894,
            "offsets_behind": "12",
        }
    ]


def test_a_failed_query_is_logged_as_an_error() -> None:
    with capture_logs() as logs:
        ProgressLog().onQueryTerminated(
            SimpleNamespace(id="q1", exception="org.apache.kafka...OffsetOutOfRange\n  at ...")
        )
    assert logs[0]["log_level"] == "error"
    assert logs[0]["error"] == "org.apache.kafka...OffsetOutOfRange"
