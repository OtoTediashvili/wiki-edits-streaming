"""Table maintenance: what runs, that Spark parses it, and that a failure
or a slow run can't take the stream job down."""

from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from pyspark.sql import Row, SparkSession
from structlog.testing import capture_logs

from wikistream.streaming import maintenance
from wikistream.streaming.maintenance import Schedule, plan
from wikistream.streaming.settings import LakeSettings

NOW = datetime(2026, 10, 19, 14, 37, 5)


def settings(**overrides: Any) -> LakeSettings:
    return LakeSettings(_env_file=None, **overrides)  # type: ignore[call-arg]


def test_compacts_only_the_days_that_ended_then_expires_both_tables() -> None:
    tasks = plan(settings(), NOW)
    assert [(t.action, t.table) for t in tasks] == [
        ("compact", "wiki.changes"),
        ("expire", "wiki.changes"),
        ("expire", "wiki.edits_per_minute"),
    ]
    compact, expire, _ = tasks
    assert compact.sql.startswith("CALL lake.system.rewrite_data_files(table => 'wiki.changes'")
    assert r"where => 'event_time < TIMESTAMP \'2026-10-19 00:00:00\''" in compact.sql
    assert "'min-input-files', '5'" in compact.sql
    assert "'target-file-size-bytes', '134217728'" in compact.sql
    # Six hours of snapshots by default, and never fewer than ten.
    assert "older_than => TIMESTAMP '2026-10-19 08:37:05'" in expire.sql
    assert "retain_last => 10" in expire.sql


def test_retention_and_namespace_come_from_settings() -> None:
    tasks = plan(
        settings(lake_namespace="e2e_x", snapshot_retention_hours=0, snapshot_retain_last=1), NOW
    )
    assert {t.table for t in tasks} == {"e2e_x.changes", "e2e_x.edits_per_minute"}
    assert "older_than => TIMESTAMP '2026-10-19 14:37:05'" in tasks[1].sql
    assert "retain_last => 1" in tasks[1].sql


def test_spark_parses_every_call_and_the_nested_filter(spark: SparkSession) -> None:
    parser = spark._jsparkSession.sessionState().sqlParser()
    for task in plan(settings(), NOW):
        parsed = parser.parsePlan(task.sql).toString()  # raises ParseException on bad syntax
        assert "UnresolvedProcedure [lake, system" in parsed
    # The filter survives its quoting as the expression rewrite_data_files parses.
    compact = parser.parsePlan(plan(settings(), NOW)[0].sql).toString()
    assert "where => event_time < TIMESTAMP '2026-10-19 00:00:00'" in compact
    parser.parseExpression("event_time < TIMESTAMP '2026-10-19 00:00:00'")


class FakeResult:
    def __init__(self, row: Row | None) -> None:
        self.row = row

    def first(self) -> Row | None:
        return self.row


class FakeSpark:
    """Answers each procedure like Iceberg does: one row of counts."""

    def __init__(self, fail_on: str | None = None) -> None:
        self.fail_on = fail_on
        self.calls: list[str] = []

    def sql(self, statement: str) -> FakeResult:
        self.calls.append(statement)
        if self.fail_on and self.fail_on in statement:
            raise RuntimeError(
                "An error occurred while calling o81.sql.\n"
                ": org.apache.iceberg.exceptions.CommitFailedException: "
                "Cannot commit, found new delete for replaced data file"
            )
        if "rewrite_data_files" in statement:
            return FakeResult(Row(rewritten_data_files_count=1440, added_data_files_count=2))
        return FakeResult(Row(deleted_data_files_count=300, deleted_manifest_files_count=12))


def test_each_result_is_logged() -> None:
    spark = FakeSpark()
    with capture_logs() as logs:
        outcomes = maintenance.run(spark, settings(), NOW)  # type: ignore[arg-type]
    assert all(o.ok for o in outcomes)
    assert len(spark.calls) == 3
    compacted = next(e for e in logs if e["event"] == "maintenance.compact")
    assert compacted["table"] == "wiki.changes"
    assert compacted["rewritten_data_files_count"] == 1440
    expired = [e for e in logs if e["event"] == "maintenance.expire"]
    assert [e["table"] for e in expired] == ["wiki.changes", "wiki.edits_per_minute"]
    assert expired[0]["deleted_data_files_count"] == 300


def test_a_failed_compaction_does_not_stop_the_expiry() -> None:
    spark = FakeSpark(fail_on="rewrite_data_files")
    with capture_logs() as logs:
        outcomes = maintenance.run(spark, settings(), NOW)  # type: ignore[arg-type]
    assert [o.ok for o in outcomes] == [False, True, True]
    [failed] = [e for e in logs if e["event"] == "maintenance.failed"]
    assert failed["action"] == "compact"
    # The Java cause, not py4j's "An error occurred while calling o81.sql."
    assert failed["error"].startswith("org.apache.iceberg.exceptions.CommitFailedException")


class Clock:
    def __init__(self) -> None:
        self.seconds = 0.0

    def __call__(self) -> float:
        return self.seconds

    def advance(self, minutes: float) -> None:
        self.seconds += minutes * 60


def test_the_schedule_waits_after_a_start_then_runs_every_interval() -> None:
    clock, runs = Clock(), []
    schedule = Schedule(lambda: runs.append(clock()), every=timedelta(minutes=60), clock=clock)
    for _ in range(9):  # polls during the first 9 minutes
        clock.advance(1)
        schedule.tick()
    assert runs == []
    clock.advance(1)
    schedule.tick()
    schedule.tick()  # the same moment again: not twice
    assert len(runs) == 1
    clock.advance(59)
    schedule.tick()
    assert len(runs) == 1
    clock.advance(1)
    schedule.tick()
    assert len(runs) == 2


def test_the_schedule_never_raises() -> None:
    clock = Clock()

    def broken() -> None:
        raise RuntimeError("session gone")

    schedule = Schedule(broken, every=timedelta(minutes=60), first_after=timedelta(0), clock=clock)
    with capture_logs() as logs:
        schedule.tick()
    assert logs[0]["event"] == "maintenance.crashed"
    assert logs[0]["error"] == "session gone"


class FakeSession:
    def __init__(self) -> None:
        self.stopped = False
        self.streams = SimpleNamespace(addListener=lambda listener: None)

    def stop(self) -> None:
        self.stopped = True


@pytest.fixture
def cli(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """The command line with Spark, the queries and the supervisor faked."""
    from wikistream.streaming import __main__ as main_module

    seen = SimpleNamespace(session=FakeSession(), outcomes=[], on_tick="not called")
    monkeypatch.setattr(main_module, "spark_session", lambda _settings: seen.session)
    monkeypatch.setattr(main_module, "_stop_on_sigterm", lambda: None)
    monkeypatch.setattr(main_module, "start_queries", lambda *a, **k: [])
    monkeypatch.setattr(maintenance, "run", lambda *a: seen.outcomes)

    def supervise(_queries: Any, on_tick: Any = None) -> None:
        seen.on_tick = on_tick

    monkeypatch.setattr(main_module, "await_queries", supervise)
    seen.main = main_module.main
    return seen


def outcome(ok: bool) -> maintenance.Outcome:
    return maintenance.Outcome(maintenance.Task("expire", "wiki.changes", "CALL ..."), ok, 0.1)


def test_make_maintain_exits_non_zero_if_anything_failed(cli: SimpleNamespace) -> None:
    cli.outcomes = [outcome(True), outcome(True), outcome(True)]
    assert cli.main(["maintain"]) == 0
    assert cli.session.stopped
    cli.outcomes = [outcome(False), outcome(True), outcome(True)]
    assert cli.main(["maintain"]) == 1


def test_the_running_job_maintains_its_tables_unless_told_not_to(
    cli: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    cli.main(["run"])
    assert callable(cli.on_tick)  # the schedule's tick
    cli.main(["once"])  # the end-to-end test's mode: no maintenance in it
    assert cli.on_tick is None
    monkeypatch.setenv("WIKISTREAM_MAINTENANCE_EVERY_MINUTES", "0")
    cli.main(["run"])
    assert cli.on_tick is None
