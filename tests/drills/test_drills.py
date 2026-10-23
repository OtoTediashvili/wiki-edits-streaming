"""The drills and their runner, with the stack, the topic and the lake faked:
what each drill breaks, in what order, and what the report says."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from wikistream.dashboard.lake import LakeState
from wikistream.drills import drills, stack, topic
from wikistream.drills.drills import Context, NotReadyError, preflight, render, run, save
from wikistream.drills.topic import Sent
from wikistream.streaming.settings import LakeSettings

T = datetime(2026, 10, 22, 18, 0, 10)


class Clock:
    def __init__(self) -> None:
        self.seconds = 0.0

    def __call__(self) -> float:
        return self.seconds

    def sleep(self, seconds: float) -> None:
        self.seconds += seconds


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def ctx(clock: Clock) -> Context:
    return Context(
        lake=LakeSettings(_env_file=None),  # type: ignore[call-arg]
        bootstrap_servers="localhost:19092",
        topic="wiki.recentchange",
        sleep=clock.sleep,
        clock=clock,
        now=lambda: T + timedelta(seconds=clock.seconds),
    )


@pytest.fixture
def broken(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Every stack action, in order, instead of doing it."""
    done: list[str] = []
    for action in ("kill", "stop", "start"):
        monkeypatch.setattr(stack, action, lambda s, a=action: done.append(f"{a} {s}"))
    monkeypatch.setattr(stack, "restarts", lambda s: done.count(f"start {s}"))
    monkeypatch.setattr(stack, "healthy", lambda s: True)
    monkeypatch.setattr(stack, "wait_until", lambda cond, **kw: cond())
    return done


def test_producer_crash_kills_then_starts_it(ctx: Context, broken: list[str], clock: Clock) -> None:
    drills.producer_crash(ctx)
    assert broken == ["kill producer", "start producer"]
    assert ctx.restored_at == 15  # timed from the restart
    assert "no saved position" in ctx.steps[0].text


def test_job_crash_kills_the_job_mid_batch_each_time(
    ctx: Context, broken: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    busy = iter([True, False])
    monkeypatch.setattr(stack, "spark_busy", lambda: next(busy))
    drills.job_crash(Context(**{**ctx.__dict__, "kills": 2, "steps": []}))
    assert broken == ["kill spark", "start spark", "kill spark", "start spark"]


def test_job_crash_says_when_no_batch_was_running(
    ctx: Context, broken: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(stack, "spark_busy", lambda: False)
    one = Context(**{**ctx.__dict__, "kills": 1, "steps": []})
    drills.job_crash(one)
    assert "between batches" in one.steps[0].text


def test_outages_stop_wait_and_start(ctx: Context, broken: list[str], clock: Clock) -> None:
    drills.storage_outage(ctx)
    assert broken == ["stop seaweedfs", "start seaweedfs"]
    assert ctx.restored_at == 120
    broken.clear()
    drills.broker_outage(Context(**{**ctx.__dict__, "seconds": 30, "steps": []}))
    assert broken == ["stop redpanda", "start redpanda"]


def test_poison_goes_straight_to_the_topic(
    ctx: Context, broken: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(topic, "send_poison", lambda servers, name: [(0, 41), (2, 7)])
    drills.poison_message(ctx)
    assert broken == []
    assert "0/41, 2/7" in ctx.steps[0].text


def test_no_drill_on_a_stack_that_is_down_or_behind(
    ctx: Context, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(stack, "running", lambda s: s != "spark")
    with pytest.raises(NotReadyError, match="not running: spark"):
        preflight(ctx)

    monkeypatch.setattr(stack, "running", lambda s: True)
    newest = [T - timedelta(hours=2)]

    class FakeLake:
        def __init__(self, *args: Any) -> None:
            pass

        def state(self) -> LakeState:
            return LakeState(newest[0], T)

    monkeypatch.setattr(drills, "Lake", FakeLake)
    monkeypatch.setattr(drills, "rest_catalog", lambda settings: None)
    with pytest.raises(NotReadyError, match="isn't live yet"):
        preflight(ctx, wait_minutes=1)
    newest[0] = T - timedelta(minutes=2)
    preflight(ctx)  # live: no complaint


def test_a_drill_waits_for_a_job_still_catching_up(
    ctx: Context, clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(stack, "running", lambda s: True)

    class CatchingUp:
        def __init__(self, *args: Any) -> None:
            pass

        def state(self) -> LakeState:
            # Live from the third look on (the clock moves 15 s per look).
            behind = timedelta(hours=1) if clock.seconds < 30 else timedelta(minutes=1)
            return LakeState(ctx.now() - behind, T)

    monkeypatch.setattr(drills, "Lake", CatchingUp)
    monkeypatch.setattr(drills, "rest_catalog", lambda settings: None)
    preflight(ctx)
    assert clock.seconds == 30


def lake(ids: list[str]) -> pa.Table:
    return pa.table(
        {
            "event_id": pa.array(ids, pa.string()),
            "wiki": pa.array(["enwiki"] * len(ids), pa.string()),
            "event_time": pa.array([T] * len(ids), pa.timestamp("us")),
        }
    )


class FakeView:
    """The lake catching up: the second event lands on the third look."""

    def __init__(self) -> None:
        self.looks = 0

    def changes(self, ranges: Any) -> pa.Table:
        self.looks += 1
        return lake(["a", "b"] if self.looks >= 3 else ["a"])

    def minutes(self, ranges: Any) -> pa.Table:
        events = 2 if self.looks >= 3 else 1
        return pa.table(
            {
                "wiki": pa.array(["enwiki"]),
                "window_start": pa.array([T.replace(second=0)], pa.timestamp("us")),
                "events": pa.array([events], pa.int64()),
            }
        )

    def rejected(self) -> set[tuple[int, int]]:
        return set()


@pytest.fixture
def window(monkeypatch: pytest.MonkeyPatch, broken: list[str]) -> None:
    monkeypatch.setattr(drills, "preflight", lambda ctx: None)
    monkeypatch.setattr(topic, "consumer", lambda servers: object())
    ends = iter([{0: 10, 1: 5}, {0: 12, 1: 6}])
    monkeypatch.setattr(topic, "end_offsets", lambda client, name: next(ends))
    monkeypatch.setattr(
        topic,
        "read_window",
        lambda client, name, start, end: [Sent(0, 10, "a", T), Sent(0, 11, "a", T),
                                           Sent(1, 5, "b", T)],
    )  # fmt: skip


def test_the_runner_waits_for_the_lake_then_reports(
    ctx: Context, clock: Clock, window: None, tmp_path: Path
) -> None:
    view = FakeView()
    report = run("producer-crash", ctx, view=view)  # type: ignore[arg-type]
    assert report.audit.ok and view.looks == 3
    assert (report.audit.messages, report.audit.events, report.audit.resent) == (3, 2, 1)
    # Restarted at 15 s; looked at 75, 90 and 105 s: passed 90 s after the restart.
    assert report.recovered_seconds == 90
    assert report.partitions == 2

    text = render(report)
    assert "result: passed" in text
    assert "| Sent more than once (at-least-once at work) | 1 |" in text
    assert (
        "| Every event in the lake, at the latest | 90 s after the broken part was back |" in text
    )
    assert "kill -9 the producer" in text
    path = save(report, tmp_path)
    assert path.name == "2026-10-22-1800-producer-crash.md"
    assert path.read_text() == text


def test_a_lake_that_never_catches_up_fails_the_drill(
    ctx: Context, window: None, tmp_path: Path
) -> None:
    class Stuck(FakeView):
        def changes(self, ranges: Any) -> pa.Table:
            return lake(["a"])

        def minutes(self, ranges: Any) -> pa.Table:
            self.looks = 0
            return super().minutes(ranges)

    report = run("producer-crash", ctx, timeout_minutes=1, view=Stuck())  # type: ignore[arg-type]
    assert not report.audit.ok and report.recovered_seconds is None
    text = render(report)
    assert "result: **FAILED**" in text and "- missing: `b`" in text
