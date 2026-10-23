"""The drills, and the runner that audits each one and writes the report."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from wikistream.dashboard.lake import Lake
from wikistream.drills import stack, topic
from wikistream.drills.audit import Audit, audit, ranges_for
from wikistream.drills.lakeview import LakeView
from wikistream.streaming.catalog import rest_catalog
from wikistream.streaming.settings import LakeSettings

SERVICES = ("redpanda", "seaweedfs", "producer", "spark")
REPORTS = Path("docs/drills")
# The newest counted minute must be this close to the clock before a drill:
# a job still catching up would make the timings meaningless.
LIVE = timedelta(minutes=5)


def utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class NotReadyError(RuntimeError):
    """The stack isn't in a state to drill: nothing was broken."""


@dataclass
class Step:
    at: datetime
    text: str


@dataclass
class Context:
    """What a drill gets: settings, a log of what it did, and the moment the
    broken part was back (from which recovery is timed)."""

    lake: LakeSettings
    bootstrap_servers: str
    topic: str
    seconds: int = 0  # outage length, for the drills that have one
    kills: int = 3
    steps: list[Step] = field(default_factory=list)
    restored_at: float | None = None
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic
    now: Callable[[], datetime] = utc_now

    def note(self, text: str) -> None:
        step = Step(self.now(), text)
        self.steps.append(step)
        print(f"{step.at:%H:%M:%S}  {text}", flush=True)

    def restored(self) -> None:
        self.restored_at = self.clock()


# --- the drills --------------------------------------------------------------


def producer_crash(ctx: Context) -> None:
    """The producer dies between two checkpoints and resends what it sent
    since the last one. Expect duplicates in the topic, none in the lake."""
    ctx.note("kill -9 the producer: no flush, no saved position")
    stack.kill("producer")
    ctx.sleep(ctx.seconds or 15)
    stack.start("producer")
    ctx.restored()
    ctx.note("producer started again; it resumes from its last saved position")
    ctx.sleep(60)


def job_crash(ctx: Context) -> None:
    """The Spark job dies in the middle of a batch, several times. Spark
    replays the unfinished batch from its checkpoint; the writes are
    idempotent, so the replay changes nothing it already wrote."""
    for n in range(1, ctx.kills + 1):
        busy = stack.wait_until(stack.spark_busy, timeout_seconds=150)
        moment = "in the middle of a batch" if busy else "between batches (none seen running)"
        ctx.note(f"kill -9 the Spark job {moment} ({n} of {ctx.kills})")
        stack.kill("spark")
        ctx.sleep(5)
        stack.start("spark")
        ctx.restored()
        ctx.note("Spark job started again; it replays the unfinished batch")
        ctx.sleep(90)


def storage_outage(ctx: Context) -> None:
    """SeaweedFS (catalog and S3) goes away. The job's batches fail, it exits,
    Docker restarts it until storage is back, and it catches up."""
    seconds = ctx.seconds or 120
    before = stack.restarts("spark")
    ctx.note(f"stop SeaweedFS for {seconds} s: no catalog, no S3")
    stack.stop("seaweedfs")
    ctx.sleep(seconds)
    stack.start("seaweedfs")
    stack.wait_until(lambda: stack.healthy("seaweedfs"), timeout_seconds=120, every=2)
    ctx.restored()
    restarts = stack.restarts("spark") - before
    ctx.note(f"SeaweedFS is back; Docker restarted the Spark job {restarts} times meanwhile")
    ctx.sleep(90)


def broker_outage(ctx: Context) -> None:
    """Redpanda goes away. The producer can't confirm delivery, exits without
    saving its position and is restarted until the broker is back; the job
    can't read and does the same. Both resume where they were."""
    seconds = ctx.seconds or 60
    before = {s: stack.restarts(s) for s in ("producer", "spark")}
    ctx.note(f"stop Redpanda for {seconds} s")
    stack.stop("redpanda")
    ctx.sleep(seconds)
    stack.start("redpanda")
    stack.wait_until(lambda: stack.healthy("redpanda"), timeout_seconds=120, every=2)
    ctx.restored()
    counts = ", ".join(f"{s} {stack.restarts(s) - before[s]}" for s in before)
    ctx.note(f"Redpanda is back; restarts by Docker meanwhile: {counts}")
    ctx.sleep(120)


def poison_message(ctx: Context) -> None:
    """Messages that aren't WikiChange records land in the topic. The job
    sets them aside in the rejected table and carries on."""
    offsets = topic.send_poison(ctx.bootstrap_servers, ctx.topic)
    where = ", ".join(f"{p}/{o}" for p, o in offsets)
    ctx.restored()
    ctx.note(f"wrote {len(offsets)} malformed messages to the topic (partition/offset {where})")
    ctx.sleep(90)


DRILLS: dict[str, Callable[[Context], None]] = {
    "producer-crash": producer_crash,
    "job-crash": job_crash,
    "storage-outage": storage_outage,
    "broker-outage": broker_outage,
    "poison-message": poison_message,
}


# --- the runner -----------------------------------------------------------------


@dataclass
class Report:
    name: str
    started: datetime
    steps: list[Step]
    audit: Audit
    recovered_seconds: float | None  # None: the audit never passed
    partitions: int


def preflight(ctx: Context, wait_minutes: float = 10) -> None:
    """Every service up, and the job live: waits for a job that is still
    catching up (after the previous drill, say), then gives up."""
    down = [s for s in SERVICES if not stack.running(s)]
    if down:
        raise NotReadyError(f"not running: {', '.join(down)}. Start the stack first.")
    lake = Lake(rest_catalog(ctx.lake), ctx.lake.lake_namespace)
    deadline = ctx.clock() + wait_minutes * 60
    while True:
        newest = lake.state().newest_minute
        if newest is not None and ctx.now() - newest <= LIVE:
            return
        if ctx.clock() >= deadline:
            raise NotReadyError(
                f"the job isn't live yet (newest counted minute: {newest or 'none'}). "
                "Let it catch up first: make peek."
            )
        ctx.sleep(15)


def run(
    name: str,
    ctx: Context,
    timeout_minutes: float = 20,
    view: LakeView | None = None,
) -> Report:
    preflight(ctx)
    client = topic.consumer(ctx.bootstrap_servers)
    started = ctx.now()
    start = topic.end_offsets(client, ctx.topic)
    DRILLS[name](ctx)
    end = topic.end_offsets(client, ctx.topic)
    ctx.note("drill over: reading what went through Redpanda meanwhile")
    sent = topic.read_window(client, ctx.topic, start, end)
    ranges = ranges_for(sent)
    view = view or LakeView(rest_catalog(ctx.lake), ctx.lake.lake_namespace)
    ctx.note(f"{len(sent):,} messages; checking the lake until it has them all")

    deadline = ctx.clock() + timeout_minutes * 60
    while True:
        verdict = audit(sent, view.changes(ranges), view.minutes(ranges), view.rejected())
        if verdict.ok or ctx.clock() >= deadline:
            break
        ctx.sleep(15)
    recovered = None
    if verdict.ok and ctx.restored_at is not None:
        recovered = ctx.clock() - ctx.restored_at
    ctx.note("audit passed" if verdict.ok else "audit FAILED")
    return Report(name, started, ctx.steps, verdict, recovered, partitions=len(end))


def render(report: Report) -> str:
    a = report.audit
    result = "passed" if a.ok else "**FAILED**"
    lines = [
        f"# Drill: {report.name}",
        "",
        f"{report.started:%Y-%m-%d %H:%M} UTC · result: {result}",
        "",
        " ".join((DRILLS[report.name].__doc__ or "").split()),
        "",
        "## What happened",
        "",
        "| Time (UTC) | |",
        "|---|---|",
        *[f"| {s.at:%H:%M:%S} | {s.text} |" for s in report.steps],
        "",
        "## The audit",
        "",
        "| | |",
        "|---|---|",
        f"| Messages in Redpanda during the drill | {a.messages:,} "
        f"across {report.partitions} partitions |",
        f"| Distinct events | {a.events:,} |",
        f"| Sent more than once (at-least-once at work) | {a.resent:,} |",
        f"| Not WikiChange records | {a.poison:,} ({len(a.not_set_aside)} not set aside) |",
        f"| Missing from the lake | {len(a.missing):,} |",
        f"| In the lake more than once | {len(a.doubled):,} |",
        f"| Minutes counted right (wiki by wiki) | {a.minutes - len(a.miscounted):,} "
        f"of {a.minutes:,} |",
    ]
    if report.recovered_seconds is not None:
        lines.append(
            f"| Every event in the lake, at the latest | {report.recovered_seconds:.0f} s "
            "after the broken part was back |"
        )
    problems = [
        *[f"- missing: `{i}`" for i in a.missing[:20]],
        *[f"- more than once: `{i}`" for i in a.doubled[:20]],
        *[f"- not set aside: partition {p}, offset {o}" for p, o in a.not_set_aside[:20]],
        *[
            f"- {m.wiki} {m.minute:%Y-%m-%d %H:%M}: counted {m.counted}, rows {m.rows}"
            for m in a.miscounted[:20]
        ],
    ]
    if problems:
        lines += ["", "## Problems (first 20 of each)", "", *problems]
    return "\n".join(lines) + "\n"


def save(report: Report, folder: Path = REPORTS) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{report.started:%Y-%m-%d-%H%M}-{report.name}.md"
    path.write_text(render(report))
    return path
