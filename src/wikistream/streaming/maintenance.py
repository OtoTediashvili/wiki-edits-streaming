"""Table maintenance: compact the days that ended, expire old snapshots.

Every batch commits to both tables, once a minute, and each commit leaves
something behind:

- `changes` gets one small data file per batch: about 1,440 a day. Readers
  open every one of them, and each costs a request to S3.
- `edits_per_minute` is copy-on-write. Each MERGE rewrites the data file
  holding today's minutes, so the table itself stays small, but every
  superseded version stays on disk for as long as an old snapshot points
  at it.
- Both get a snapshot per commit, and every snapshot is listed in the
  table's metadata file, which every commit and every reader loads.

So, with Iceberg's own Spark procedures:

1. `rewrite_data_files` on `changes`, only for days that ended (UTC). The
   job only appends to `changes`, so a rewrite of yesterday and the job's
   append of today commute: neither has to retry. Today is left alone; its
   files are what the job and the dashboard read. The minute table is not
   compacted: copy-on-write already keeps it to about one file a day.
2. `expire_snapshots` on both tables: snapshots older than the retention
   (6 hours by default), keeping at least the last 10. This deletes the
   data and metadata files only they still referenced.

Runs inside the stream job once an hour (between its batches, in the same
Spark session: no second JVM), and on demand with `make maintain`.
Orphan files (written by a batch that crashed before committing) are not
removed: listing the warehouse needs a file system Spark here doesn't
have, and with idempotent batches they are rare. `make peek` shows them.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from pyspark.sql import SparkSession

from wikistream.logging_config import get_logger
from wikistream.streaming.settings import CATALOG, CHANGES, EDITS_PER_MINUTE, LakeSettings
from wikistream.streaming.tables import java_cause, sql_string

log = get_logger(__name__)

# Compacted files: big enough that a day is one or two files, small enough
# for a 1 GB driver to rewrite. Only days with at least 5 small files are
# rewritten, so a late event added to a compacted day doesn't trigger a
# rewrite of the whole day. One group at a time, for the laptop.
COMPACTION_OPTIONS = {
    "target-file-size-bytes": str(128 * 1024 * 1024),
    "min-input-files": "5",
    "partial-progress.enabled": "true",
    "max-concurrent-file-group-rewrites": "1",
}

# The first run after a start waits a little: a job that has just started
# is usually catching up, and a restart loop shouldn't compact every time.
FIRST_RUN_AFTER = timedelta(minutes=10)

_TS = "%Y-%m-%d %H:%M:%S"


def utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


@dataclass(frozen=True)
class Task:
    action: str  # "compact" or "expire"
    table: str  # namespace.table, as the procedures take it
    sql: str


def _map(options: dict[str, str]) -> str:
    return "map(" + ", ".join(f"{sql_string(k)}, {sql_string(v)}" for k, v in options.items()) + ")"


def compact_sql(table: str, column: str, before: datetime) -> str:
    where = f"{column} < TIMESTAMP '{before:{_TS}}'"
    return (
        f"CALL {CATALOG}.system.rewrite_data_files("
        f"table => {sql_string(table)}, strategy => 'binpack', where => {sql_string(where)}, "
        f"options => {_map(COMPACTION_OPTIONS)})"
    )


def expire_sql(table: str, older_than: datetime, retain_last: int) -> str:
    return (
        f"CALL {CATALOG}.system.expire_snapshots("
        f"table => {sql_string(table)}, older_than => TIMESTAMP '{older_than:{_TS}}', "
        f"retain_last => {retain_last}, stream_results => true)"
    )


def plan(settings: LakeSettings, now: datetime) -> list[Task]:
    """What one run does, at `now` (naive UTC). Compaction first: the files
    it replaces are then expired by a later run, once old enough."""
    namespace = settings.lake_namespace
    changes, minutes = f"{namespace}.{CHANGES}", f"{namespace}.{EDITS_PER_MINUTE}"
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    older_than = now - timedelta(hours=settings.snapshot_retention_hours)
    return [
        Task("compact", changes, compact_sql(changes, "event_time", today)),
        Task("expire", changes, expire_sql(changes, older_than, settings.snapshot_retain_last)),
        Task("expire", minutes, expire_sql(minutes, older_than, settings.snapshot_retain_last)),
    ]


@dataclass(frozen=True)
class Outcome:
    task: Task
    ok: bool
    seconds: float
    result: dict[str, Any] = field(default_factory=dict)


def run(spark: SparkSession, settings: LakeSettings, now: datetime) -> list[Outcome]:
    """Every task, in order. A failure is logged and the rest still run: a
    compaction that lost a commit race must not stop the expiry."""
    outcomes = []
    for task in plan(settings, now):
        started = time.monotonic()
        try:
            row = spark.sql(task.sql).first()
        except Exception as exc:
            seconds = round(time.monotonic() - started, 1)
            log.error(
                "maintenance.failed",
                action=task.action,
                table=task.table,
                seconds=seconds,
                error=java_cause(exc),
            )
            outcomes.append(Outcome(task, ok=False, seconds=seconds))
            continue
        seconds = round(time.monotonic() - started, 1)
        result = row.asDict() if row is not None else {}
        log.info(f"maintenance.{task.action}", table=task.table, seconds=seconds, **result)
        outcomes.append(Outcome(task, ok=True, seconds=seconds, result=result))
    return outcomes


class Schedule:
    """Runs maintenance from the stream job's supervision loop: `tick` is
    called every poll and does the work when it's due. Never raises, so a
    maintenance problem can't take the streaming query down with it."""

    def __init__(
        self,
        work: Callable[[], object],
        every: timedelta,
        first_after: timedelta = FIRST_RUN_AFTER,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._work = work
        self._every = every.total_seconds()
        self._clock = clock
        self._due = clock() + first_after.total_seconds()

    def tick(self) -> None:
        now = self._clock()
        if now < self._due:
            return
        self._due = now + self._every
        try:
            self._work()
        except Exception as exc:
            log.error("maintenance.crashed", error=java_cause(exc))
