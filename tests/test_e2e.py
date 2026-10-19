"""The whole pipeline, against the real stack: `make up`, then `make test-e2e`.

The recording is produced into Redpanda twice (a producer that crashed and
resent everything) and the Spark image runs the job over it, in small
batches; then an event three days older arrives and the job runs again.
PyIceberg reads the tables back through the catalog after each run, and the
dashboard is rendered over them and its numbers checked against the rows.
Last, table maintenance runs in the Spark image and everything is checked
again: the same data in fewer files, on less disk. Everything uses a fresh
topic, namespace and checkpoint, so it never touches the live tables and can
run while they stream.
"""

from __future__ import annotations

import json
import subprocess
import uuid
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest
from confluent_kafka.admin import AdminClient
from pyiceberg.catalog import Catalog
from streamlit.testing.v1 import AppTest

from wikistream.__main__ import build_pipeline
from wikistream.config import Settings
from wikistream.dashboard.lake import Lake, naive_utc
from wikistream.dashboard.queries import compact, percent, signed_bytes
from wikistream.source import replay
from wikistream.sse import SseEvent
from wikistream.streaming.catalog import rest_catalog
from wikistream.streaming.peek import footprint
from wikistream.streaming.settings import CHANGES, EDITS_PER_MINUTE, LakeSettings

pytestmark = pytest.mark.e2e

FIXTURE = Path(__file__).parent / "fixtures" / "recentchange_sample.sse"
REPO = Path(__file__).parent.parent


@dataclass
class Run:
    topic: str
    namespace: str
    checkpoint_dir: str
    producer: Settings
    lake: LakeSettings


@pytest.fixture
def run(tmp_path: Path) -> Iterator[Run]:
    run_id = uuid.uuid4().hex[:8]
    topic = f"e2e.{run_id}.recentchange"
    producer = Settings(
        _env_file=None,  # type: ignore[call-arg]
        topic=topic,
        dlq_topic=f"{topic}.dlq",
        state_path=tmp_path / "state",
    )
    # Reads .env like the stack does, so custom SeaweedFS credentials work.
    lake = LakeSettings(lake_namespace=f"e2e_{run_id}")
    checkpoint_dir = f"/checkpoints/e2e/{run_id}"
    yield Run(topic, lake.lake_namespace, checkpoint_dir, producer, lake)

    catalog = rest_catalog(lake)
    for name in (CHANGES, EDITS_PER_MINUTE):
        if catalog.table_exists(f"{lake.lake_namespace}.{name}"):
            catalog.purge_table(f"{lake.lake_namespace}.{name}")
    if catalog.namespace_exists(lake.lake_namespace):
        catalog.drop_namespace(lake.lake_namespace)
    admin = AdminClient({"bootstrap.servers": producer.bootstrap_servers})
    for future in admin.delete_topics([topic, producer.dlq_topic]).values():
        future.result()
    remove = ["--no-deps", "--entrypoint", "rm", "spark", "-rf", checkpoint_dir]
    subprocess.run(["docker", "compose", "run", "--rm", "-T", *remove], cwd=REPO, check=False)


def days_earlier(last: SseEvent, days: int) -> SseEvent:
    """A copy of an event, made `days` older, with its own id: history that
    arrives after newer events were already processed."""
    event = json.loads(last.data)
    event["timestamp"] -= days * 86_400
    when = datetime.fromisoformat(event["meta"]["dt"]) - timedelta(days=days)
    event["meta"]["dt"] = when.astimezone(UTC).isoformat().replace("+00:00", "Z")
    event["meta"]["id"] = f"e2e-{uuid.uuid4()}"
    return SseEvent("message", json.dumps(event), None)


def run_spark(run: Run, command: str, **extra_env: str) -> None:
    """A command of the Spark image, against this test's topic and tables.
    check=True: `maintain` exits non-zero if any procedure failed."""
    env = {
        "WIKISTREAM_TOPIC": run.topic,
        "WIKISTREAM_LAKE_NAMESPACE": run.namespace,
        # Its own checkpoint, kept between this test's runs of the job.
        "WIKISTREAM_CHECKPOINT_DIR": run.checkpoint_dir,
        **extra_env,
    }
    flags = [arg for key, value in env.items() for arg in ("-e", f"{key}={value}")]
    subprocess.run(
        ["docker", "compose", "run", "--rm", "-T", *flags, "spark", command],
        cwd=REPO,
        check=True,
        timeout=900,
    )


def test_replays_and_late_history_land_once_and_minutes_add_up(run: Run) -> None:
    recorded = list(replay(FIXTURE))
    recorded_ids = {json.loads(e.data)["meta"]["id"] for e in recorded if e.event == "message"}

    # A producer that crashed and resent everything: 190 messages, 95 events.
    for _ in range(2):
        assert build_pipeline(run.producer, None).run(iter(recorded)).produced == 95
    # Small batches, like a day of live traffic: one small file each.
    run_spark(run, "once", WIKISTREAM_MAX_OFFSETS_PER_TRIGGER="10")
    check_lake(rest_catalog(run.lake), run.namespace, recorded_ids)

    # Then an event three days older than anything processed so far. A
    # watermark-based job drops it as late; this one must write it.
    late = days_earlier(recorded[-1], days=3)
    assert build_pipeline(run.producer, None).run(iter([late])).produced == 1
    run_spark(run, "once")
    late_id = json.loads(late.data)["meta"]["id"]
    check_lake(rest_catalog(run.lake), run.namespace, recorded_ids | {late_id})
    check_dashboard(rest_catalog(run.lake), run.namespace)

    # Maintenance, with every snapshot but the last old enough to expire.
    catalog = rest_catalog(run.lake)
    tables = {name: f"{run.namespace}.{name}" for name in (CHANGES, EDITS_PER_MINUTE)}
    before = {name: footprint(catalog.load_table(t)) for name, t in tables.items()}
    assert before[CHANGES].data_files >= 6  # the recorded day in small files, and the late one
    run_spark(
        run,
        "maintain",
        WIKISTREAM_SNAPSHOT_RETENTION_HOURS="0",
        WIKISTREAM_SNAPSHOT_RETAIN_LAST="1",
    )
    catalog = rest_catalog(run.lake)
    after = {name: footprint(catalog.load_table(t)) for name, t in tables.items()}
    # The recorded day compacted to one file; the late event's day, a single
    # file already, left as it was.
    assert after[CHANGES].data_files == 2
    for name in tables:
        assert after[name].snapshots == 1
        assert after[name].rows == before[name].rows
        assert after[name].disk_objects < before[name].disk_objects
        assert after[name].disk_bytes < before[name].disk_bytes
    # And nothing a reader sees has changed.
    check_lake(catalog, run.namespace, recorded_ids | {late_id})
    check_dashboard(catalog, run.namespace)


def check_lake(catalog: Catalog, namespace: str, expected_ids: set[str]) -> None:
    changes_table = catalog.load_table(f"{namespace}.{CHANGES}")
    changes = changes_table.scan().to_arrow()

    # Every event exactly once.
    assert sorted(changes["event_id"].to_pylist()) == sorted(expected_ids)
    assert [f.transform.__class__.__name__ for f in changes_table.spec().fields] == ["DayTransform"]

    # A count for every minute that has events, agreeing with the rows.
    minutes = catalog.load_table(f"{namespace}.{EDITS_PER_MINUTE}").scan().to_arrow()
    written = Counter(
        {
            (start, wiki): events
            for start, wiki, events in zip(
                minutes["window_start"].to_pylist(),
                minutes["wiki"].to_pylist(),
                minutes["events"].to_pylist(),
                strict=True,
            )
        }
    )
    expected = Counter(
        (when.replace(second=0, microsecond=0), wiki)
        for when, wiki in zip(
            changes["event_time"].to_pylist(), changes["wiki"].to_pylist(), strict=True
        )
    )
    assert written == expected


def dashboard_page(namespace: str, clock: str) -> None:
    """The real page over the test's tables, run by AppTest."""
    from datetime import datetime

    from wikistream.dashboard.app import render
    from wikistream.dashboard.lake import Lake
    from wikistream.streaming.catalog import rest_catalog
    from wikistream.streaming.settings import LakeSettings

    render(
        lambda: Lake(rest_catalog(LakeSettings()), namespace),
        refresh_seconds=None,
        now=lambda: datetime.fromisoformat(clock),
    )


def check_dashboard(catalog: Catalog, namespace: str) -> None:
    """The dashboard's numbers over what Spark wrote, against the same
    numbers counted straight from the rows."""
    rows = naive_utc(catalog.load_table(f"{namespace}.{CHANGES}").scan().to_arrow())
    minutes = [t.replace(second=0) for t in rows["event_time"].to_pylist()]
    newest = max(minutes)
    # From the file statistics Spark wrote, not from reading the rows.
    assert Lake(catalog, namespace).state().newest_minute == newest

    # The page holds the newest minute back as still filling, and the late
    # event is three days older than a 24-hour window reaches.
    start = newest - timedelta(hours=24)
    shown = rows.filter(pa.array([start <= m < newest for m in minutes]))
    assert 0 < shown.num_rows < rows.num_rows

    app = AppTest.from_function(
        dashboard_page,
        kwargs={"namespace": namespace, "clock": (newest + timedelta(minutes=2)).isoformat()},
        default_timeout=60,
    )
    app.query_params["window"] = "24h"
    app.run()
    assert not app.exception, app.exception
    assert "Live" in app.markdown[0].value
    tiles = {m.label: m.value for m in app.metric}

    def count(column: str, value: object) -> int:
        return sum(1 for v in shown[column].to_pylist() if v == value)

    covered = len({m for m in minutes if start <= m < newest})
    deltas = [d for d in shown["length_delta"].to_pylist() if d is not None]
    assert tiles == {
        "Changes per minute": compact(shown.num_rows / covered),
        "Edits": compact(count("type", "edit")),
        "New pages": compact(count("type", "new")),
        "Bot share": percent(count("bot", True) / shown.num_rows),
        "Editors, last hour": compact(len({u for u in shown["user"].to_pylist() if u})),
        "Content added": signed_bytes(sum(deltas)),
    }
