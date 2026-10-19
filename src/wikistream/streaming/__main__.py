"""Command line for the stream job.

python -m wikistream.streaming run         stream until stopped
python -m wikistream.streaming once        process what is in the topic, then exit
python -m wikistream.streaming maintain    compact and expire snapshots once, then exit
python -m wikistream.streaming fetch-jars  download the connector jars (image build)
"""

from __future__ import annotations

import argparse
import signal
import sys
from datetime import timedelta
from types import FrameType

from wikistream.logging_config import configure_logging, get_logger
from wikistream.streaming import maintenance
from wikistream.streaming.job import ProgressLog, await_queries, start_queries
from wikistream.streaming.session import PACKAGES, spark_session
from wikistream.streaming.settings import LakeSettings

log = get_logger(__name__)


def _stop_on_sigterm() -> None:
    """`docker stop` sends SIGTERM; stop the queries cleanly, as Ctrl-C would."""

    def handler(_signum: int, _frame: FrameType | None) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, handler)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m wikistream.streaming")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("run", help="stream from Redpanda into Iceberg until stopped")
    commands.add_parser("once", help="process everything in the topic now, then exit")
    commands.add_parser("maintain", help="compact and expire snapshots once, then exit")
    commands.add_parser("fetch-jars", help="download the connector jars into the Ivy cache")
    args = parser.parse_args(argv)

    settings = LakeSettings()
    configure_logging(settings.log_level, settings.log_json)

    if args.command == "fetch-jars":
        spark_session(settings).stop()
        log.info("jars.ready", packages=list(PACKAGES), cache=settings.spark_jars_ivy)
        return 0

    _stop_on_sigterm()
    spark = spark_session(settings)
    if args.command == "maintain":
        try:
            outcomes = maintenance.run(spark, settings, maintenance.utc_now())
        finally:
            spark.stop()
        return 0 if all(outcome.ok for outcome in outcomes) else 1

    spark.streams.addListener(ProgressLog())
    once = args.command == "once"
    log.info(
        "stream.start", mode=args.command, topic=settings.topic, namespace=settings.lake_namespace
    )
    schedule = None
    if not once and settings.maintenance_every_minutes:
        schedule = maintenance.Schedule(
            lambda: maintenance.run(spark, settings, maintenance.utc_now()),
            every=timedelta(minutes=settings.maintenance_every_minutes),
        )
    try:
        await_queries(
            start_queries(spark, settings, once=once),
            on_tick=schedule.tick if schedule else None,
        )
    except KeyboardInterrupt:
        log.info("stream.stopped")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
