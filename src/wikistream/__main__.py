"""Command line: `wikistream live` streams Wikimedia into Redpanda;
`wikistream replay FILE` sends a recorded capture instead (tests, demos).

    python -m wikistream live
    python -m wikistream replay tests/fixtures/recentchange_sample.sse
"""

from __future__ import annotations

import argparse
import signal
import sys
from pathlib import Path
from types import FrameType

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from wikistream.config import Settings, get_settings
from wikistream.logging_config import configure_logging, get_logger
from wikistream.model import WikiChange, schema_str
from wikistream.producer import Pipeline, ensure_topics, producer_config
from wikistream.source import EventStream, replay
from wikistream.state import ResumePosition

log = get_logger(__name__)


def _stop_on_sigterm() -> None:
    """`docker stop` sends SIGTERM. Treat it like Ctrl-C, so the pipeline
    confirms delivery and saves its position before exiting."""

    def handler(_signum: int, _frame: FrameType | None) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, handler)


def build_pipeline(settings: Settings, position: ResumePosition | None) -> Pipeline:
    ensure_topics(AdminClient({"bootstrap.servers": settings.bootstrap_servers}), settings)
    registry = SchemaRegistryClient({"url": settings.schema_registry_url})
    serializer = AvroSerializer(registry, schema_str())
    context = SerializationContext(settings.topic, MessageField.VALUE)

    def serialize(record: WikiChange) -> bytes:
        value: bytes | None = serializer(record, context)
        if value is None:
            raise ValueError("the Avro serializer returned nothing")
        return value

    return Pipeline(settings, Producer(producer_config(settings)), serialize, position)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="wikistream")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("live", help="stream Wikimedia recent changes into Redpanda")
    replay_cmd = commands.add_parser("replay", help="send a recorded SSE capture into Redpanda")
    replay_cmd.add_argument("path", type=Path)
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    _stop_on_sigterm()

    try:
        if args.command == "live":
            position = ResumePosition(settings.state_path)
            resume_from = position.load()
            log.info("producer.start", mode="live", resuming=resume_from is not None)
            pipeline = build_pipeline(settings, position)
            pipeline.run(EventStream(settings).events(resume_from))
        else:
            log.info("producer.start", mode="replay", path=str(args.path))
            # A replay never touches the live resume position.
            stats = build_pipeline(settings, None).run(replay(args.path))
            log.info("replay.done", produced=stats.produced, dead_lettered=stats.dead_lettered)
    except KeyboardInterrupt:
        log.info("producer.stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
