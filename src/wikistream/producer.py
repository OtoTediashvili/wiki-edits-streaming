"""From stream events to Redpanda: validate, serialize, produce, checkpoint.

Delivery guarantee, end to end: at-least-once.

* The Kafka producer is idempotent with acks=all, so its own retries never
  duplicate or reorder messages within a partition.
* Every `checkpoint_seconds` the producer flushes, which waits until the broker
  has acknowledged everything sent, and only then saves the resume position.
  A crash between checkpoints resumes from the last saved position and sends
  those few seconds again. The stream job removes the duplicates by event_id.
* A delivery failure stops the producer without saving: nothing is skipped.

Bad events are separated from bad infrastructure. An event that can't be
mapped or doesn't fit the schema goes to the dead-letter topic with the
reason, and the stream continues. A broker or registry failure raises.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, Protocol

import fastavro
import fastavro.validation
from confluent_kafka import KafkaError, KafkaException

# NewTopic lives in the typed C module; confluent_kafka.admin re-exports it
# without declaring it, which strict mypy rejects.
from confluent_kafka.cimpl import NewTopic

from wikistream.config import Settings
from wikistream.logging_config import get_logger
from wikistream.model import (
    InvalidEventError,
    WikiChange,
    decode,
    is_canary,
    message_key,
    schema_str,
    to_record,
)
from wikistream.sse import SseEvent
from wikistream.state import ResumePosition

log = get_logger(__name__)

DeliveryCallback = Callable[[Any, Any], None]


class ProducerLike(Protocol):
    """The slice of confluent_kafka.Producer used here, so tests can fake it."""

    def produce(
        self,
        topic: str,
        value: str | bytes | None = None,
        key: str | bytes | None = None,
        partition: int = -1,
        callback: DeliveryCallback | None = None,
        on_delivery: DeliveryCallback | None = None,
        timestamp: int = 0,
        headers: Any = None,
    ) -> None: ...

    def poll(self, timeout: float = -1) -> int: ...

    def flush(self, timeout: float = -1) -> int: ...


class DeliveryError(RuntimeError):
    """The broker did not confirm delivery. The resume position is not advanced."""


def producer_config(settings: Settings) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": "wikistream-producer",
        # Retries can't create duplicates or reorder a partition.
        "enable.idempotence": True,
        "acks": "all",
        # ~30 events/s of ~1.4 KB JSON: batching a few milliseconds and zstd
        # cut what the broker stores several times over.
        "compression.type": "zstd",
        "linger.ms": 50,
    }


def topic_config(settings: Settings) -> dict[str, str]:
    return {
        "cleanup.policy": "delete",
        "retention.ms": str(settings.retention_hours * 3600 * 1000),
        "retention.bytes": str(settings.retention_bytes),
        "compression.type": "zstd",
    }


class AdminLike(Protocol):
    def create_topics(self, new_topics: list[NewTopic], **kwargs: Any) -> dict[str, Any]: ...


def ensure_topics(admin: AdminLike, settings: Settings) -> None:
    """Create the topics with their retention if missing. Safe to run every start."""
    config = topic_config(settings)
    topics = [
        NewTopic(
            settings.topic, num_partitions=settings.partitions, replication_factor=1, config=config
        ),
        NewTopic(settings.dlq_topic, num_partitions=1, replication_factor=1, config=config),
    ]
    for name, future in admin.create_topics(topics).items():
        try:
            future.result()
            log.info("topic.created", topic=name, **config)
        except KafkaException as exc:
            error = exc.args[0]
            if isinstance(error, KafkaError) and error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                log.info("topic.exists", topic=name)
            else:
                raise


@dataclass
class Stats:
    produced: int = 0
    dead_lettered: int = 0
    canaries: int = 0
    ignored: int = 0
    newest_event_time_ms: int | None = None
    started: float = 0.0


class Pipeline:
    def __init__(
        self,
        settings: Settings,
        producer: ProducerLike,
        serialize: Callable[[WikiChange], bytes],
        position: ResumePosition | None,
        clock: Callable[[], float] = time.monotonic,
        progress_every_seconds: float = 30.0,
    ) -> None:
        self.settings = settings
        self.producer = producer
        self.serialize = serialize
        self.position = position
        self.clock = clock
        self.progress_every = progress_every_seconds
        self.schema = fastavro.parse_schema(json.loads(schema_str()))
        self.stats = Stats(started=clock())
        self._delivery_error: Any = None
        self._pending_id: str | None = None

    # -------------------------------------------------------------- delivery

    def _on_delivery(self, err: Any, _msg: Any) -> None:
        if err is not None and self._delivery_error is None:
            self._delivery_error = err

    def checkpoint(self) -> None:
        """Wait for every sent message to be acknowledged, then save the position."""
        remaining = self.producer.flush(30.0)
        if self._delivery_error is not None:
            raise DeliveryError(f"broker rejected a message: {self._delivery_error}")
        if remaining:
            raise DeliveryError(f"{remaining} messages still unconfirmed after 30 s")
        if self._pending_id and self.position is not None:
            self.position.save(self._pending_id)

    # ---------------------------------------------------------------- events

    def _dead_letter(self, sse: SseEvent, reason: str) -> None:
        self.stats.dead_lettered += 1
        log.warning("event.dead_lettered", reason=reason)
        self.producer.produce(
            self.settings.dlq_topic,
            value=sse.data.encode(),
            headers=[("error", reason.encode())],
            on_delivery=self._on_delivery,
        )

    def handle(self, sse: SseEvent) -> None:
        if sse.event != "message":
            self.stats.ignored += 1
            return
        try:
            event = decode(sse.data)
            if is_canary(event):
                self.stats.canaries += 1
                return
            record = to_record(event, sse.data)
            fastavro.validation.validate(record, self.schema, raise_errors=True)
        except (InvalidEventError, fastavro.validation.ValidationError) as exc:
            self._dead_letter(sse, str(exc))
            return
        self.producer.produce(
            self.settings.topic,
            value=self.serialize(record),
            key=message_key(record).encode(),
            on_delivery=self._on_delivery,
        )
        self.stats.produced += 1
        self.stats.newest_event_time_ms = record["event_time"]

    def run(self, events: Iterable[SseEvent]) -> Stats:
        """Produce every event; checkpoint on a timer and once more at the end,
        including when stopped by Ctrl-C or SIGTERM."""
        last_checkpoint = last_progress = self.clock()
        try:
            for sse in events:
                self.handle(sse)
                if sse.id:
                    self._pending_id = sse.id
                self.producer.poll(0)
                if self._delivery_error is not None:
                    raise DeliveryError(f"broker rejected a message: {self._delivery_error}")
                now = self.clock()
                if now - last_checkpoint >= self.settings.checkpoint_seconds:
                    self.checkpoint()
                    last_checkpoint = now
                if now - last_progress >= self.progress_every:
                    self._log_progress()
                    last_progress = now
        except DeliveryError:
            raise
        except BaseException:
            # Stopped (Ctrl-C, SIGTERM) or failed upstream: still confirm and
            # save what was delivered, then let the exception through.
            self.checkpoint()
            raise
        self.checkpoint()
        self._log_progress()
        return self.stats

    def _log_progress(self) -> None:
        lag = (
            None
            if self.stats.newest_event_time_ms is None
            else round(time.time() - self.stats.newest_event_time_ms / 1000, 1)
        )
        elapsed = max(self.clock() - self.stats.started, 1e-9)
        log.info(
            "producer.progress",
            produced=self.stats.produced,
            dead_lettered=self.stats.dead_lettered,
            canaries=self.stats.canaries,
            per_second=round(self.stats.produced / elapsed, 1),
            seconds_behind_newest_edit=lag,
        )
