"""The live connection to Wikimedia EventStreams, reconnecting and resuming.

EventStreams closes connections from time to time, and networks drop them. On
every reconnect the client sends Last-Event-ID: the token of the newest event
it received, so the stream continues from there instead of from "now", and
nothing in between is lost. (One catch in that token is handled by
`resume_id`; see there.)

Errors are split the way the HTTP status says: 429 and 5xx and dropped
connections are retried with exponential backoff; any other 4xx (a malformed
request, a refused User-Agent) cannot fix itself and stops the producer.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import httpx

from wikistream.config import Settings
from wikistream.logging_config import get_logger
from wikistream.sse import SseEvent, parse

log = get_logger(__name__)


class SourceError(RuntimeError):
    """A failure that retrying cannot fix."""


# How far before the newest delivered event a topic without a position
# resumes. Events in that margin that were already delivered come again, and
# the stream job removes the copies.
RESUME_MARGIN_MS = 60_000


def resume_id(last_event_id: str) -> str:
    """The Last-Event-ID to send, with every datacenter topic resuming.

    An EventStreams id holds one position per source topic, one topic per
    Wikimedia datacenter: `[{"topic": "eqiad...", "timestamp": ...},
    {"topic": "codfw...", "offset": -1}]`. A topic with no event yet in the
    connection is recorded as offset -1, which Kafka, and so EventStreams,
    reads as "from the end". The second datacenter carries few events, so
    its position is nearly always -1, and resuming after time away would
    start that topic at "now" while the other replays history: its events
    from the gap are skipped, and live events get mixed into the replay.

    Such topics resume from the newest timestamp among the others, less a
    margin. Ids that can't be read this way are sent unchanged.
    """
    try:
        positions = json.loads(last_event_id)
    except json.JSONDecodeError:
        return last_event_id
    if not isinstance(positions, list) or not all(isinstance(p, dict) for p in positions):
        return last_event_id
    stamps = [p["timestamp"] for p in positions if isinstance(p.get("timestamp"), int)]
    if not stamps:
        return last_event_id
    resume_at = max(stamps) - RESUME_MARGIN_MS
    fixed = [
        {"topic": p.get("topic"), "partition": p.get("partition"), "timestamp": resume_at}
        if p.get("offset") == -1 and "timestamp" not in p
        else p
        for p in positions
    ]
    if fixed == positions:
        return last_event_id
    return json.dumps(fixed, separators=(",", ":"))


class _RetryableError(Exception):
    pass


class EventStream:
    def __init__(
        self,
        settings: Settings,
        client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not settings.contact.strip():
            raise SourceError(
                "WIKISTREAM_CONTACT is empty. Wikimedia's User-Agent policy asks every "
                "client for a way to reach its operator: set it to your GitHub URL or email."
            )
        self.settings = settings
        self.client = client or httpx.Client(
            timeout=httpx.Timeout(
                connect=settings.connect_timeout_seconds,
                read=settings.read_timeout_seconds,
                write=10.0,
                pool=10.0,
            )
        )
        self.sleep = sleep

    def headers(self, last_event_id: str | None) -> dict[str, str]:
        headers = {"Accept": "text/event-stream", "User-Agent": self.settings.user_agent}
        if last_event_id:
            headers["Last-Event-ID"] = resume_id(last_event_id)
        return headers

    def events(self, last_event_id: str | None = None) -> Iterator[SseEvent]:
        """Yield events forever, reconnecting as needed. Each event's id is
        remembered and sent on the next reconnect."""
        backoff = 1.0
        while True:
            try:
                with self.client.stream(
                    "GET", self.settings.stream_url, headers=self.headers(last_event_id)
                ) as response:
                    status = response.status_code
                    if status == 429 or status >= 500:
                        raise _RetryableError(f"HTTP {status}")
                    if status >= 400:
                        raise SourceError(f"HTTP {status} from {self.settings.stream_url}")
                    log.info("source.connected", resumed=last_event_id is not None)
                    for event in parse(response.iter_lines()):
                        if event.id:
                            last_event_id = event.id
                        backoff = 1.0
                        yield event
                log.warning("source.closed_by_server")
            except (httpx.TransportError, _RetryableError) as exc:
                log.warning("source.retry", error=str(exc), backoff_seconds=backoff)
            self.sleep(backoff)
            backoff = min(backoff * 2, self.settings.max_backoff_seconds)


def replay(path: Path) -> Iterator[SseEvent]:
    """Events from a recorded capture (curl output), for tests and offline demos."""
    with path.open(encoding="utf-8") as handle:
        yield from parse(handle)
