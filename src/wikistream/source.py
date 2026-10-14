"""The live connection to Wikimedia EventStreams, reconnecting and resuming.

EventStreams closes connections from time to time, and networks drop them. On
every reconnect the client sends Last-Event-ID: the token of the newest event
it received, so the stream continues from there instead of from "now", and
nothing in between is lost.

Errors are split the way the HTTP status says: 429 and 5xx and dropped
connections are retried with exponential backoff; any other 4xx (a malformed
request, a refused User-Agent) cannot fix itself and stops the producer.
"""

from __future__ import annotations

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
            headers["Last-Event-ID"] = last_event_id
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
