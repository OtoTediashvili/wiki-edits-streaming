"""Server-Sent Events: lines in, events out.

EventStreams speaks SSE (the WHATWG EventSource format): `field: value` lines,
an empty line ends an event, a line starting with ":" is a comment. Parsing is
a pure function over an iterable of lines, so the same code reads the live
HTTP stream and a recorded file, and tests need no network.

Only what EventStreams uses is implemented: event, data, id, retry, comments.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SseEvent:
    """One dispatched event.

    `id` is the resume token: EventStreams sends a JSON list with a position
    per source topic (one per datacenter). It is kept as the exact string
    received, because it is handed back verbatim as Last-Event-ID.
    """

    event: str
    data: str
    id: str | None
    retry_ms: int | None = None


def parse(lines: Iterable[str]) -> Iterator[SseEvent]:
    """Yield each complete event. An event cut off at the end, with no blank
    line after it, is incomplete and is not dispatched (as the spec says)."""
    event_type = ""
    data: list[str] = []
    event_id: str | None = None
    retry_ms: int | None = None
    seen_field = False

    for raw in lines:
        line = raw.rstrip("\r\n")
        if not line:
            if seen_field and data:
                yield SseEvent(event_type or "message", "\n".join(data), event_id, retry_ms)
            event_type, data, event_id, retry_ms, seen_field = "", [], None, None, False
            continue
        if line.startswith(":"):
            continue  # comment, e.g. the ":ok" EventStreams sends on connect

        field, sep, value = line.partition(":")
        if sep and value.startswith(" "):
            value = value[1:]
        seen_field = True
        if field == "event":
            event_type = value
        elif field == "data":
            data.append(value)
        elif field == "id":
            if "\0" not in value:
                event_id = value
        elif field == "retry" and value.isdigit():
            retry_ms = int(value)
        # Unknown fields are ignored, per the spec.
