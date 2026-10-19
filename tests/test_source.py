"""The live connection: headers, resuming, retries, and what stops it."""

from __future__ import annotations

import json
from collections.abc import Iterator
from itertools import islice
from pathlib import Path

import httpx
import pytest
import respx

from wikistream.config import Settings
from wikistream.source import RESUME_MARGIN_MS, EventStream, SourceError, replay, resume_id
from wikistream.sse import SseEvent, parse

URL = "https://stream.test/v2/stream/recentchange"


def sse_body(*events: tuple[str, str]) -> bytes:
    """(id, data) pairs as EventStreams sends them."""
    return (
        ":ok\n\n" + "".join(f"event: message\nid: {i}\ndata: {d}\n\n" for i, d in events)
    ).encode()


class StopLoopError(Exception):
    """Ends the otherwise endless reconnect loop inside a test."""


class Sleeps:
    def __init__(self, allow: int = 10) -> None:
        self.calls: list[float] = []
        self.allow = allow

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        if len(self.calls) > self.allow:
            raise StopLoopError


def take(stream: Iterator[SseEvent], n: int) -> list[SseEvent]:
    return list(islice(stream, n))


@respx.mock
def test_identifies_itself_and_asks_for_an_event_stream(settings: Settings) -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(200, content=sse_body(("1", "a"))))
    take(EventStream(settings, sleep=Sleeps()).events(), 1)
    headers = route.calls[0].request.headers
    assert headers["accept"] == "text/event-stream"
    assert headers["user-agent"] == "wiki-edits-streaming/0.1 (+https://example.org/test-operator)"
    assert "last-event-id" not in headers


@respx.mock
def test_starts_from_a_saved_position(settings: Settings) -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(200, content=sse_body(("2", "b"))))
    take(EventStream(settings, sleep=Sleeps()).events("saved-token"), 1)
    assert route.calls[0].request.headers["last-event-id"] == "saved-token"


@respx.mock
def test_reconnects_where_it_left_off(settings: Settings) -> None:
    """The server closes the connection; the next request resumes from the
    last event received, so nothing in between is lost or repeated."""
    route = respx.get(URL).mock(
        side_effect=[
            httpx.Response(200, content=sse_body(("1", "a"), ("2", "b"))),
            httpx.Response(200, content=sse_body(("3", "c"))),
        ]
    )
    sleeps = Sleeps()
    events = take(EventStream(settings, sleep=sleeps).events(), 3)
    assert [e.data for e in events] == ["a", "b", "c"]
    assert route.calls[1].request.headers["last-event-id"] == "2"
    assert sleeps.calls == [1.0]


class _CutOff(httpx.SyncByteStream):
    """Sends some bytes, then the connection drops mid-event."""

    def __init__(self, data: bytes) -> None:
        self.data = data

    def __iter__(self) -> Iterator[bytes]:
        yield self.data
        raise httpx.ReadError("connection reset")


@respx.mock
def test_a_dropped_connection_resumes_after_the_last_complete_event(settings: Settings) -> None:
    half = sse_body(("1", "a")) + b'event: message\nid: 2\ndata: {"cut'
    route = respx.get(URL).mock(
        side_effect=[
            httpx.Response(200, stream=_CutOff(half)),
            httpx.Response(200, content=sse_body(("2", "b"))),
        ]
    )
    events = take(EventStream(settings, sleep=Sleeps()).events(), 2)
    assert [e.data for e in events] == ["a", "b"]
    assert route.calls[1].request.headers["last-event-id"] == "1"


@respx.mock
def test_server_errors_back_off_exponentially_up_to_the_cap(settings: Settings) -> None:
    respx.get(URL).mock(return_value=httpx.Response(503))
    sleeps = Sleeps(allow=5)
    with pytest.raises(StopLoopError):
        take(EventStream(settings, sleep=sleeps).events(), 1)
    assert sleeps.calls == [1.0, 2.0, 4.0, 8.0, 8.0, 8.0]


@respx.mock
def test_rate_limiting_is_retried(settings: Settings) -> None:
    respx.get(URL).mock(
        side_effect=[httpx.Response(429), httpx.Response(200, content=sse_body(("1", "a")))]
    )
    assert [e.data for e in take(EventStream(settings, sleep=Sleeps()).events(), 1)] == ["a"]


@respx.mock
def test_network_errors_are_retried(settings: Settings) -> None:
    respx.get(URL).mock(
        side_effect=[
            httpx.ConnectError("no route"),
            httpx.Response(200, content=sse_body(("1", "a"))),
        ]
    )
    assert [e.data for e in take(EventStream(settings, sleep=Sleeps()).events(), 1)] == ["a"]


@respx.mock
def test_backoff_resets_once_events_flow_again(settings: Settings) -> None:
    respx.get(URL).mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(503),
            httpx.Response(200, content=sse_body(("1", "a"))),
            httpx.Response(503),
            httpx.Response(200, content=sse_body(("2", "b"))),
        ]
    )
    sleeps = Sleeps()
    take(EventStream(settings, sleep=sleeps).events(), 2)
    assert sleeps.calls == [1.0, 2.0, 1.0, 2.0]


@respx.mock
def test_a_client_error_stops_instead_of_retrying(settings: Settings) -> None:
    """403 means the request itself is refused (for example its User-Agent).
    Retrying forever would only hammer the server."""
    route = respx.get(URL).mock(return_value=httpx.Response(403))
    sleeps = Sleeps()
    with pytest.raises(SourceError, match="HTTP 403"):
        take(EventStream(settings, sleep=sleeps).events(), 1)
    assert route.call_count == 1
    assert sleeps.calls == []


def test_refuses_to_start_without_a_contact(settings: Settings) -> None:
    with pytest.raises(SourceError, match="WIKISTREAM_CONTACT"):
        EventStream(settings.model_copy(update={"contact": "  "}))


def test_replay_reads_a_recorded_capture() -> None:
    path = Path(__file__).parent / "fixtures" / "recentchange_sample.sse"
    assert len(list(replay(path))) == 95


# Positions as EventStreams writes them: the main datacenter by timestamp, the
# quiet one still at "-1" because none of its events has arrived yet.
EQIAD = "eqiad.mediawiki.recentchange"
CODFW = "codfw.mediawiki.recentchange"
QUIET_CODFW = (
    f'[{{"topic":"{EQIAD}","partition":0,"timestamp":1791909668426}},'
    f'{{"topic":"{CODFW}","partition":0,"offset":-1}}]'
)


def test_a_topic_without_a_position_resumes_with_the_others_not_from_now() -> None:
    fixed = json.loads(resume_id(QUIET_CODFW))
    assert fixed == [
        {"topic": EQIAD, "partition": 0, "timestamp": 1791909668426},
        {"topic": CODFW, "partition": 0, "timestamp": 1791909668426 - RESUME_MARGIN_MS},
    ]


def test_ids_with_every_position_known_are_sent_unchanged() -> None:
    both = (
        f'[{{"topic":"{EQIAD}","partition":0,"timestamp":1791909765967}},'
        f'{{"topic":"{CODFW}","partition":0,"timestamp":1791909726942}}]'
    )
    assert resume_id(both) is both
    for opaque in (
        "2",
        "saved-token",
        "[]",
        '{"a": 1}',
        "[1, 2]",
        f'[{{"topic":"{CODFW}","offset":-1}}]',
    ):
        assert resume_id(opaque) == opaque


def test_every_recorded_id_resumes_all_topics() -> None:
    fixture = Path(__file__).parent / "fixtures" / "recentchange_sample.sse"
    with fixture.open(encoding="utf-8") as handle:
        ids = [e.id for e in parse(handle) if e.id]
    assert sum('"offset":-1' in i for i in ids) > 90  # as recorded: codfw is quiet
    for i in ids:
        assert all("timestamp" in p for p in json.loads(resume_id(i)))


@respx.mock
def test_the_header_carries_the_fixed_id(settings: Settings) -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(200, content=sse_body(("2", "b"))))
    take(EventStream(settings, sleep=Sleeps()).events(QUIET_CODFW), 1)
    assert route.calls[0].request.headers["last-event-id"] == resume_id(QUIET_CODFW)
    assert "-1" not in route.calls[0].request.headers["last-event-id"]
