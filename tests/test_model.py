"""Mapping real events to the registered Avro schema."""

from __future__ import annotations

import json
from collections import Counter

import fastavro
import pytest

from wikistream.model import (
    InvalidEventError,
    decode,
    is_canary,
    message_key,
    schema_str,
    to_record,
)
from wikistream.sse import SseEvent

SCHEMA = fastavro.parse_schema(json.loads(schema_str()))


def records(recorded: list[SseEvent]) -> list[dict[str, object]]:
    return [to_record(decode(e.data), e.data) for e in recorded]


def test_every_real_event_fits_the_schema(recorded: list[SseEvent]) -> None:
    for record in records(recorded):
        assert fastavro.validation.validate(record, SCHEMA, raise_errors=True)


def test_the_capture_covers_every_shape_of_event(recorded: list[SseEvent]) -> None:
    """The fixture was cut to include each case the mapping must survive."""
    rows = records(recorded)
    assert Counter(r["type"] for r in rows) == {"categorize": 52, "edit": 29, "log": 9, "new": 5}
    assert sum(r["rc_id"] is None for r in rows) == 7
    assert sum(str(r["source_topic"]).startswith("codfw") for r in rows) == 1
    assert sum(str(r["user"]).startswith("~") for r in rows) == 9


def test_raw_is_the_event_exactly_as_received(recorded: list[SseEvent]) -> None:
    for event, record in zip(recorded, records(recorded), strict=True):
        assert record["raw"] == event.data


def test_size_and_revision_only_where_the_source_sends_them(recorded: list[SseEvent]) -> None:
    for record in records(recorded):
        if record["type"] in ("edit", "new"):
            assert record["length_new"] is not None
            assert record["revision_new"] is not None
        else:
            assert record["length_new"] is None
            assert record["revision_new"] is None


def test_log_entries_without_a_recent_changes_id(recorded: list[SseEvent]) -> None:
    """Some log types (abuse filter hits, patrols, reviews) have no rc id.
    That is why deduplication uses event_id, which every event has."""
    no_id = [r for r in records(recorded) if r["rc_id"] is None]
    assert {r["type"] for r in no_id} == {"log"}
    assert all(r["event_id"] for r in records(recorded))
    assert len({r["event_id"] for r in records(recorded)}) == len(recorded)


def test_times_are_milliseconds_and_stream_follows_edit(recorded: list[SseEvent]) -> None:
    for record in records(recorded):
        event_time, stream_time = record["event_time"], record["stream_time"]
        assert isinstance(event_time, int) and isinstance(stream_time, int)
        assert event_time % 1000 == 0, "the source's edit time is whole seconds"
        # Published within a minute of the edit: the capture's worst case was 46 s.
        assert -1000 < stream_time - event_time < 60_000


def test_page_key_spreads_commons_over_many_keys(recorded: list[SseEvent]) -> None:
    """Keyed by wiki, Commons (about half of all events) would be one hot key."""
    commons = [r for r in records(recorded) if r["wiki"] == "commonswiki"]
    assert len({message_key(r) for r in commons}) > len(commons) / 3


def base_event() -> dict[str, object]:
    return {
        "$schema": "/mediawiki/recentchange/1.0.0",
        "meta": {
            "id": "00000000-0000-0000-0000-000000000001",
            "dt": "2026-10-13T16:41:08.425Z",
            "topic": "eqiad.mediawiki.recentchange",
            "partition": 0,
            "offset": 1,
            "domain": "en.wikipedia.org",
        },
        "type": "edit",
        "wiki": "enwiki",
        "server_name": "en.wikipedia.org",
        "namespace": 0,
        "title": "Tbilisi",
        "user": "Someone",
        "bot": False,
        "timestamp": 1791909666,
    }


def test_canary_events_are_recognised() -> None:
    event = base_event()
    event["meta"] = {**event["meta"], "domain": "canary"}  # type: ignore[dict-item]
    assert is_canary(event)
    assert not is_canary(base_event())


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"timestamp": "yesterday"}, "'timestamp' is not int"),
        ({"timestamp": True}, "'timestamp' is not int"),
        ({"meta": None}, "missing meta"),
        ({"title": None}, "'title' is not str"),
        ({"bot": "no"}, "'bot' is not bool"),
        ({"namespace": "0"}, "'namespace' is not int"),
    ],
)
def test_malformed_events_are_rejected_with_a_reason(
    change: dict[str, object], reason: str
) -> None:
    """A null where text belongs is a broken event. It must not be coerced
    into the string "None" and published as a page title."""
    with pytest.raises(InvalidEventError, match=reason):
        to_record({**base_event(), **change}, "{}")


def test_a_missing_required_field_is_named() -> None:
    event = base_event()
    del event["wiki"]
    with pytest.raises(InvalidEventError, match="missing field 'wiki'"):
        to_record(event, "{}")


@pytest.mark.parametrize("raw", ["not json", "[1, 2]"])
def test_decode_rejects_anything_but_an_object(raw: str) -> None:
    with pytest.raises(InvalidEventError):
        decode(raw)


def test_a_malformed_stream_time_is_rejected() -> None:
    event = base_event()
    event["meta"] = {**event["meta"], "dt": "not a date"}  # type: ignore[dict-item]
    with pytest.raises(InvalidEventError, match=r"bad value in meta\.dt"):
        to_record(event, "{}")
