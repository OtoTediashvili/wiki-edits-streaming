"""SSE parsing, against the real capture and the spec's edge cases."""

from __future__ import annotations

import json

from wikistream.sse import SseEvent, parse


def test_the_capture_parses_into_every_event(recorded: list[SseEvent]) -> None:
    assert len(recorded) == 95
    assert {e.event for e in recorded} == {"message"}


def test_every_event_carries_its_resume_token(recorded: list[SseEvent]) -> None:
    """EventStreams' ids are JSON lists: one position per source topic, one
    topic per datacenter. They are kept verbatim, to be sent back as is."""
    for event in recorded:
        assert event.id is not None
        positions = json.loads(event.id)
        assert {p["topic"] for p in positions} == {
            "eqiad.mediawiki.recentchange",
            "codfw.mediawiki.recentchange",
        }


def test_comments_are_skipped() -> None:
    assert list(parse([":ok\n", "\n", "data: x\n", "\n"])) == [SseEvent("message", "x", None)]


def test_multi_line_data_is_joined_with_newlines() -> None:
    lines = ["event: message\n", "data: first\n", "data: second\n", "\n"]
    assert next(parse(lines)).data == "first\nsecond"


def test_crlf_line_endings() -> None:
    lines = ["id: 7\r\n", "data: {}\r\n", "\r\n"]
    assert list(parse(lines)) == [SseEvent("message", "{}", "7")]


def test_an_unfinished_event_at_the_end_is_not_dispatched() -> None:
    """A cut connection can leave half an event. The spec drops it, and the
    resume token makes the source send it again."""
    lines = ["data: complete\n", "\n", "data: cut off mid-ev"]
    assert [e.data for e in parse(lines)] == ["complete"]


def test_retry_and_unknown_fields() -> None:
    lines = ["retry: 3000\n", "foo: bar\n", "data: x\n", "\n"]
    assert list(parse(lines)) == [SseEvent("message", "x", None, 3000)]


def test_an_id_containing_nul_is_ignored() -> None:
    lines = ["id: bad\0id\n", "data: x\n", "\n"]
    assert next(parse(lines)).id is None


def test_a_field_without_a_space_after_the_colon() -> None:
    assert next(parse(["data:x\n", "\n"])).data == "x"
