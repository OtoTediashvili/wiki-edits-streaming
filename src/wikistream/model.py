"""Turn one recentchange event (JSON text) into a WikiChange record.

The record's shape is wiki_change.avsc, next to this file, and registered
with the schema registry. Common fields are typed so consumers can rely on
them; the original JSON is kept whole in `raw`, so a field this code ignores
today is still there tomorrow.

Fields vary by event type: size and revision only on edits and new pages, log
fields only on log entries, and some log types carry no recent-changes id at
all. Missing optional fields become null; missing required ones make the
event invalid, and it goes to the dead-letter topic instead.
"""

from __future__ import annotations

import json
from datetime import datetime
from importlib import resources
from typing import Any

WikiChange = dict[str, Any]

_SCHEMA_FILE = "wiki_change.avsc"


class InvalidEventError(ValueError):
    """The event cannot be turned into a WikiChange record."""


def schema_str() -> str:
    """The Avro schema, as shipped inside the package."""
    return resources.files("wikistream").joinpath(_SCHEMA_FILE).read_text(encoding="utf-8")


def is_canary(event: dict[str, Any]) -> bool:
    """Wikimedia injects synthetic 'canary' events to monitor its streams.

    They are not edits. Their meta.domain is "canary".
    """
    meta = event.get("meta")
    return isinstance(meta, dict) and meta.get("domain") == "canary"


def _millis(iso: str) -> int:
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


def _opt_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _opt_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _opt_str(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _req(container: dict[str, Any], key: str, kind: type, where: str = "") -> Any:
    """A required field of an exact type. A null or a number where text belongs
    is a broken event, not something to coerce into the string "None"."""
    if key not in container:
        raise InvalidEventError(f"missing field {where}{key!r}")
    value = container[key]
    if not isinstance(value, kind) or (kind is int and isinstance(value, bool)):
        raise InvalidEventError(f"field {where}{key!r} is not {kind.__name__}: {value!r}")
    return value


def decode(raw: str) -> dict[str, Any]:
    """Parse the JSON text of one event. Raises InvalidEventError."""
    try:
        event = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise InvalidEventError(f"not JSON: {exc}") from exc
    if not isinstance(event, dict):
        raise InvalidEventError("not a JSON object")
    return event


def to_record(event: dict[str, Any], raw: str) -> WikiChange:
    """Map one decoded event. Raises InvalidEventError with the reason."""
    meta = event.get("meta")
    if not isinstance(meta, dict):
        raise InvalidEventError("missing meta")
    length = event.get("length")
    length = length if isinstance(length, dict) else {}
    revision = event.get("revision")
    revision = revision if isinstance(revision, dict) else {}
    timestamp: int = _req(event, "timestamp", int)
    try:
        stream_time = _millis(_req(meta, "dt", str, "meta."))
    except ValueError as exc:
        raise InvalidEventError(f"bad value in meta.dt: {exc}") from exc
    return {
        "event_id": _req(meta, "id", str, "meta."),
        "event_time": timestamp * 1000,
        "stream_time": stream_time,
        "type": _req(event, "type", str),
        "wiki": _req(event, "wiki", str),
        "server_name": _req(event, "server_name", str),
        "namespace": _req(event, "namespace", int),
        "title": _req(event, "title", str),
        "user": _opt_str(event.get("user")),
        "bot": _req(event, "bot", bool),
        "minor": _opt_bool(event.get("minor")),
        "patrolled": _opt_bool(event.get("patrolled")),
        "rc_id": _opt_int(event.get("id")),
        "length_old": _opt_int(length.get("old")),
        "length_new": _opt_int(length.get("new")),
        "revision_old": _opt_int(revision.get("old")),
        "revision_new": _opt_int(revision.get("new")),
        "log_type": _opt_str(event.get("log_type")),
        "log_action": _opt_str(event.get("log_action")),
        "source_topic": _req(meta, "topic", str, "meta."),
        "source_partition": _req(meta, "partition", int, "meta."),
        "source_offset": _req(meta, "offset", int, "meta."),
        "schema_uri": _req(event, "$schema", str),
        "raw": raw,
    }


def message_key(record: WikiChange) -> str:
    """Partition key: the page, not the wiki.

    Keying by wiki would send Wikimedia Commons, about half of all events, to
    a single partition. A page key spreads load evenly and still keeps every
    change to one page in order.
    """
    return f"{record['wiki']}:{record['title']}"
