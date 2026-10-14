"""Shared fixtures. Tests never read a developer's .env or environment."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from wikistream.config import Settings
from wikistream.sse import SseEvent, parse

FIXTURE = Path(__file__).parent / "fixtures" / "recentchange_sample.sse"


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for key in [k for k in os.environ if k.startswith("WIKISTREAM_")]:
        monkeypatch.delenv(key, raising=False)
    yield


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,  # type: ignore[call-arg]
        contact="https://example.org/test-operator",
        stream_url="https://stream.test/v2/stream/recentchange",
        state_path=tmp_path / "state" / "last_event_id",
        checkpoint_seconds=5.0,
        max_backoff_seconds=8.0,
    )


@pytest.fixture(scope="session")
def recorded() -> list[SseEvent]:
    """The real capture: 95 events from 13 Oct 2026, chosen to include every
    event type, log entries without a recent-changes id, an event from the
    second datacenter, temporary accounts and the latest arrivals."""
    with FIXTURE.open(encoding="utf-8") as handle:
        return list(parse(handle))
