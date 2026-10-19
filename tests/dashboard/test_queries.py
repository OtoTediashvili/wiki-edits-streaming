"""The dashboard's numbers, against small tables worked out by hand."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pytest

from tests.dashboard.lakes import change, minute, read_back
from wikistream.dashboard.queries import (
    ALL_WIKIS,
    DASH,
    MINUS,
    Numbers,
    Window,
    compact,
    detail_window,
    duration,
    page_url,
    percent,
    signed_bytes,
    window_for,
)
from wikistream.streaming.transform import CHANGE_COLUMNS, MINUTE_COLUMNS

T0 = datetime(2026, 10, 19, 10, 0)


def at(minutes: float) -> datetime:
    return T0 + timedelta(minutes=minutes)


def numbers(
    minute_rows: list[dict[str, Any]],
    change_rows: list[dict[str, Any]] | None = None,
    wiki: str = ALL_WIKIS,
) -> Numbers:
    return Numbers(
        read_back(MINUTE_COLUMNS, minute_rows), read_back(CHANGE_COLUMNS, change_rows or []), wiki
    )


# Four minutes. enwiki is quiet in two of them; commons is mostly bots.
MINUTES = [
    minute(
        "enwiki", at(0), humans=30, bots=10, new_pages=2, bytes_added=5_000, bytes_removed=1_000
    ),
    minute("commonswiki", at(0), humans=10, bots=90),
    minute("enwiki", at(1), humans=20, bots=0, bytes_added=100, bytes_removed=2_100),
    minute("commonswiki", at(1), humans=5, bots=95),
    minute("commonswiki", at(2), humans=0, bots=80),
    minute("commonswiki", at(3), humans=10, bots=60),
    minute("dewiki", at(3), humans=0, bots=0),  # a row with nothing in it
]


def test_totals_add_up_over_the_window() -> None:
    totals = numbers(MINUTES).totals()
    assert totals.changes == 40 + 100 + 20 + 100 + 80 + 70
    assert totals.edits == totals.changes - 2
    assert totals.new_pages == 2
    assert totals.bot_events == 10 + 90 + 95 + 80 + 60
    assert totals.net_bytes == 5_100 - 3_100
    assert totals.wikis == 2  # dewiki had no changes
    assert totals.minutes == 4
    assert totals.per_minute == pytest.approx(410 / 4)
    assert totals.bot_share == pytest.approx(335 / 410)


def test_a_quiet_wikis_rate_counts_its_empty_minutes() -> None:
    """enwiki has rows in 2 of the 4 minutes: its rate is per 4 minutes, not 2."""
    totals = numbers(MINUTES, wiki="enwiki").totals()
    assert (totals.changes, totals.minutes) == (60, 4)
    assert totals.per_minute == 15
    assert totals.wikis == 1


def test_editors_are_distinct_people_not_a_sum_of_minutes() -> None:
    """Alice edits in three minutes: per-minute counts say 3 editors, she is 1."""
    edits = [
        change(at(0), user="Alice"),
        change(at(1), user="Alice"),
        change(at(2), user="Alice", wiki="dewiki", server_name="de.wikipedia.org"),
        change(at(2), user="Bob"),
        change(at(3), user=None, type="log"),  # some log entries have no user
    ]
    assert numbers(MINUTES, edits).totals().editors == 2
    assert numbers(MINUTES, edits, wiki="dewiki").totals().editors == 1


def test_per_minute_has_every_covered_minute_in_order() -> None:
    frame = numbers(MINUTES, wiki="enwiki").per_minute()
    assert list(frame["minute"]) == [at(0), at(1), at(2), at(3)]
    assert list(frame["Humans"]) == [30, 20, 0, 0]
    assert list(frame["Bots"]) == [10, 0, 0, 0]
    everything = numbers(MINUTES).per_minute()
    assert list(everything["Humans"] + everything["Bots"]) == [140, 120, 80, 70]


def test_top_wikis_busiest_first_and_only_those_with_changes() -> None:
    rows = [*MINUTES, minute("frwiki", at(0), humans=60)]
    top = numbers(rows).top_wikis()
    assert list(top["wiki"]) == ["commonswiki", "enwiki", "frwiki"]
    assert list(top["Changes"]) == [350, 60, 60]  # enwiki before frwiki: a tie, by name
    assert list(top["Humans"] + top["Bots"]) == list(top["Changes"])
    assert list(numbers(rows).top_wikis(n=1)["wiki"]) == ["commonswiki"]


def test_the_wiki_list_ignores_the_wiki_filter() -> None:
    assert numbers(MINUTES, wiki="enwiki").wikis() == [("commonswiki", 350), ("enwiki", 60)]


def test_pages_with_the_most_editors_with_links() -> None:
    edits = [
        # One person running a tool: many edits, one editor. Not flagged as a bot.
        *[
            change(at(0), title="Q42", wiki="wikidatawiki", user="Tool user",
                   server_name="www.wikidata.org")
            for _ in range(9)
        ],
        *[change(at(0), title="Ada Lovelace", user="A") for _ in range(3)],
        *[change(at(1), title="Zürich", user=u) for u in ("B", "C", "B")],
        change(at(1), title="Talk:Ada Lovelace", user="A", namespace=1),
        *[
            change(at(2), title="Q64", wiki="wikidatawiki", user="Bot", bot=True,
                   server_name="www.wikidata.org")
            for _ in range(20)
        ],
        *[change(at(2), title="Zürich", user="D", type=kind) for kind in ("log", "categorize")],
    ]  # fmt: skip
    pages = numbers(MINUTES, edits).busiest_pages()
    # Two people editing a page beats one person's 9 edits; then edits count.
    assert list(pages["title"]) == ["Zürich", "Q42", "Ada Lovelace", "Talk:Ada Lovelace"]
    assert list(pages["editors"]) == [2, 1, 1, 1]
    assert list(pages["edits"]) == [3, 9, 3, 1]  # no bot edits, no log entries
    assert list(pages["url"]) == [
        "https://en.wikipedia.org/wiki/Z%C3%BCrich",
        "https://www.wikidata.org/wiki/Q42",
        "https://en.wikipedia.org/wiki/Ada_Lovelace",
        "https://en.wikipedia.org/wiki/Talk:Ada_Lovelace",
    ]
    assert "server_name" not in pages.columns


def test_newest_articles_are_new_human_articles_newest_first() -> None:
    created = [
        change(at(0), title="Old", type="new"),
        change(at(2), title="Newest", type="new", length_new=4_321),
        change(at(1), title="Middle", type="new"),
        change(at(3), title="User:Alice/Sandbox", type="new", namespace=2),
        change(at(3), title="Q99", type="new", bot=True),
        change(at(3), title="Edited", type="edit"),
    ]
    articles = numbers(MINUTES, created).newest_articles()
    assert list(articles["title"]) == ["Newest", "Middle", "Old"]
    assert list(articles["time"]) == [at(2), at(1), at(0)]
    assert articles["bytes"].iloc[0] == 4_321
    assert list(numbers(MINUTES, created).newest_articles(n=1)["title"]) == ["Newest"]


def test_an_empty_window_gives_zeros_not_errors() -> None:
    empty = numbers([])
    totals = empty.totals()
    assert (totals.changes, totals.minutes, totals.editors) == (0, 0, 0)
    assert totals.per_minute is None and totals.bot_share is None
    assert empty.per_minute().empty and empty.top_wikis().empty
    assert empty.busiest_pages().empty and empty.newest_articles().empty
    assert empty.wikis() == []


def test_the_window_ends_where_the_newest_minute_starts() -> None:
    newest = datetime(2026, 10, 19, 10, 42)
    live = window_for(newest, now=datetime(2026, 10, 19, 10, 43, 15), span=timedelta(hours=1))
    assert live == Window(datetime(2026, 10, 19, 9, 42), newest)
    assert live.minutes == 60
    # Catching up: the window follows the lake, not the clock.
    behind = window_for(newest, now=datetime(2026, 10, 21, 8, 0), span=timedelta(minutes=15))
    assert behind.end == newest and behind.minutes == 15
    # An event stamped in the future doesn't drag the window past the clock.
    ahead = window_for(newest, now=datetime(2026, 10, 19, 10, 30, 59), span=timedelta(minutes=15))
    assert ahead.end == datetime(2026, 10, 19, 10, 30)


def test_single_changes_are_read_for_at_most_an_hour() -> None:
    day = Window(datetime(2026, 10, 18, 12, 0), datetime(2026, 10, 19, 12, 0))
    assert detail_window(day) == Window(datetime(2026, 10, 19, 11, 0), day.end)
    short = Window(datetime(2026, 10, 19, 11, 45), datetime(2026, 10, 19, 12, 0))
    assert detail_window(short) == short


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (None, DASH),
        (0, "0"),
        (946.4, "946"),
        (1_284, "1,284"),
        (9_999, "9,999"),
        (12_949, "12.9K"),
        (99_949, "99.9K"),
        (99_950, "100K"),
        (129_400, "129K"),
        (999_499, "999K"),
        (999_500, "1.0M"),
        (4_200_000, "4.2M"),
    ],
)
def test_compact_numbers(value: float | None, text: str) -> None:
    assert compact(value) == text


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (0, "0 B"),
        (340, "+340 B"),
        (999, "+999 B"),
        (1_000, "+1.0 KB"),
        (1_234, "+1.2 KB"),
        (340_000, "+340 KB"),
        (999_600, "+1.0 MB"),
        (1_234_567, "+1.2 MB"),
        (-340_000, f"{MINUS}340 KB"),
    ],
)
def test_signed_bytes(value: int, text: str) -> None:
    assert signed_bytes(value) == text


@pytest.mark.parametrize(
    ("seconds", "text"),
    [
        (-3, "0 s"),
        (45, "45 s"),
        (60, "1 min"),
        (720, "12 min"),
        (3_600, "1 h"),
        (8_040, "2 h 14 min"),
        (3 * 86_400, "3 days"),
    ],
)
def test_durations(seconds: int, text: str) -> None:
    assert duration(timedelta(seconds=seconds)) == text


def test_percent_and_page_urls() -> None:
    assert (percent(None), percent(0.2949), percent(1.0)) == (DASH, "29%", "100%")
    assert page_url("de.wikipedia.org", "Köln (Begriffsklärung)") == (
        "https://de.wikipedia.org/wiki/K%C3%B6ln_(Begriffskl%C3%A4rung)"
    )
    assert page_url(None, "Anything") is None
    assert page_url("en.wikipedia.org", None) is None
