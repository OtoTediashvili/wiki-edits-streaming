"""The dashboard's numbers: SQL over the Arrow tables that `lake` read.

DuckDB runs inside this process and queries the Arrow tables where they
are; nothing is copied into a database. Each method returns plain values
or a small pandas DataFrame, ready for Streamlit.

Two rules the numbers follow:

- Only complete minutes. The newest minute in the lake is still filling
  (the job writes every minute, events arrive seconds late), so the window
  ends where it starts. Otherwise every chart would end in a false drop.
- Distinct counts come from single changes, never from adding up per-minute
  counts: an editor active in 30 minutes is one editor, not 30.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import quote

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

MINUTE = timedelta(minutes=1)

# The windows a viewer can pick. The keys are the labels and go into the URL
# as they are: ?window=6h.
SPANS: dict[str, timedelta] = {
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "6h": timedelta(hours=6),
    "24h": timedelta(hours=24),
}
DEFAULT_SPAN = "1h"

# Single changes are read for at most this long: about 110,000 rows. The
# per-minute table covers longer windows.
DETAIL_SPAN = timedelta(hours=1)


@dataclass(frozen=True)
class Window:
    start: datetime  # inclusive
    end: datetime  # exclusive

    @property
    def minutes(self) -> int:
        return int((self.end - self.start) / MINUTE)


def window_for(newest_minute: datetime, now: datetime, span: timedelta) -> Window:
    """The `span` of complete minutes before the newest one, which is still
    filling. Anchored to the lake, not the clock, so the page shows the
    latest counts while the job catches up after time away; but never past
    the clock, in case an event is stamped in the future."""
    end = min(newest_minute, now.replace(second=0, microsecond=0))
    return Window(end - span, end)


def detail_window(window: Window) -> Window:
    """The part of the window that single changes are read for."""
    return Window(max(window.start, window.end - DETAIL_SPAN), window.end)


@dataclass(frozen=True)
class Totals:
    changes: int
    edits: int
    new_pages: int
    bot_events: int
    net_bytes: int
    wikis: int
    editors: int  # distinct user names, over the detail window
    minutes: int  # minutes in the window that the lake has counts for

    @property
    def per_minute(self) -> float | None:
        return self.changes / self.minutes if self.minutes else None

    @property
    def bot_share(self) -> float | None:
        return self.bot_events / self.changes if self.changes else None


ALL_WIKIS = "all"


def _only(table: pa.Table, wiki: str) -> pa.Table:
    if wiki == ALL_WIKIS:
        return table
    return table.filter(pc.equal(table["wiki"], wiki))


class Numbers:
    """Everything on the page for one window, optionally one wiki."""

    def __init__(self, minutes: pa.Table, changes: pa.Table, wiki: str = ALL_WIKIS) -> None:
        self._db = duckdb.connect()
        # `covered` tells which minutes the lake has counts for at all, so a
        # quiet wiki's empty minutes count as zero rather than missing.
        self._db.register("covered", minutes.select(["window_start"]))
        self._db.register("all_minutes", minutes.select(["wiki", "events"]))
        self._db.register("minutes", _only(minutes, wiki))
        self._db.register("changes", _only(changes, wiki))

    def _rows(self, sql: str, **params: Any) -> list[tuple[Any, ...]]:
        return self._db.execute(sql, params).fetchall()

    def _frame(self, sql: str, **params: Any) -> pd.DataFrame:
        return self._db.execute(sql, params).df()

    def totals(self) -> Totals:
        [(changes, edits, new_pages, bots, net_bytes, wikis)] = self._rows(
            """
            SELECT coalesce(sum(events), 0),
                   coalesce(sum(edits), 0),
                   coalesce(sum(new_pages), 0),
                   coalesce(sum(bot_events), 0),
                   coalesce(sum(bytes_added) - sum(bytes_removed), 0),
                   count(DISTINCT wiki) FILTER (WHERE events > 0)
            FROM minutes
            """
        )
        [(editors,)] = self._rows('SELECT count(DISTINCT "user") FROM changes')
        [(minutes,)] = self._rows("SELECT count(DISTINCT window_start) FROM covered")
        return Totals(
            changes=int(changes),
            edits=int(edits),
            new_pages=int(new_pages),
            bot_events=int(bots),
            net_bytes=int(net_bytes),
            wikis=int(wikis),
            editors=int(editors),
            minutes=int(minutes),
        )

    def per_minute(self) -> pd.DataFrame:
        """minute, Humans, Bots: one row per minute the lake has counts for."""
        return self._frame(
            """
            WITH counted AS (
                SELECT window_start, sum(human_events) AS humans, sum(bot_events) AS bots
                FROM minutes
                GROUP BY window_start
            )
            SELECT window_start AS minute,
                   coalesce(humans, 0)::BIGINT AS "Humans",
                   coalesce(bots, 0)::BIGINT AS "Bots"
            FROM (SELECT DISTINCT window_start FROM covered)
            LEFT JOIN counted USING (window_start)
            ORDER BY minute
            """
        )

    def wikis(self) -> list[tuple[str, int]]:
        """Every wiki with changes in the window, busiest first; ignores the
        wiki filter, so the picker always offers all of them."""
        rows = self._rows(
            """
            SELECT wiki, sum(events) AS changes
            FROM all_minutes
            GROUP BY wiki
            HAVING changes > 0
            ORDER BY changes DESC, wiki
            """
        )
        return [(str(wiki), int(changes)) for wiki, changes in rows]

    def top_wikis(self, n: int = 10) -> pd.DataFrame:
        """wiki, Humans, Bots, Changes: the n wikis with the most changes."""
        return self._frame(
            """
            SELECT wiki,
                   sum(human_events)::BIGINT AS "Humans",
                   sum(bot_events)::BIGINT AS "Bots",
                   sum(events)::BIGINT AS "Changes"
            FROM minutes
            GROUP BY wiki
            HAVING "Changes" > 0
            ORDER BY "Changes" DESC, wiki
            LIMIT $n
            """,
            n=n,
        )

    def busiest_pages(self, n: int = 10) -> pd.DataFrame:
        """wiki, title, editors, edits, url: the pages the most people edited.

        Ranked by distinct editors, then edits. By edits alone, the first
        hour of real data put a Wikidata item on top with 101 edits, all by
        one person running a tool; semi-automated tools aren't flagged as
        bots. Pages several people are editing are what's happening. Bots
        are left out for the same reason."""
        frame = self._frame(
            """
            SELECT wiki, server_name, title,
                   count(*) AS edits,
                   count(DISTINCT "user") AS editors
            FROM changes
            WHERE type IN ('edit', 'new') AND NOT coalesce(bot, false)
            GROUP BY wiki, server_name, title
            ORDER BY editors DESC, edits DESC, wiki, title
            LIMIT $n
            """,
            n=n,
        )
        return _with_urls(frame)

    def newest_articles(self, n: int = 10) -> pd.DataFrame:
        """time, wiki, title, bytes, url: the latest pages people created in
        a wiki's main namespace (0): articles, dictionary entries, Wikidata
        items; no talk, user or file pages."""
        frame = self._frame(
            """
            SELECT event_time AS time, wiki, server_name, title, length_new AS bytes
            FROM changes
            WHERE type = 'new' AND namespace = 0 AND NOT coalesce(bot, false)
            ORDER BY event_time DESC, wiki, title
            LIMIT $n
            """,
            n=n,
        )
        return _with_urls(frame)


# What MediaWiki leaves unescaped in page URLs (its wfUrlencode), on top of
# what `quote` always keeps.
_URL_SAFE = ";@$!*(),/~:"


def page_url(server_name: str | None, title: str | None) -> str | None:
    if not server_name or not title:
        return None
    return f"https://{server_name}/wiki/{quote(title.replace(' ', '_'), safe=_URL_SAFE)}"


def _with_urls(frame: pd.DataFrame) -> pd.DataFrame:
    frame["url"] = [
        page_url(server, title)
        for server, title in zip(frame["server_name"], frame["title"], strict=True)
    ]
    return frame.drop(columns=["server_name"])


# --- formatting -------------------------------------------------------------

DASH = "\u2013"  # en dash: no value
MINUS = "\u2212"  # minus sign, as wide as the plus


def compact(value: float | None) -> str:
    """1,284 · 12.9K · 129K · 4.2M: exact below 10,000, then short."""
    if value is None:
        return DASH
    size = abs(value)
    if size < 10_000:
        return f"{value:,.0f}"
    if size < 99_950:
        return f"{value / 1e3:.1f}K"
    if size < 999_500:
        return f"{value / 1e3:.0f}K"
    return f"{value / 1e6:.1f}M"


def percent(share: float | None) -> str:
    return DASH if share is None else f"{share:.0%}"


def signed_bytes(value: int) -> str:
    """+1.2 MB, \u2212340 KB: decimal units and a real minus sign."""
    sign = "+" if value > 0 else MINUS if value < 0 else ""
    size, unit = float(abs(value)), "B"
    for bigger in ("KB", "MB", "GB"):
        if size < 999.5:
            break
        size, unit = size / 1000, bigger
    number = f"{size:.1f}" if unit != "B" and size < 99.95 else f"{size:.0f}"
    return f"{sign}{number} {unit}"


def duration(delta: timedelta) -> str:
    """45 s · 12 min · 2 h 14 min · 3 days."""
    seconds = int(delta.total_seconds())
    if seconds < 60:
        return f"{max(seconds, 0)} s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} min"
    hours, minutes = divmod(minutes, 60)
    if hours < 48:
        return f"{hours} h {minutes} min" if minutes else f"{hours} h"
    return f"{hours // 24} days"
