"""The live dashboard: `make dashboard` (container, :8501) or `make dashboard-dev`.

A Streamlit script. It reads the two Iceberg tables through the REST catalog
(`lake`), computes every number with DuckDB (`queries`), and redraws itself
every 30 seconds without reloading the page. Nothing here writes.

Everything is UTC, like the tables. The window ends at the newest complete
minute in the lake rather than at the clock, so after time away the page
shows the latest counts while the job catches up, and says how far behind
they are. During a replay, a few live changes can be counted hours ahead of
the rest; the window then ends at the newest *full* minute instead
(`Lake.position`), and the page says what is counted ahead of it.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import altair as alt
import pandas as pd
import pyarrow as pa
import streamlit as st
from pydantic import Field
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

from wikistream.dashboard.lake import Lake, LakeState, Position
from wikistream.dashboard.queries import (
    ALL_WIKIS,
    DEFAULT_SPAN,
    DETAIL_SPAN,
    MINUTE,
    SPANS,
    Numbers,
    Totals,
    Window,
    compact,
    detail_window,
    duration,
    percent,
    signed_bytes,
    window_for,
)
from wikistream.streaming.catalog import rest_catalog
from wikistream.streaming.settings import LakeSettings

TITLE = "Wikipedia edits, live"
MINUTE_MS = 60_000

# Live: the newest complete minute is at most this far behind the clock.
LIVE_LAG = timedelta(minutes=5)
# The job commits once a minute; this long without a commit means it stopped.
STALLED_AFTER = timedelta(minutes=5)
# Where to look for the newest full minute first: the hour before the newest
# minute. Only a longer replay, with stragglers alone in that hour, makes the
# page read the whole minute table (a few megabytes).
NEAR = timedelta(hours=1)


class DashboardSettings(LakeSettings):
    # How often the page reads the lake again. The job writes once a minute.
    dashboard_refresh_seconds: int = Field(default=30, ge=5)


Clock = Callable[[], datetime]


def utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


# --- colors -------------------------------------------------------------------


@dataclass(frozen=True)
class Palette:
    humans: str
    bots: str
    muted: str  # labels on the plot


# Two categorical slots, one step per mode, checked for color-blind
# separation and contrast against Streamlit's light and dark backgrounds.
# Nothing is drawn in the background color: Streamlit can report the wrong
# mode on a first load, and a "gap" would then show as a dark outline.
PALETTES = {
    "light": Palette(humans="#2a78d6", bots="#eb6834", muted="#898781"),
    "dark": Palette(humans="#3987e5", bots="#d95926", muted="#898781"),
}


def palette() -> Palette:
    return PALETTES.get(st.context.theme.type or "light", PALETTES["light"])


# Axis labels like the tiles: 500, 1.5K, 20K (Vega's own SI format prints "0k").
SHORT_NUMBER = (
    "datum.value >= 1000 ? format(datum.value / 1000, '~r') + 'K' : format(datum.value, '~r')"
)


def _who(colors: Palette) -> alt.Scale:
    """Humans and bots keep their colors in every chart."""
    return alt.Scale(domain=["Humans", "Bots"], range=[colors.humans, colors.bots])


# --- charts -------------------------------------------------------------------


def minute_chart(per_minute: pd.DataFrame, colors: Palette) -> alt.LayerChart:
    """Changes per minute, people and bots: two lines, a crosshair on hover."""
    # Naive times reach the browser as local wall-clock times and would be
    # shifted by the viewer's offset. Marked as UTC, they arrive as instants,
    # and the UTC scale prints them back in UTC.
    data = per_minute.assign(
        minute=per_minute["minute"].dt.tz_localize("UTC"),
        label=per_minute["minute"].dt.strftime("%Y-%m-%d %H:%M"),
    )
    # Labels name whole minutes, so ticks are at least a minute apart: in a
    # 15-minute window Vega would otherwise tick every 30 s and print each
    # minute twice.
    axis = alt.Axis(format="%H:%M", tickMinStep=MINUTE_MS)
    x = alt.X("minute:T", title=None, scale=alt.Scale(type="utc"), axis=axis)
    y = alt.Y("changes:Q", title="Changes per minute", axis=alt.Axis(labelExpr=SHORT_NUMBER))
    color = alt.Color("who:N", scale=_who(colors), legend=alt.Legend(title=None, orient="top"))
    series = alt.Chart(data).transform_fold(["Humans", "Bots"], as_=["who", "changes"])
    lines = series.mark_line(strokeWidth=2, strokeCap="round", strokeJoin="round").encode(
        x=x, y=y, color=color
    )
    ends = (
        series.transform_window(rank="rank()", sort=[alt.SortField("minute", order="descending")])
        .transform_filter(alt.datum.rank == 1)
        .mark_text(align="left", dx=8, color=colors.muted)
        .encode(x=x, y=y, text="who:N")
    )
    hover = alt.selection_point(
        fields=["minute"], nearest=True, on="pointerover", clear="pointerout", empty=False
    )
    rule = (
        alt.Chart(data)
        .mark_rule(color=colors.muted, strokeWidth=1)
        .encode(
            x=x,
            opacity=alt.condition(hover, alt.value(1), alt.value(0)),
            tooltip=[
                alt.Tooltip("label:N", title="Minute (UTC)"),
                alt.Tooltip("Humans:Q", format=","),
                alt.Tooltip("Bots:Q", format=","),
            ],
        )
        .add_params(hover)
    )
    dots = lines.mark_point(filled=True, size=64).encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0))
    )
    return alt.LayerChart(layer=[lines, ends, rule, dots]).properties(
        height=300, padding={"right": 56}
    )


def wikis_chart(top: pd.DataFrame, colors: Palette) -> alt.LayerChart:
    """The busiest wikis: one bar each, people then bots, the total at the tip."""
    data = top.assign(
        total=top["Changes"].map(compact),
        share=(top["Bots"] / top["Changes"]).map(percent),
    )
    y = alt.Y("wiki:N", sort=list(top["wiki"]), title=None, axis=alt.Axis(ticks=False))
    tooltip = [
        alt.Tooltip("wiki:N", title="Wiki"),
        alt.Tooltip("Changes:Q", format=","),
        alt.Tooltip("Humans:Q", format=","),
        alt.Tooltip("Bots:Q", format=","),
        alt.Tooltip("share:N", title="Bot share"),
    ]
    # One row per wiki and side, each a bar segment from `start` to `end`:
    # people from zero, bots from where people end.
    segments = (
        alt.Chart(data)
        .transform_fold(["Humans", "Bots"], as_=["who", "changes"])
        .transform_calculate(
            start="datum.who === 'Humans' ? 0 : datum.Humans",
            end="datum.who === 'Humans' ? datum.Humans : datum.Changes",
        )
    )
    encoding = {
        "y": y,
        "x": alt.X(
            "start:Q",
            title=None,
            axis=alt.Axis(labelExpr=SHORT_NUMBER),
            scale=alt.Scale(domainMax=float(top["Changes"].max()) * 1.15),
        ),
        "x2": "end:Q",
        "color": alt.Color(
            "who:N", scale=_who(colors), legend=alt.Legend(title=None, orient="top")
        ),
        "tooltip": tooltip,
    }
    people = segments.transform_filter(alt.datum.who == "Humans").mark_bar(size=18)
    # The outer segment gets the rounded end, and starts 2 px late: a gap
    # where it meets the inner one.
    bots = segments.transform_filter(alt.datum.who == "Bots").mark_bar(
        size=18, cornerRadiusEnd=4, xOffset=2
    )
    totals = (
        alt.Chart(data)
        .mark_text(align="left", dx=6, color=colors.muted)
        .encode(y=y, x="Changes:Q", text="total:N")
    )
    people, bots = people.encode(**encoding), bots.encode(**encoding)
    return alt.LayerChart(layer=[people, bots, totals]).properties(height=28 * len(top) + 40)


# --- reads, cached --------------------------------------------------------------
#
# Several viewers, one read: results are shared for a few seconds, less than
# the refresh interval, so every refresh still sees the job's latest write.
# `key` stands in for the Lake object, which Streamlit can't hash.


@st.cache_data(ttl=10, show_spinner=False)
def _state(_lake: Lake, key: str) -> LakeState:
    return _lake.state()


# Keyed on the newest minute and the last write, so it's read again only
# after the job has written.
@st.cache_data(ttl=600, max_entries=4, show_spinner=False)
def _position(_lake: Lake, key: str, newest: datetime, last_write: datetime | None) -> Position:
    near = _lake.position(since=newest - NEAR)
    return near if near.full_minute is not None else _lake.position()


def anchor(state: LakeState, position: Position) -> tuple[datetime, int]:
    """The minute the window ends at, and how many straggling changes are
    counted after it: the newest minute, unless a replay has put stragglers
    far ahead of the newest full one."""
    assert state.newest_minute is not None
    if position.straggling and position.full_minute is not None:
        return position.full_minute, position.ahead
    return state.newest_minute, 0


@st.cache_data(ttl=20, show_spinner=False)
def _read(_lake: Lake, key: str, window: Window, detail: Window) -> tuple[pa.Table, pa.Table]:
    return _lake.minutes(window.start, window.end), _lake.changes(detail.start, detail.end)


# --- the page -----------------------------------------------------------------


def render(
    open_lake: Callable[[], Lake],
    *,
    refresh_seconds: int | None,
    now: Clock = utc_now,
    where: str = "the catalog",
    reconnect: Callable[[], None] = lambda: None,
) -> None:
    """The whole page. `open_lake` connects (or returns a cached connection);
    `reconnect` drops a cached connection after an error. Tests pass a lake
    on a local catalog, a fixed clock and no refresh."""
    st.set_page_config(page_title=TITLE, page_icon=":material/edit_note:", layout="wide")
    st.title(TITLE)
    st.caption(
        "Every change on every Wikimedia wiki, streamed through Redpanda and Spark into "
        "Iceberg tables, and read back from them here. All times UTC."
    )
    every = timedelta(seconds=refresh_seconds) if refresh_seconds else None

    @st.fragment(run_every=every)
    def live() -> None:
        _live(open_lake, now, where, reconnect)

    live()
    st.caption(
        "Data: [Wikimedia EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/"
        "EventStreams_HTTP_Service), CC BY-SA. Code: "
        "[wiki-edits-streaming](https://github.com/OtoTediashvili/wiki-edits-streaming)."
    )


def _live(
    open_lake: Callable[[], Lake], now: Clock, where: str, reconnect: Callable[[], None]
) -> None:
    try:
        lake = open_lake()
        state = _state(lake, lake.key)
        position = (
            Position(None, None, 0)
            if state.newest_minute is None
            else _position(lake, lake.key, state.newest_minute, state.last_write)
        )
    except (NoSuchTableError, NoSuchNamespaceError):
        st.info(
            "No tables yet. The stream job creates them when it starts: `make stream`. "
            "This page looks again every 30 seconds.",
            icon=":material/hourglass_empty:",
        )
        return
    except Exception as exc:
        reconnect()
        st.error(
            f"Can't read the lake at {where}: {_reason(exc)}. Is SeaweedFS running? `make up`",
            icon=":material/error:",
        )
        return
    if state.newest_minute is None:
        st.info(
            "The tables exist but hold no counts yet: the job's first batch is still running.",
            icon=":material/hourglass_empty:",
        )
        return

    clock = now()
    controls = st.columns([3, 2], vertical_alignment="bottom")
    with controls[0]:
        span = st.segmented_control(
            "Window",
            options=list(SPANS),
            default=DEFAULT_SPAN,
            required=True,
            key="window",
            bind="query-params",
        )
    end, ahead = anchor(state, position)
    window = window_for(end, clock, SPANS[span or DEFAULT_SPAN])
    detail = detail_window(window)
    minutes, changes = _read(lake, lake.key, window, detail)
    everything = Numbers(minutes, changes)
    with controls[1]:
        wiki = _pick_wiki(everything.wikis())
    numbers = everything if wiki == ALL_WIKIS else Numbers(minutes, changes, wiki)

    _status(state, end, ahead, window, clock)
    totals = numbers.totals()
    _tiles(totals, window, detail)
    if totals.changes == 0:
        st.info("No changes in this window.", icon=":material/info:")
        return

    colors = palette()
    st.subheader("Changes per minute")
    # The chart's toolbar shows its numbers as a table, too.
    st.altair_chart(minute_chart(numbers.per_minute(), colors), width="stretch")

    since = _last(detail.end - detail.start)
    if wiki == ALL_WIKIS:
        left, right = st.columns(2, gap="large")
        with left:
            st.subheader("Busiest wikis")
            st.caption(f"Top 10 of {totals.wikis:,} wikis with changes in the window.")
            st.altair_chart(wikis_chart(numbers.top_wikis(), colors), width="stretch")
        pages = right
    else:
        pages = st.container()
    with pages:
        st.subheader("Pages with the most editors")
        st.caption(f"How many people edited each page, {since}. Bots are left out.")
        _pages_table(numbers.busiest_pages())

    st.subheader("Newest articles")
    st.caption(
        f"Pages people created in each wiki's main namespace (articles, dictionary "
        f"entries, Wikidata items), {since}, newest first."
    )
    _articles_table(numbers.newest_articles())


def _pick_wiki(active: list[tuple[str, int]]) -> str:
    """A wiki to narrow the page to, busiest first. Bound to the URL
    (`?wiki=enwiki`), so a view can be shared. Streamlit puts an option's
    label in the URL, so the labels are the bare wiki names."""
    options = [ALL_WIKIS, *(wiki for wiki, _ in active)]
    # A wiki picked earlier, or named in the URL, stays selectable even in a
    # window where it has no changes.
    for chosen in (st.session_state.get("wiki"), st.query_params.get("wiki")):
        if chosen and chosen not in options:
            options.append(chosen)
    picked = st.selectbox(
        f"Wiki ({len(active):,} with changes)",
        options,
        format_func=lambda wiki: "All wikis" if wiki == ALL_WIKIS else wiki,
        key="wiki",
        bind="query-params",
    )
    return str(picked or ALL_WIKIS)


def _status(state: LakeState, end: datetime, ahead: int, window: Window, clock: datetime) -> None:
    behind = clock - (end + MINUTE)
    quiet = clock - state.last_write if state.last_write else timedelta(0)
    written = f"last write {state.last_write:%H:%M:%S}" if state.last_write else "no writes"
    upto = f"Complete minutes up to {window.end:%Y-%m-%d %H:%M} UTC · {written}"
    if quiet > STALLED_AFTER:
        badge = ":red-badge[:material/error: Stalled]"
        line = f"No write for {duration(quiet)}: is the job running? `make stream-logs`"
    elif behind > LIVE_LAG:
        badge = ":orange-badge[:material/history: Catching up]"
        line = f"The newest counts are {duration(behind)} behind the clock."
        if ahead:
            line += (
                f" {ahead:,} straggling {'change is' if ahead == 1 else 'changes are'} "
                "counted ahead of them: live changes come mixed into a replay."
            )
    else:
        badge = ":green-badge[:material/check_circle: Live]"
        line = ""
    st.markdown(" ".join(part for part in (badge, line, upto) if part))


def _tiles(totals: Totals, window: Window, detail: Window) -> None:
    editors = "Editors" if detail == window else f"Editors, {_last(DETAIL_SPAN)}"
    tiles = [
        (
            "Changes per minute",
            compact(totals.per_minute),
            f"Average over the {totals.minutes:,} complete minutes in the window.",
        ),
        ("Edits", compact(totals.edits), "Changes to existing pages."),
        ("New pages", compact(totals.new_pages), "Pages created, in any namespace."),
        ("Bot share", percent(totals.bot_share), "Changes made by accounts flagged as bots."),
        (editors, compact(totals.editors), "Distinct user names."),
        ("Content added", signed_bytes(totals.net_bytes), "Bytes added minus bytes removed."),
    ]
    for column, (label, value, help_text) in zip(st.columns(len(tiles)), tiles, strict=True):
        column.metric(label, value, help=help_text, border=True)


def _pages_table(pages: pd.DataFrame) -> None:
    st.dataframe(
        pages,
        hide_index=True,
        width="stretch",
        column_order=["wiki", "title", "editors", "edits", "url"],
        column_config={
            "wiki": st.column_config.TextColumn("Wiki"),
            "title": st.column_config.TextColumn("Page"),
            "edits": st.column_config.NumberColumn("Edits", format="%d"),
            "editors": st.column_config.NumberColumn("Editors", format="%d"),
            "url": st.column_config.LinkColumn("Link", display_text="Open"),
        },
    )


def _articles_table(articles: pd.DataFrame) -> None:
    st.dataframe(
        articles,
        hide_index=True,
        width="stretch",
        column_order=["time", "wiki", "title", "bytes", "url"],
        column_config={
            "time": st.column_config.DatetimeColumn("Time (UTC)", format="HH:mm:ss"),
            "wiki": st.column_config.TextColumn("Wiki"),
            "title": st.column_config.TextColumn("Article"),
            "bytes": st.column_config.NumberColumn("Size (bytes)", format="localized"),
            "url": st.column_config.LinkColumn("Link", display_text="Open"),
        },
    )


def _last(span: timedelta) -> str:
    return "last hour" if span == timedelta(hours=1) else f"last {duration(span)}"


def _reason(exc: Exception) -> str:
    text = str(exc).strip().splitlines()
    return f"{type(exc).__name__}: {text[0]}" if text else type(exc).__name__


# --- entry point ----------------------------------------------------------------


@st.cache_resource(ttl=timedelta(minutes=10), show_spinner=False)
def _connect(_settings: DashboardSettings, uri: str, namespace: str) -> Lake:
    # Reconnecting every 10 minutes also renews the catalog's OAuth token.
    return Lake(rest_catalog(_settings), namespace)


def main() -> None:
    settings = DashboardSettings()
    uri, namespace = settings.lake_catalog_uri, settings.lake_namespace
    render(
        lambda: _connect(settings, uri, namespace),
        refresh_seconds=settings.dashboard_refresh_seconds,
        where=uri,
        reconnect=_connect.clear,
    )


if __name__ == "__main__":
    main()
