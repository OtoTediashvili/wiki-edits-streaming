"""The page itself, run headless with Streamlit's AppTest over a local lake.

The data is placed relative to the real clock, because the lake stamps each
commit with it: "last write" is always the time the test wrote.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pyiceberg.catalog import Catalog
from streamlit.testing.v1 import AppTest

from tests.dashboard.lakes import (
    RECONNECTS,
    append_changes,
    append_minutes,
    catalog,
    change,
    create_tables,
    minute,
)
from wikistream.dashboard import __main__ as launcher
from wikistream.dashboard.app import PALETTES, minute_chart, wikis_chart
from wikistream.dashboard.queries import Numbers

NOW = datetime.now(UTC).replace(tzinfo=None, second=0, microsecond=0)


def page(folder: str, clock: str, fail: bool = False) -> None:
    """The script AppTest runs: the real page on a local lake."""
    from datetime import datetime
    from pathlib import Path

    from tests.dashboard.lakes import RECONNECTS, open_lake
    from wikistream.dashboard.app import render
    from wikistream.dashboard.lake import Lake

    def connect() -> Lake:
        if fail:
            raise ConnectionError("Connection refused\nmore detail")
        return open_lake(Path(folder))

    render(
        connect,
        refresh_seconds=None,
        now=lambda: datetime.fromisoformat(clock),
        where="http://seaweedfs:8181",
        reconnect=lambda: RECONNECTS.append(folder),
    )


def run(
    folder: Path, clock: datetime, query: dict[str, str] | None = None, **kwargs: Any
) -> AppTest:
    app = AppTest.from_function(
        page,
        kwargs={"folder": str(folder), "clock": clock.isoformat(), **kwargs},
        default_timeout=30,
    )
    for key, value in (query or {}).items():
        app.query_params[key] = value
    app.run()
    assert not app.exception, app.exception
    return app


def fill(cat: Catalog, newest: datetime, minutes: int = 90) -> None:
    """`minutes` of traffic up to `newest`: enwiki all people, commons mostly bots."""
    rows = []
    for m in range(minutes):
        start = newest - timedelta(minutes=m)
        rows.append(minute("enwiki", start, humans=30, bots=10, new_pages=1, bytes_added=3_000))
        rows.append(minute("commonswiki", start, humans=10, bots=90, bytes_removed=500))
    append_minutes(cat, rows)
    when = newest - timedelta(minutes=5)
    append_changes(
        cat,
        [
            change(when, title="Ada Lovelace", user="Alice"),
            change(when, title="Ada Lovelace", user="Bob"),
            change(when, title="Brand new article", type="new", user="Carol"),
            change(
                when, title="Q42", wiki="wikidatawiki", bot=True, server_name="www.wikidata.org"
            ),
        ],
    )


def text(app: AppTest) -> str:
    parts = [m.value for m in app.markdown] + [c.value for c in app.caption]
    parts += [i.value for i in app.info] + [e.value for e in app.error]
    return "\n".join(str(p) for p in parts)


def metrics(app: AppTest) -> dict[str, str]:
    return {m.label: m.value for m in app.metric}


@pytest.fixture
def cat(tmp_path: Path) -> Catalog:
    return create_tables(tmp_path)


def test_live_page_with_numbers_charts_and_tables(tmp_path: Path, cat: Catalog) -> None:
    fill(cat, newest=NOW - timedelta(minutes=1))
    app = run(tmp_path, clock=NOW + timedelta(seconds=20))

    assert "Live" in app.markdown[0].value
    assert f"up to {NOW - timedelta(minutes=1):%Y-%m-%d %H:%M} UTC" in app.markdown[0].value
    # The default window: the hour before the newest minute, which is still filling.
    assert metrics(app) == {
        "Changes per minute": "140",
        "Edits": "8,340",  # 60 x 140, less 60 new pages
        "New pages": "60",
        "Bot share": "71%",
        "Editors": "3",
        "Content added": "+150 KB",  # 60 x (3,000 - 500)
    }
    assert [h.value for h in app.subheader] == [
        "Changes per minute",
        "Busiest wikis",
        "Most edited pages",
        "Newest articles",
    ]
    pages, articles = app.dataframe[0].value, app.dataframe[1].value
    assert "Edits by people, last hour. Bots are left out." in text(app)
    assert list(pages["title"]) == ["Ada Lovelace", "Brand new article"]
    assert list(pages["editors"]) == [2, 1]
    assert list(articles["url"]) == ["https://en.wikipedia.org/wiki/Brand_new_article"]


def test_a_shorter_window_from_the_url(tmp_path: Path, cat: Catalog) -> None:
    fill(cat, newest=NOW - timedelta(minutes=1))
    app = run(tmp_path, clock=NOW, query={"window": "15m"})
    assert metrics(app)["Edits"] == "2,085"  # 15 x 140 - 15
    assert "last 15 min" in text(app)
    assert "15 complete minutes" in app.metric[0].help


def test_a_long_window_reads_single_changes_for_the_last_hour_only(
    tmp_path: Path, cat: Catalog
) -> None:
    fill(cat, newest=NOW - timedelta(minutes=1), minutes=90)
    app = run(tmp_path, clock=NOW, query={"window": "6h"})
    # The lake holds 89 complete minutes of the 6 hours: the rate is per minute held.
    assert metrics(app)["Changes per minute"] == "140"
    assert metrics(app)["Edits"] == "12.4K"  # 89 x 140 - 89
    assert metrics(app)["Editors, last hour"] == "3"


def test_one_wiki(tmp_path: Path, cat: Catalog) -> None:
    fill(cat, newest=NOW - timedelta(minutes=1))
    app = run(tmp_path, clock=NOW)
    app.selectbox(key="wiki").select("commonswiki").run()
    assert not app.exception
    assert metrics(app)["Bot share"] == "90%"
    assert metrics(app)["Changes per minute"] == "100"
    # No one-bar chart of the busiest wikis when there's one wiki.
    assert "Busiest wikis" not in [h.value for h in app.subheader]
    assert app.session_state["wiki"] == "commonswiki"


def test_a_wiki_without_changes_stays_picked(tmp_path: Path, cat: Catalog) -> None:
    fill(cat, newest=NOW - timedelta(minutes=1))
    app = run(tmp_path, clock=NOW, query={"wiki": "tlhwiki"})
    assert app.selectbox(key="wiki").value == "tlhwiki"
    assert "No changes in this window." in text(app)


def test_catching_up_after_time_away(tmp_path: Path, cat: Catalog) -> None:
    newest = NOW - timedelta(hours=3)
    fill(cat, newest=newest)
    app = run(tmp_path, clock=NOW)
    status = app.markdown[0].value
    assert "Catching up" in status and "2 h 59 min behind" in status
    # The window follows the lake: it still shows the hour before its newest minute.
    assert metrics(app)["Changes per minute"] == "140"


def test_a_job_that_stopped_writing(tmp_path: Path, cat: Catalog) -> None:
    fill(cat, newest=NOW - timedelta(minutes=1))
    app = run(tmp_path, clock=NOW + timedelta(minutes=20))
    status = app.markdown[0].value
    assert "Stalled" in status and "make stream-logs" in status


def test_before_the_job_created_the_tables(tmp_path: Path) -> None:
    catalog(tmp_path)  # a catalog, nothing in it
    app = run(tmp_path, clock=NOW)
    assert "No tables yet" in text(app)
    assert not app.metric


def test_tables_without_rows_yet(tmp_path: Path, cat: Catalog) -> None:
    app = run(tmp_path, clock=NOW)
    assert "hold no counts yet" in text(app)


def test_an_unreachable_catalog_is_shown_and_reconnected(tmp_path: Path) -> None:
    RECONNECTS.clear()
    app = run(tmp_path, clock=NOW, fail=True)
    message = app.error[0].value
    assert "http://seaweedfs:8181" in message and "ConnectionError: Connection refused" in message
    assert "more detail" not in message
    assert [str(tmp_path)] == RECONNECTS


def test_charts_keep_people_and_bots_in_fixed_colors_in_both_themes() -> None:
    from tests.dashboard.lakes import read_back
    from wikistream.streaming.transform import CHANGE_COLUMNS, MINUTE_COLUMNS

    rows = [
        minute("enwiki", NOW, humans=30, bots=10),
        minute("commonswiki", NOW, humans=10, bots=90),
    ]
    numbers = Numbers(read_back(MINUTE_COLUMNS, rows), read_back(CHANGE_COLUMNS, []))
    for mode, colors in PALETTES.items():
        for chart in (
            minute_chart(numbers.per_minute(), colors),
            wikis_chart(numbers.top_wikis(), colors),
        ):
            spec = str(chart.to_dict())
            assert "'domain': ['Humans', 'Bots']" in spec, mode
            assert f"'range': ['{colors.humans}', '{colors.bots}']" in spec, mode
    # Minutes go to the browser as UTC instants and are drawn on a UTC scale.
    # Naive times were read as the viewer's local time: a Tbilisi browser
    # showed 06:18 for 10:18 UTC.
    drawn = minute_chart(numbers.per_minute(), PALETTES["light"]).to_dict()
    assert "'type': 'utc'" in str(drawn)
    [rows] = drawn["datasets"].values()
    assert [row["minute"] for row in rows] == [f"{NOW:%Y-%m-%dT%H:%M:%S}+00:00"]


def test_the_launcher_runs_this_app_with_fixed_options(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[list[str]] = []
    monkeypatch.setattr("streamlit.web.cli.main", lambda: seen.append(list(sys.argv)))
    launcher.main(["--server.address", "127.0.0.1"])
    [argv] = seen
    assert argv[:3] == ["streamlit", "run", str(launcher.APP)]
    assert launcher.APP.is_file()
    assert "--browser.gatherUsageStats=false" in argv
    assert argv[-2:] == ["--server.address", "127.0.0.1"]


def test_the_dashboard_runs_without_spark() -> None:
    """Its image has no PySpark: importing it must not need it."""
    code = "import sys; sys.modules['pyspark'] = None; import wikistream.dashboard.app"
    subprocess.run([sys.executable, "-c", code], check=True, capture_output=True)
