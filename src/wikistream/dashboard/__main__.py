"""Serve the dashboard: `python -m wikistream.dashboard [streamlit options]`.

Wraps `streamlit run` so the container and `make dashboard-dev` start the same
app the same way. Extra arguments go to Streamlit, for example
`--server.address 127.0.0.1`.
"""

from __future__ import annotations

import sys
from pathlib import Path

from streamlit.web import cli

APP = Path(__file__).with_name("app.py")

OPTIONS = (
    "--server.headless=true",  # don't try to open a browser
    "--browser.gatherUsageStats=false",  # no telemetry to Streamlit
    "--client.toolbarMode=viewer",  # no developer menu or Deploy button
    # Controls in the blue of "Humans". Set per mode: a top-level theme
    # option would pin the page to light and ignore the viewer's dark mode.
    "--theme.light.primaryColor=#2a78d6",
    "--theme.dark.primaryColor=#3987e5",
)


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    sys.argv = ["streamlit", "run", str(APP), *OPTIONS, *args]
    cli.main()


if __name__ == "__main__":
    main()
