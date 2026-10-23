"""Command line: python -m wikistream.drills NAME [--seconds N] [--kills N]."""

from __future__ import annotations

import argparse
import sys
import textwrap

from wikistream.drills.drills import DRILLS, Context, NotReadyError, run, save
from wikistream.streaming.settings import LakeSettings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m wikistream.drills",
        description="Break the running pipeline on purpose, then audit the lake.",
    )
    parser.add_argument("name", choices=[*DRILLS, "list"])
    parser.add_argument("--seconds", type=int, default=0, help="outage length (default per drill)")
    parser.add_argument("--kills", type=int, default=3, help="job-crash: how many kills")
    parser.add_argument("--timeout-minutes", type=float, default=20)
    args = parser.parse_args(argv)

    if args.name == "list":
        for name, drill in DRILLS.items():
            summary = textwrap.fill(" ".join((drill.__doc__ or "").split()), 76)
            print(f"{name}\n{textwrap.indent(summary, '    ')}\n")
        return 0

    # The lake settings carry the broker and topic too, under the same names
    # as the producer's (WIKISTREAM_BOOTSTRAP_SERVERS, WIKISTREAM_TOPIC).
    lake = LakeSettings()
    ctx = Context(
        lake=lake,
        bootstrap_servers=lake.bootstrap_servers,
        topic=lake.topic,
        seconds=args.seconds,
        kills=args.kills,
    )
    try:
        report = run(args.name, ctx, timeout_minutes=args.timeout_minutes)
    except NotReadyError as refused:
        print(f"Not drilling: {refused}", file=sys.stderr)
        return 2
    path = save(report)
    print(f"\nReport: {path}")
    return 0 if report.audit.ok else 1


if __name__ == "__main__":
    sys.exit(main())
