from __future__ import annotations

from pathlib import Path

from wikistream.state import ResumePosition


def test_no_position_yet(tmp_path: Path) -> None:
    assert ResumePosition(tmp_path / "missing").load() is None


def test_round_trip_and_overwrite(tmp_path: Path) -> None:
    position = ResumePosition(tmp_path / "state" / "last_event_id")
    position.save('[{"topic":"a","partition":0,"timestamp":1}]')
    position.save('[{"topic":"a","partition":0,"timestamp":2}]')
    assert position.load() == '[{"topic":"a","partition":0,"timestamp":2}]'
    assert [p.name for p in position.path.parent.iterdir()] == ["last_event_id"]


def test_an_empty_file_means_no_position(tmp_path: Path) -> None:
    path = tmp_path / "last_event_id"
    path.write_text("\n")
    assert ResumePosition(path).load() is None
