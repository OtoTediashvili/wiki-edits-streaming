"""The resume position: the Last-Event-ID of the newest event Redpanda has confirmed.

Saved only after the broker acknowledges every event up to it, so after a
crash the producer resumes from a point it knows was delivered. Events after
that point are fetched again: at-least-once delivery, never at-most-once. The
stream job removes the duplicates.
"""

from __future__ import annotations

from pathlib import Path


class ResumePosition:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> str | None:
        try:
            value = self.path.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            return None
        return value or None

    def save(self, event_id: str) -> None:
        """Atomic: write a temp file, then rename over the old one. A crash
        mid-write leaves the previous position, never a truncated one."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + ".tmp")
        tmp.write_text(event_id, encoding="utf-8")
        tmp.replace(self.path)
