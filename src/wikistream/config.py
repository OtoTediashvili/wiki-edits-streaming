"""Typed settings, from environment variables (prefix WIKISTREAM_) or a .env file.

Nothing else reads os.environ. A missing or malformed setting fails at
startup, not an hour into a stream.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", env_prefix="WIKISTREAM_", extra="ignore"
    )

    # --- source -----------------------------------------------------------
    stream_url: str = "https://stream.wikimedia.org/v2/stream/recentchange"
    contact: str = Field(
        default="",
        description=(
            "How Wikimedia can reach the operator: a URL or email. Wikimedia's "
            "User-Agent policy requires one, so the live producer refuses to start without it."
        ),
    )
    connect_timeout_seconds: float = Field(default=10.0, gt=0)
    # The stream carries ~30 events a second; a minute of silence means the
    # connection is dead even if TCP has not noticed.
    read_timeout_seconds: float = Field(default=60.0, gt=0)
    max_backoff_seconds: float = Field(default=60.0, gt=0)

    # --- Redpanda ---------------------------------------------------------
    bootstrap_servers: str = "localhost:19092"
    schema_registry_url: str = "http://localhost:18081"
    topic: str = "wiki.recentchange"
    dlq_topic: str = "wiki.recentchange.dlq"
    partitions: int = Field(default=3, ge=1)
    retention_hours: int = Field(default=48, ge=1)
    # Per partition. A cap so a stalled consumer cannot fill the laptop's disk.
    retention_bytes: int = Field(default=1024**3, ge=1)

    # --- resume -----------------------------------------------------------
    state_path: Path = Path("data/state/last_event_id")
    # How often delivered events are confirmed and the resume position saved.
    # A crash replays at most this much, as duplicates the stream job removes.
    checkpoint_seconds: float = Field(default=5.0, gt=0)

    # --- logging ----------------------------------------------------------
    log_level: str = "INFO"
    log_json: bool = False

    @property
    def user_agent(self) -> str:
        return f"wiki-edits-streaming/0.1 (+{self.contact})"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
