"""Settings for the stream job, from WIKISTREAM_* variables or .env.

The same variable names as the producer where they mean the same thing
(WIKISTREAM_BOOTSTRAP_SERVERS, WIKISTREAM_TOPIC), so one .env serves both.
Defaults point at the local stack from the host; docker-compose.yml overrides
the addresses for the container.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

CATALOG = "lake"
CHANGES = "changes"
EDITS_PER_MINUTE = "edits_per_minute"


class LakeSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", env_prefix="WIKISTREAM_", extra="ignore"
    )

    # --- source -----------------------------------------------------------
    bootstrap_servers: str = "localhost:19092"
    topic: str = "wiki.recentchange"
    # Caps one micro-batch. Live, a batch is a minute's worth (~2,000 events).
    # Catching up after time away, 200,000 a batch works through a day's
    # backlog (~2.8 million) in about 15 batches.
    max_offsets_per_trigger: int = Field(default=200_000, ge=1)
    # If retention deleted messages the job never read (it was stopped for
    # longer than 48 hours), stop and say so. Set to false for one restart to
    # accept the gap and carry on from the oldest message still there.
    fail_on_data_loss: bool = True

    # --- lake: SeaweedFS serves both the Iceberg REST catalog and S3 ---------
    lake_catalog_uri: str = "http://localhost:8181"
    lake_warehouse: str = "s3://lake"
    lake_s3_endpoint: str = "http://localhost:8333"
    lake_region: str = "us-east-1"
    # Local-stack credentials, the same defaults docker-compose.yml gives
    # SeaweedFS. Override both places together to change them.
    lake_access_key: str = "wikistream"
    lake_secret_key: SecretStr = SecretStr("wikistream-local-secret")
    lake_namespace: str = "wiki"

    # --- stream processing ------------------------------------------------
    checkpoint_dir: Path = Path("data/checkpoints")
    # How often a batch is written. Each batch is two Iceberg commits (a
    # snapshot each, plus a few small files); a minute keeps that modest.
    trigger_seconds: int = Field(default=60, ge=1)

    # --- Spark --------------------------------------------------------------
    spark_driver_memory: str = "1g"
    # Where the connector jars are cached. The Spark image downloads them at
    # build time into /opt/ivy, so containers start without the network.
    spark_jars_ivy: str | None = None

    # --- logging ------------------------------------------------------------
    log_level: str = "INFO"
    log_json: bool = False

    def table(self, name: str) -> str:
        return f"{CATALOG}.{self.lake_namespace}.{name}"
