"""The Iceberg tables, created on start if missing.

Explicit DDL rather than letting the first write create them: partitioning,
column comments and table properties are part of the design, not defaults.
"""

from __future__ import annotations

import re
import time

from pyspark.sql import SparkSession

from wikistream.logging_config import get_logger
from wikistream.streaming.settings import (
    CATALOG,
    CHANGES,
    EDITS_PER_MINUTE,
    REJECTED,
    LakeSettings,
)
from wikistream.streaming.transform import (
    CHANGE_COLUMNS,
    MINUTE_COLUMNS,
    REJECTED_COLUMNS,
    ColumnSpec,
)

log = get_logger(__name__)

# Format v2: read by every current engine, DuckDB included. One commit a
# minute writes a metadata file each time; keep the last 100 and delete
# older ones as part of each commit instead of letting them pile up.
TABLE_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
}


def sql_string(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def create_table_sql(
    table: str, columns: tuple[ColumnSpec, ...], partition_by: str, comment: str
) -> str:
    """CREATE TABLE for Iceberg, partitioned by a transform of one column.

    The partition column can't be the first column. Spark numbers the columns
    of a new table from 0 in its create request, and SeaweedFS's catalog
    (iceberg-go) rejects a partition whose source column id is 0 with "400
    Invalid request body". Other catalogs renumber and don't mind; this one
    needs the partition column at position 1 or later.
    """
    source = re.fullmatch(r"\w+\((\w+)\)", partition_by)
    if source is None:
        raise ValueError(f"expected a transform of one column, like days(ts): {partition_by!r}")
    if columns[0].name == source.group(1):
        raise ValueError(
            f"{table}: partition column {source.group(1)!r} is the first column; "
            "SeaweedFS's catalog rejects that (source id 0). Put another column first."
        )
    cols = ",\n  ".join(f"`{c.name}` {c.sql_type} COMMENT {sql_string(c.comment)}" for c in columns)
    props = ",\n  ".join(f"{sql_string(k)} = {sql_string(v)}" for k, v in TABLE_PROPERTIES.items())
    return (
        f"CREATE TABLE IF NOT EXISTS {table} (\n  {cols}\n)\n"
        f"USING iceberg\n"
        f"PARTITIONED BY ({partition_by})\n"
        f"COMMENT {sql_string(comment)}\n"
        f"TBLPROPERTIES (\n  {props}\n)"
    )


def ddl(settings: LakeSettings) -> list[str]:
    return [
        f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{settings.lake_namespace}",
        create_table_sql(
            settings.table(CHANGES),
            CHANGE_COLUMNS,
            # A day holds ~2.6 million changes: one partition, a few files
            # after compaction. Finer partitions would only make small files.
            "days(event_time)",
            "Every change on every Wikimedia wiki, each event once",
        ),
        create_table_sql(
            settings.table(EDITS_PER_MINUTE),
            MINUTE_COLUMNS,
            "days(window_start)",
            "Changes per wiki per minute of event time; final once written",
        ),
        create_table_sql(
            settings.table(REJECTED),
            REJECTED_COLUMNS,
            "days(kafka_timestamp)",
            "Messages from the topic that didn't decode, set aside with the reason",
        ),
    ]


# Signs that the catalog isn't reachable yet, as opposed to a request it
# understood and refused. Only these are worth waiting out.
_NOT_READY = (
    "ConnectException",
    "Connection refused",
    "UnknownHost",
    "ServiceUnavailable",
    "ServiceFailure",
    "SocketTimeout",
    "timed out",
)


def java_cause(exc: Exception) -> str:
    """The Java exception behind a Py4J error, in one line: py4j's own first
    line ("An error occurred while calling o76.sql.") says nothing."""
    lines = [line.strip() for line in str(exc).splitlines() if line.strip()]
    for line in lines:
        if "Exception" in line or "Error:" in line:
            return line.removeprefix(": ")
    return lines[0] if lines else type(exc).__name__


def ensure_tables(
    spark: SparkSession, settings: LakeSettings, attempts: int = 6, wait_seconds: float = 5.0
) -> None:
    """Create the namespace and tables if missing.

    Waits for a catalog that is still starting (connection refused, 503), but
    not for one that refused the request: retrying a 400 gets another 400.
    """
    for attempt in range(1, attempts + 1):
        try:
            for statement in ddl(settings):
                spark.sql(statement)
        except Exception as exc:
            cause = java_cause(exc)
            if attempt == attempts or not any(sign in cause for sign in _NOT_READY):
                log.error("tables.failed", attempt=attempt, error=cause)
                raise
            log.warning("tables.retry", attempt=attempt, error=cause)
            time.sleep(wait_seconds)
        else:
            log.info("tables.ready", namespace=settings.lake_namespace)
            return
