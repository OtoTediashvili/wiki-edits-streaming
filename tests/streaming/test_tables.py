"""The DDL: what the tables look like, and that Spark's parser accepts it."""

from __future__ import annotations

from typing import cast

import pytest
from pyspark.sql import SparkSession
from structlog.testing import capture_logs

from wikistream.streaming.settings import LakeSettings
from wikistream.streaming.tables import TABLE_PROPERTIES, create_table_sql, ddl, ensure_tables
from wikistream.streaming.transform import (
    CHANGE_COLUMNS,
    MINUTE_COLUMNS,
    REJECTED_COLUMNS,
    ColumnSpec,
)


@pytest.fixture
def statements() -> list[str]:
    return ddl(LakeSettings(_env_file=None, lake_namespace="wiki"))  # type: ignore[call-arg]


def test_namespace_then_the_tables(statements: list[str]) -> None:
    namespace, changes, minutes, rejected = statements
    assert namespace == "CREATE NAMESPACE IF NOT EXISTS lake.wiki"
    assert changes.startswith("CREATE TABLE IF NOT EXISTS lake.wiki.changes (")
    assert "PARTITIONED BY (days(event_time))" in changes
    assert minutes.startswith("CREATE TABLE IF NOT EXISTS lake.wiki.edits_per_minute (")
    assert "PARTITIONED BY (days(window_start))" in minutes
    assert rejected.startswith("CREATE TABLE IF NOT EXISTS lake.wiki.rejected (")
    assert "PARTITIONED BY (days(kafka_timestamp))" in rejected
    assert "`value` BINARY COMMENT" in rejected


def test_columns_come_from_the_specs_in_order(statements: list[str]) -> None:
    _, changes, minutes, rejected = statements
    for sql, columns in (
        (changes, CHANGE_COLUMNS),
        (minutes, MINUTE_COLUMNS),
        (rejected, REJECTED_COLUMNS),
    ):
        positions = [sql.index(f"`{c.name}` {c.sql_type} COMMENT") for c in columns]
        assert positions == sorted(positions)
    for key, value in TABLE_PROPERTIES.items():
        assert f"'{key}' = '{value}'" in changes


def test_spark_parses_every_statement(spark: SparkSession, statements: list[str]) -> None:
    parser = spark._jsparkSession.sessionState().sqlParser()
    for sql in statements:
        parser.parsePlan(sql)  # raises ParseException on bad syntax


def test_quotes_in_comments_are_escaped(spark: SparkSession) -> None:
    sql = create_table_sql(
        "lake.t.t",
        (ColumnSpec("x", "STRING", "x"), ColumnSpec("a", "TIMESTAMP", "it's a \\ test")),
        "days(a)",
        "o'clock",
    )
    spark._jsparkSession.sessionState().sqlParser().parsePlan(sql)


# How Py4J reports a Java exception: a useless first line, then the cause.
CONNECTION_REFUSED = (
    "An error occurred while calling o76.sql.\n"
    ": org.apache.iceberg.exceptions.RESTException: Error occurred while processing POST "
    "request: java.net.ConnectException: Connection refused\n\tat org.apache..."
)
BAD_REQUEST = (
    "An error occurred while calling o76.sql.\n"
    ": org.apache.iceberg.exceptions.BadRequestException: Malformed request: "
    "Invalid request body\n\tat org.apache.iceberg.rest.ErrorHandlers..."
)


class FlakyCatalog:
    """A SparkSession stand-in whose catalog fails the first few calls."""

    def __init__(self, failures: int, error: str = CONNECTION_REFUSED) -> None:
        self.failures = failures
        self.error = error
        self.statements: list[str] = []
        self.calls = 0

    def sql(self, statement: str) -> None:
        self.calls += 1
        if self.failures:
            self.failures -= 1
            raise RuntimeError(self.error)
        self.statements.append(statement)


def test_tables_are_created_once_the_catalog_answers(statements: list[str]) -> None:
    spark = FlakyCatalog(failures=2)
    settings = LakeSettings(_env_file=None, lake_namespace="wiki")  # type: ignore[call-arg]
    ensure_tables(cast(SparkSession, spark), settings, wait_seconds=0)
    assert spark.statements == statements


def test_a_catalog_that_never_answers_fails_the_start() -> None:
    spark = FlakyCatalog(failures=99)
    settings = LakeSettings(_env_file=None)  # type: ignore[call-arg]
    with pytest.raises(RuntimeError, match="Connection refused"):
        ensure_tables(cast(SparkSession, spark), settings, attempts=3, wait_seconds=0)


def test_a_refused_request_fails_at_once_with_the_java_cause() -> None:
    spark = FlakyCatalog(failures=99, error=BAD_REQUEST)
    settings = LakeSettings(_env_file=None)  # type: ignore[call-arg]
    with capture_logs() as logs, pytest.raises(RuntimeError):
        ensure_tables(cast(SparkSession, spark), settings, attempts=5, wait_seconds=0)
    assert spark.calls == 1
    assert logs[-1]["error"] == (
        "org.apache.iceberg.exceptions.BadRequestException: Malformed request: Invalid request body"
    )


def test_no_table_is_partitioned_by_its_first_column() -> None:
    """SeaweedFS's catalog rejects a partition on column id 0, the id Spark
    gives the first column. Found by capturing Spark's create request."""
    for columns, partition_column in (
        (CHANGE_COLUMNS, "event_time"),
        (MINUTE_COLUMNS, "window_start"),
        (REJECTED_COLUMNS, "kafka_timestamp"),
    ):
        assert columns[0].name != partition_column


def test_partitioning_by_the_first_column_is_refused_up_front() -> None:
    columns = (ColumnSpec("ts", "TIMESTAMP", "t"), ColumnSpec("x", "STRING", "x"))
    with pytest.raises(ValueError, match="first column"):
        create_table_sql("lake.t.t", columns, "days(ts)", "c")
    with pytest.raises(ValueError, match="transform of one column"):
        create_table_sql("lake.t.t", columns, "ts", "c")
    assert "PARTITIONED BY (days(ts))" in create_table_sql(
        "lake.t.t", columns[::-1], "days(ts)", "c"
    )
