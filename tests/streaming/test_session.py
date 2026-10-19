"""Spark configuration, checked without starting Spark where possible."""

from __future__ import annotations

from typing import Any

from pydantic import SecretStr
from pyspark.sql import SparkSession

from wikistream.streaming.session import (
    ICEBERG_VERSION,
    PACKAGES,
    QUIET_LOGGERS,
    SPARK_VERSION,
    quiet_loggers,
    spark_conf,
)
from wikistream.streaming.settings import LakeSettings


def settings(**overrides: Any) -> LakeSettings:
    return LakeSettings(_env_file=None, **overrides)  # type: ignore[call-arg]


def test_the_catalog_points_at_seaweedfs_with_oauth_and_path_style_s3() -> None:
    conf = spark_conf(
        settings(
            lake_catalog_uri="http://seaweedfs:8181",
            lake_s3_endpoint="http://seaweedfs:8333",
            lake_access_key="key",
            lake_secret_key=SecretStr("secret"),
        )
    )
    c = "spark.sql.catalog.lake"
    assert conf[c] == "org.apache.iceberg.spark.SparkCatalog"
    assert conf[f"{c}.type"] == "rest"
    assert conf[f"{c}.uri"] == "http://seaweedfs:8181"
    assert conf[f"{c}.warehouse"] == "s3://lake"
    assert conf[f"{c}.credential"] == "key:secret"
    assert conf[f"{c}.oauth2-server-uri"] == "http://seaweedfs:8181/v1/oauth/tokens"
    assert conf[f"{c}.s3.endpoint"] == "http://seaweedfs:8333"
    assert conf[f"{c}.s3.path-style-access"] == "true"
    assert conf[f"{c}.s3.access-key-id"] == "key"
    assert conf[f"{c}.s3.secret-access-key"] == "secret"
    assert conf["spark.sql.defaultCatalog"] == "lake"


def test_connectors_match_the_spark_and_iceberg_versions() -> None:
    import pyspark

    assert pyspark.__version__ == SPARK_VERSION
    packages = spark_conf(settings())["spark.jars.packages"].split(",")
    assert packages == list(PACKAGES)
    assert all(p.endswith((f":{SPARK_VERSION}", f":{ICEBERG_VERSION}")) for p in packages)
    assert any("iceberg-spark-runtime-4.0_2.13" in p for p in packages)


def test_batches_read_fresh_table_state_with_few_shuffle_partitions() -> None:
    conf = spark_conf(settings())
    # Each batch must see what the previous one committed.
    assert conf["spark.sql.catalog.lake.cache-enabled"] == "false"
    assert conf["spark.sql.shuffle.partitions"] == "4"
    assert conf["spark.sql.session.timeZone"] == "UTC"


def test_ivy_cache_only_when_configured() -> None:
    assert "spark.jars.ivy" not in spark_conf(settings())
    assert spark_conf(settings(spark_jars_ivy="/opt/ivy"))["spark.jars.ivy"] == "/opt/ivy"


def test_the_secret_does_not_leak_into_settings_repr() -> None:
    assert "wikistream-local-secret" not in repr(settings())


def test_data_loss_fails_the_query_unless_accepted_explicitly() -> None:
    from unittest.mock import MagicMock

    from wikistream.streaming.job import read_kafka

    for value, expected in ((True, "true"), (False, "false")):
        spark = MagicMock()
        reader = spark.readStream.format.return_value
        reader.option.return_value = reader
        read_kafka(spark, settings(fail_on_data_loss=value))
        options = dict(call.args for call in reader.option.call_args_list)
        assert options["failOnDataLoss"] == expected
        assert options["startingOffsets"] == "earliest"
        assert options["subscribe"] == "wiki.recentchange"


def test_the_per_batch_planner_warning_is_silenced(spark: SparkSession) -> None:
    jvm = spark.sparkContext._jvm
    assert jvm is not None
    manager = jvm.org.apache.logging.log4j.LogManager

    def level(name: str) -> str:
        return str(manager.getLogger(name).getLevel().toString())

    spark.sparkContext.setLogLevel("WARN")  # as in production
    try:
        quiet_loggers(spark)
        assert all(level(name) == "ERROR" for name in QUIET_LOGGERS)
        assert level("org.apache.spark.sql.execution") == "WARN", "only that logger"
    finally:
        spark.sparkContext.setLogLevel("ERROR")
