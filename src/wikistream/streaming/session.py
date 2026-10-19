"""The SparkSession: connectors, the Iceberg catalog, streaming state.

Everything is in `spark_conf`, a plain dict, so the configuration is tested
without starting Spark.
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from wikistream.streaming.settings import CATALOG, LakeSettings

SPARK_VERSION = "4.0.4"  # must match pyspark in pyproject.toml
SCALA_VERSION = "2.13"
ICEBERG_VERSION = "1.11.0"

# Resolved from Maven Central with their dependencies (Spark's --packages).
PACKAGES = (
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}",
    f"org.apache.spark:spark-avro_{SCALA_VERSION}:{SPARK_VERSION}",
    f"org.apache.iceberg:iceberg-spark-runtime-4.0_{SCALA_VERSION}:{ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
)


def spark_conf(settings: LakeSettings) -> dict[str, str]:
    catalog = f"spark.sql.catalog.{CATALOG}"
    secret = settings.lake_secret_key.get_secret_value()
    conf = {
        "spark.app.name": "wikistream",
        "spark.master": "local[2]",
        "spark.driver.memory": settings.spark_driver_memory,
        "spark.jars.packages": ",".join(PACKAGES),
        "spark.sql.session.timeZone": "UTC",
        "spark.ui.showConsoleProgress": "false",
        # --- Iceberg, through SeaweedFS's REST catalog --------------------
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": CATALOG,
        catalog: "org.apache.iceberg.spark.SparkCatalog",
        f"{catalog}.type": "rest",
        f"{catalog}.uri": settings.lake_catalog_uri,
        f"{catalog}.warehouse": settings.lake_warehouse,
        # OAuth2 client credentials: the catalog swaps the S3 key pair for a
        # bearer token, and refreshes it.
        f"{catalog}.rest.auth.type": "oauth2",
        f"{catalog}.credential": f"{settings.lake_access_key}:{secret}",
        f"{catalog}.oauth2-server-uri": f"{settings.lake_catalog_uri}/v1/oauth/tokens",
        # Data and metadata files go straight to S3.
        f"{catalog}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{catalog}.s3.endpoint": settings.lake_s3_endpoint,
        f"{catalog}.s3.path-style-access": "true",
        f"{catalog}.s3.access-key-id": settings.lake_access_key,
        f"{catalog}.s3.secret-access-key": secret,
        f"{catalog}.client.region": settings.lake_region,
        # Each batch reads what the previous one wrote; no cached table state.
        f"{catalog}.cache-enabled": "false",
        # --- batches ----------------------------------------------------------
        # The default 200 shuffle partitions are for a cluster; a batch here
        # is at most a few hundred thousand rows.
        "spark.sql.shuffle.partitions": "4",
    }
    if settings.spark_jars_ivy:
        conf["spark.jars.ivy"] = settings.spark_jars_ivy
    return conf


# Planning each batch's MERGE logs "Can't translate true to source filter" at
# WARN: a condition of `true` (no filter at all) has no source-filter form.
# Harmless, and one line per batch drowns the useful ones.
QUIET_LOGGERS = ("org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy",)


def quiet_loggers(spark: SparkSession) -> None:
    jvm = spark.sparkContext._jvm
    assert jvm is not None, "no JVM: the session has stopped"
    log4j = jvm.org.apache.logging.log4j
    for name in QUIET_LOGGERS:
        log4j.core.config.Configurator.setLevel(name, log4j.Level.ERROR)


def spark_session(settings: LakeSettings) -> SparkSession:
    # Any Python worker Spark starts must be this interpreter, not whatever
    # `python3` comes first on PATH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = SparkSession.builder
    for key, value in spark_conf(settings).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    quiet_loggers(spark)
    return spark
