"""A local SparkSession and the recorded events in the shape the job decodes.

The Kafka, Avro and Iceberg connectors are not on this classpath: these tests
cover the logic after decoding. The end-to-end test runs the real thing.
"""

from __future__ import annotations

import os
import shutil
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession

from tests.streaming.frames import decoded_rows
from wikistream.sse import SseEvent


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    here = Path(__file__).parent
    for item in items:
        if here in item.path.parents:
            item.add_marker(pytest.mark.spark)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    if not (shutil.which("java") or os.environ.get("JAVA_HOME")):
        pytest.fail(
            "The Spark tests need Java 17 or 21. On Ubuntu/WSL: "
            "sudo apt install -y openjdk-21-jre-headless",
            pytrace=False,
        )
    # Spark runs in UTC; make Python's view of naive timestamps agree.
    os.environ["TZ"] = "UTC"
    time.tzset()
    # Spark starts Python workers with `python3` from PATH unless told
    # otherwise; they must be this interpreter, or versions can mismatch.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[2]")
        .appName("wikistream-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def decoded(recorded: list[SseEvent]) -> list[dict[str, Any]]:
    return decoded_rows(recorded)
