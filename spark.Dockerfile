# syntax=docker/dockerfile:1
#
# The stream job image: Python 3.12, PySpark, a Java 21 runtime, and the
# Kafka, Avro and Iceberg connector jars downloaded at build time. Spark runs
# in local mode inside this one container: ~30 events/s needs no cluster.

FROM python:3.12-slim-trixie AS build

# Same uv as the producer image and CI.
COPY --from=ghcr.io/astral-sh/uv:0.12.24 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Runtime dependencies plus the `spark` group (PySpark), from the lockfile.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-default-groups --group spark --no-install-project

COPY src ./src
RUN uv sync --locked --no-default-groups --group spark --no-editable


FROM python:3.12-slim-trixie

# Spark 4.0 runs on Java 17 or 21; Debian 13 (trixie, pinned above) ships 21.
# procps provides `ps`, which Spark's launch scripts call.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin app \
    && mkdir /checkpoints /opt/ivy \
    && chown app /checkpoints /opt/ivy

COPY --from=build /app/.venv /app/.venv

ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    WIKISTREAM_SPARK_JARS_IVY=/opt/ivy \
    WIKISTREAM_CHECKPOINT_DIR=/checkpoints \
    WIKISTREAM_LOG_JSON=true

USER app
WORKDIR /home/app

# Resolve the connector jars (session.PACKAGES) and their dependencies from
# Maven Central now, into /opt/ivy. Containers then start from that cache:
# the same jars every time, and no download on the first start.
RUN python -m wikistream.streaming fetch-jars

ENTRYPOINT ["python", "-m", "wikistream.streaming"]
CMD ["run"]
