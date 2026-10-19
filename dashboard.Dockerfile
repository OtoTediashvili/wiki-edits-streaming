# syntax=docker/dockerfile:1
#
# The dashboard image: Streamlit, PyIceberg and DuckDB reading the Iceberg
# tables through the REST catalog. No Spark and no Java: the `dashboard`
# dependency group only.

FROM python:3.12-slim AS build

# Same uv as the other images and CI.
COPY --from=ghcr.io/astral-sh/uv:0.12.24 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Runtime dependencies plus the `dashboard` group, from the lockfile.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-default-groups --group dashboard --no-install-project

COPY src ./src
RUN uv sync --locked --no-default-groups --group dashboard --no-editable


FROM python:3.12-slim

RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin app

COPY --from=build /app/.venv /app/.venv

# Inside the container Streamlit listens on every interface; compose
# publishes the port on 127.0.0.1 only.
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_PORT=8501

USER app
WORKDIR /home/app
EXPOSE 8501

ENTRYPOINT ["python", "-m", "wikistream.dashboard"]
