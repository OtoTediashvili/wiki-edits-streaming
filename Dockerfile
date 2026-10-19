# syntax=docker/dockerfile:1
#
# The producer image. Two stages: uv builds the virtualenv from the lockfile,
# and only that venv is copied into a clean runtime image. No compiler, no uv,
# no source tree in what runs.

FROM python:3.12-slim AS build

# The same uv version CI uses (.github/workflows/ci.yml). Pinned, so a uv
# release can't change how the lockfile resolves inside the image.
COPY --from=ghcr.io/astral-sh/uv:0.12.24 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Dependencies first, in their own layer: editing code doesn't reinstall them.
# --no-default-groups: only the runtime dependencies, without the dev tools
# and without PySpark, which is the Spark image's business (spark.Dockerfile).
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-default-groups --no-install-project

# Then the project itself, as a regular (non-editable) install, so the runtime
# stage needs nothing but the venv.
COPY src ./src
RUN uv sync --locked --no-default-groups --no-editable


FROM python:3.12-slim

# A fixed non-root user. /state is created here, owned by that user, so a
# named volume mounted on it starts out writable.
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app \
    && mkdir /state && chown app /state

COPY --from=build /app/.venv /app/.venv

ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    WIKISTREAM_STATE_PATH=/state/last_event_id \
    WIKISTREAM_LOG_JSON=true

USER app
WORKDIR /home/app

ENTRYPOINT ["wikistream"]
CMD ["live"]
