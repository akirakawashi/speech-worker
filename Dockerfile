# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.11.18 AS uv

FROM python:3.12-slim
COPY --from=uv /uv /usr/local/bin/uv

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    SPEECH_WORKER_TEMP_DIR=/var/tmp/speech-worker \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

RUN apt-get update \
    && apt-get install --no-install-recommends -y ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 worker \
    && useradd --system --uid 10001 --gid worker --no-create-home \
       --shell /usr/sbin/nologin worker \
    && mkdir -p /var/tmp/speech-worker \
    && chown 10001:10001 /var/tmp/speech-worker

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project

COPY README.md ./
COPY src ./src

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable
RUN uv pip check \
    && speech-worker --help \
    && speech-sync-directories --help \
    && speech-prune --help \
    && ffmpeg -version > /dev/null \
    && ffprobe -version > /dev/null

USER 10001:10001
CMD ["speech-worker", "--lanes", "daily,backfill"]
