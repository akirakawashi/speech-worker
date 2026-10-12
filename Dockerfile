# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPEECH_WORKER_TEMP_DIR=/var/tmp/speech-worker

WORKDIR /app

RUN apt-get update \
    && apt-get install --no-install-recommends -y ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 worker \
    && useradd --system --uid 10001 --gid worker --no-create-home \
       --shell /usr/sbin/nologin worker \
    && mkdir -p /var/tmp/speech-worker \
    && chown 10001:10001 /var/tmp/speech-worker

COPY requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir --no-deps . \
    && python -m pip check \
    && speech-worker --help \
    && speech-ingest --help \
    && ffmpeg -version > /dev/null \
    && ffprobe -version > /dev/null

USER 10001:10001
CMD ["speech-worker", "--lanes", "daily,backfill"]
