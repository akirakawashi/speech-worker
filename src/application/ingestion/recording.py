"""Техническое скачивание и измерение записи по готовой команде бэкенда."""

import logging
from pathlib import Path

from src.application.dto.calls import JobErrorClass, JobKind
from src.application.dto.ingest_control import (
    FetchRecording,
    IngestClaim,
    IngestDecision,
    IngestObservation,
    ResolveIngest,
)
from src.application.errors.external import MediaError, RecordingNotAvailable, StorageError
from src.application.pipeline.stages import StageDeps, StageError
from src.application.ports.ingest_control import IngestControl
from src.application.ports.queue import ClaimedJob

logger = logging.getLogger(__name__)
DURATION_MISMATCH_RATIO = 0.1


class RecordingIngestionService:
    """Получить команду, скачать/измерить файл и вернуть готовое решение о его сохранении."""

    def __init__(self, control: IngestControl) -> None:
        """Внедрить порт бэкенда; источники/хранилище передаются для одного задания."""
        self._control = control

    async def execute(self, job: ClaimedJob, deps: StageDeps) -> IngestDecision:
        """Выполнить получение без локального порога длительности и выбора следующего этапа.

        Вход — живой захват и технические порты. Выход — решение бэкенда для
        короткой транзакции очереди. Бэкенд недоступен — StageDeferred; его
        команда повтора отсутствующей записи — StageError NOT_FOUND.
        """
        if job.kind is not JobKind.INGEST or job.claim_token is None or not job.locked_by:
            raise StageError(JobErrorClass.INVALID_OUTPUT, "ingest claim has no identity")
        claim = IngestClaim(
            call_id=job.call_id,
            job_id=job.id,
            claim_token=job.claim_token,
            worker_id=job.locked_by,
            attempt=job.attempts,
        )
        plan = await self._control.prepare(claim)
        if plan.completion is not None:
            return plan.completion
        assert plan.fetch is not None
        try:
            observation = await self._fetch(job, deps, plan.fetch)
        except RecordingNotAvailable:
            observation = IngestObservation(outcome="missing")
        resolved = await self._control.resolve(ResolveIngest(claim=claim, observation=observation))
        if resolved.retry:
            raise StageError(JobErrorClass.NOT_FOUND, "source has no recording yet")
        assert resolved.completion is not None
        return resolved.completion

    @staticmethod
    async def _fetch(
        job: ClaimedJob, deps: StageDeps, command: FetchRecording
    ) -> IngestObservation:
        """Проверить готовый объект либо скачать/измерить/сохранить файл; убрать локальную копию."""
        if deps.storage is None or deps.fetcher is None or deps.media is None:
            raise RuntimeError("recording ingestion ports are not configured")
        key = command.object_key
        try:
            if command.check_stored and await deps.storage.exists(key):
                return IngestObservation(outcome="stored", object_key=key)
        except StorageError:
            raise StageError(
                JobErrorClass.PROVIDER_ERROR, "recording storage is unavailable"
            ) from None
        with deps.workspace.directory(prefix="speech-ingest-") as tmp:
            path = Path(tmp) / "recording.mp3"
            written = await deps.fetcher.download(command.ref, path)
            try:
                measured = await deps.media.probe(path)
            except MediaError:
                raise StageError(JobErrorClass.INVALID_OUTPUT, "unusable audio file") from None
            try:
                await deps.storage.put_file(key, path)
            except StorageError:
                raise StageError(JobErrorClass.PROVIDER_ERROR, "recording upload failed") from None
        stated = command.ref.duration_s
        if stated and measured.duration_s > 0:
            if abs(measured.duration_s - stated) / stated > DURATION_MISMATCH_RATIO:
                logger.warning(
                    "call=%s: source duration=%s measured=%.1f",
                    job.call_id,
                    stated,
                    measured.duration_s,
                )
        return IngestObservation(
            outcome="stored",
            object_key=key,
            audio_bytes=written,
            channels=measured.channels,
            duration_s=round(measured.duration_s),
        )
