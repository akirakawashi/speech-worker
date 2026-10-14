"""Техническое получение выполняет команды бэкенда и очищает файлы при любом исходе."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from src.application.dto.calls import JobErrorClass, JobKind, JobLane, ProcessingStatus
from src.application.dto.ingest_control import (
    FetchRecording,
    IngestClaim,
    IngestDecision,
    IngestObservation,
    IngestPlan,
    IngestResolution,
)
from src.application.dto.pipeline import IngestWork, StageResult
from src.application.dto.sources import CallRef
from src.application.errors.external import RecordingNotAvailable
from src.application.ingestion.recording import RecordingIngestionService
from src.application.pipeline.stages import StageDeferred, StageDeps, StageError
from src.application.ports.queue import ClaimedJob
from src.infrastructure.media.audio import AudioProbe
from src.infrastructure.media.processor import FfmpegMediaProcessor
from src.infrastructure.media.workspace import TemporaryRecordingWorkspace
from src.infrastructure.sources.fetcher import SourceRecordingFetcher
from src.infrastructure.storage.pipeline import PipelineStorage
from src.presentation.jobs.ingest import IngestHandler

KEY = "calls/2026/01/14/session.mp3"


class Source:
    """Записать число открытий потока и выдать байты либо техническую ошибку."""

    def __init__(self, error=None, chunks=(b"ID3", b"audio-bytes")):
        self.error, self.chunks, self.opened = error, chunks, 0

    async def open_audio(self, ref):
        self.opened += 1
        if self.error:
            raise self.error

        async def stream():
            for chunk in self.chunks:
                yield chunk

        return stream()


class Storage:
    """Проверить настоящее чтение временного файла и запомнить переданные байты."""

    def __init__(self):
        self.has = False
        self.puts = []
        self.exists_calls = 0

    async def exists(self, key):
        self.exists_calls += 1
        return self.has

    async def put(self, key, chunks):
        body = b"".join([chunk async for chunk in chunks])
        self.puts.append((key, body))
        return len(body)


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Подставить управление/ffprobe; файловые адаптеры и очистку оставить настоящими."""
    source, storage = Source(), Storage()
    measured = AudioProbe(duration_s=208, channels=2, sample_rate=8000, bit_rate=24000, codec="mp3")
    monkeypatch.setattr(
        "src.infrastructure.media.audio.probe_audio", AsyncMock(return_value=measured)
    )
    job = ClaimedJob(
        id=uuid4(),
        call_id=uuid4(),
        kind=JobKind.INGEST,
        lane=JobLane.DAILY,
        attempts=1,
        max_attempts=6,
        payload={"_claim_token": str(uuid4())},
        lease_seconds=300,
        locked_by="worker-test",
    )
    claim = IngestClaim(
        call_id=job.call_id,
        job_id=job.id,
        claim_token=job.claim_token,
        worker_id=job.locked_by,
        attempt=job.attempts,
    )
    fetch = FetchRecording(
        ref=CallRef(session_id="session", day=datetime.now(UTC).date(), audio_locator="record"),
        object_key=KEY,
    )
    control = AsyncMock()
    control.prepare.return_value = IngestPlan(claim=claim, fetch=fetch)
    decision = IngestDecision(
        work=IngestWork(outcome="stored", object_key=KEY),
        stage=StageResult(
            processing_status=ProcessingStatus.TRANSCRIBING, next_kind=JobKind.TRANSCRIBE
        ),
    )
    control.resolve.return_value = IngestResolution(claim=claim, completion=decision)
    deps = StageDeps(
        workspace=TemporaryRecordingWorkspace(tmp_path),
        media=FfmpegMediaProcessor(),
        fetcher=SourceRecordingFetcher(source),
        storage=PipelineStorage(storage),
    )
    return job, claim, control, source, storage, deps, tmp_path


async def test_recording_downloads_probes_and_uploads_before_backend_resolution(env):
    """В бэкенд уходят измерения; возвращается именно его решение, а временный файл удалён."""
    job, claim, control, source, storage, deps, scratch = env
    result = await RecordingIngestionService(control).execute(job, deps)
    assert source.opened == 1 and storage.puts == [(KEY, b"ID3audio-bytes")]
    sent = control.resolve.call_args.args[0]
    assert sent.claim == claim
    assert sent.observation == IngestObservation(
        outcome="stored", object_key=KEY, audio_bytes=14, channels=2, duration_s=208
    )
    assert result == control.resolve.return_value.completion
    assert list(scratch.glob("job-*")) == []


async def test_ready_completion_never_opens_source_or_storage(env):
    """Готовая команда завершения не скачивает аудио и не обращается за повторным решением."""
    job, claim, control, source, storage, deps, _ = env
    decision = IngestDecision(
        work=IngestWork(outcome="skipped_short", segment="backend-project"),
        stage=StageResult(processing_status=ProcessingStatus.SKIPPED_SHORT),
    )
    control.prepare.return_value = IngestPlan(claim=claim, completion=decision)
    assert await RecordingIngestionService(control).execute(job, deps) == decision
    assert source.opened == 0 and storage.puts == []
    control.resolve.assert_not_awaited()


async def test_worker_does_not_second_guess_short_duration_in_download_command(env):
    """Бэкенд разрешил получение девяти секунд — локального порога у воркера нет."""
    job, _, control, source, _, deps, _ = env
    fetch = control.prepare.return_value.fetch
    control.prepare.return_value = control.prepare.return_value.model_copy(
        update={
            "fetch": fetch.model_copy(
                update={"ref": fetch.ref.model_copy(update={"duration_s": 9})}
            )
        }
    )
    await RecordingIngestionService(control).execute(job, deps)
    assert source.opened == 1


@pytest.mark.parametrize("retry", [True, False])
async def test_missing_recording_is_reported_and_backend_decides_even_on_last_attempt(env, retry):
    """Номер попытки не заставляет воркер самому объявлять запись недоступной."""
    job, claim, control, source, _, deps, scratch = env
    source.error = RecordingNotAvailable("PRIVATE_URL")
    job = job.model_copy(update={"attempts": job.max_attempts})
    claim = claim.model_copy(update={"attempt": job.attempts})
    control.prepare.return_value = control.prepare.return_value.model_copy(update={"claim": claim})
    decision = IngestDecision(
        work=IngestWork(outcome="no_audio"),
        stage=StageResult(processing_status=ProcessingStatus.SKIPPED_NO_AUDIO),
    )
    control.resolve.return_value = IngestResolution(
        claim=claim, retry=retry, completion=None if retry else decision
    )
    if retry:
        with pytest.raises(StageError) as error:
            await RecordingIngestionService(control).execute(job, deps)
        assert error.value.error_class == JobErrorClass.NOT_FOUND
        assert "PRIVATE_URL" not in str(error.value)
    else:
        assert await RecordingIngestionService(control).execute(job, deps) == decision
    assert control.resolve.call_args.args[0].observation == IngestObservation(outcome="missing")
    assert list(scratch.glob("job-*")) == []


async def test_store_applies_backend_data_and_returns_its_transition(env):
    """Короткая фаза ничего не решает: данные и переход берутся из одного DTO бэкенда."""
    job, _, control, _, _, deps, _ = env
    handler = IngestHandler(RecordingIngestionService(control))
    decision = await handler.work(job, deps)
    persistence = AsyncMock()
    assert await handler.store(persistence, job, decision) == decision.stage
    persistence.save_ingest.assert_awaited_once_with(job, decision.work)


@pytest.mark.parametrize("exists", [True, False])
async def test_existing_object_is_verified_and_missing_object_is_downloaded_again(env, exists):
    """Проверка MinIO сохраняется; наличие колонки не выдаётся за наличие файла."""
    job, _, control, source, storage, deps, _ = env
    storage.has = exists
    control.prepare.return_value = control.prepare.return_value.model_copy(
        update={
            "fetch": control.prepare.return_value.fetch.model_copy(update={"check_stored": True})
        }
    )
    await RecordingIngestionService(control).execute(job, deps)
    assert storage.exists_calls == 1 and source.opened == (0 if exists else 1)
    observation = control.resolve.call_args.args[0].observation
    assert observation.channels == (None if exists else 2)
    assert observation.audio_bytes == (None if exists else 14)


@pytest.mark.parametrize("error,chunks", [(ConnectionResetError("PRIVATE_URL"), ()), (None, ())])
async def test_broken_source_or_empty_file_never_becomes_missing_recording(env, error, chunks):
    """Техническая поломка не отправляется как штатное отсутствие записи."""
    job, _, control, source, _, deps, scratch = env
    source.error, source.chunks = error, chunks
    with pytest.raises(StageError) as failed:
        await RecordingIngestionService(control).execute(job, deps)
    assert failed.value.error_class == JobErrorClass.PROVIDER_ERROR
    control.resolve.assert_not_awaited()
    assert list(scratch.glob("job-*")) == []


async def test_unavailable_backend_defers_before_downloading(env):
    """При недоступном управлении локальные файлы не появляются и бюджет скачиваний не тратится."""
    job, _, control, source, storage, deps, scratch = env
    control.prepare.side_effect = StageDeferred(JobErrorClass.PROVIDER_ERROR, "unavailable")
    with pytest.raises(StageDeferred):
        await RecordingIngestionService(control).execute(job, deps)
    assert source.opened == 0 and storage.puts == [] and list(scratch.glob("job-*")) == []
