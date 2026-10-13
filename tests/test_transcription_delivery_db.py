"""PostgreSQL: долговременный буфер, смена захвата, повторы и отсутствие локальной записи."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from src.application.dto.transcription_delivery import TranscriptionDeliveryRequest
from src.application.ports.queue import CLAIM_TOKEN_KEY
from src.application.ports.transcription_delivery import (
    TRANSCRIPTION_DELIVERY_KEY,
    TranscriptionDeliveryError,
)
from src.application.transcription_delivery import TranscriptionDeliveryService
from src.di.pipeline import build_registry
from src.domain.calls.contracts import JobErrorClass, JobKind, JobLane, JobStatus
from src.infrastructure.persistence.transcription_delivery import (
    PostgresTranscriptionDeliveryBuffer,
)
from src.infrastructure.queue.resources import WorkerResources
from src.infrastructure.queue.worker import Worker
from tests.speech_db_support import speech_test_database_url
from tests.test_speech_queue_protocol import _enqueue, _make_call, queue as queue
from tests.test_transcription_delivery import _dependencies, _receipt

pytestmark = pytest.mark.skipif(
    not speech_test_database_url(), reason="SPEECH_POSTGRES_TEST_URL is required"
)


async def _ready(queue, tmp_path, *, max_attempts=3):
    """Выдать настоящий захват и сохранить подготовленный DTO через транзакционный порт."""
    call_id = await _make_call(queue)
    async with queue._sessionmaker() as session, session.begin():
        await session.execute(
            text(
                "UPDATE speech_calls SET audio_status = 'stored', "
                "audio_object_key = 'calls/test.wav', "
                "audio_channels = 1, processing_status = 'queued' WHERE id = :id"
            ),
            {"id": call_id},
        )
    await _enqueue(queue, call_id, kind=JobKind.TRANSCRIBE, max_attempts=max_attempts)
    job = await queue.claim([JobLane.DAILY], "gpu-test", kinds=[JobKind.TRANSCRIBE])
    assert job is not None and job.claim_token is not None and job.locked_by == "gpu-test"
    deps = _dependencies(tmp_path)
    buffer = PostgresTranscriptionDeliveryBuffer(queue._sessionmaker)
    delivery = SimpleNamespace(deliver=AsyncMock(side_effect=_receipt))
    service = TranscriptionDeliveryService(buffer, delivery)
    await service.execute(job, deps)
    command = delivery.deliver.await_args.args[0]
    return job, command, service, deps


async def _payload(queue, job_id):
    """Прочитать текущий буфер из базы отдельной сессией, а не из исходного DTO захвата."""
    async with queue._sessionmaker() as session:
        return await session.scalar(
            text("SELECT payload FROM speech_jobs WHERE id = :id"), {"id": job_id}
        )


async def test_prepared_result_is_durable_without_writing_business_tables(queue, tmp_path) -> None:
    """После короткого commit в базе есть DTO, но нет транскрипта, анализа или complete."""
    job, command, _, _ = await _ready(queue, tmp_path)
    payload = await _payload(queue, job.id)
    assert (
        TranscriptionDeliveryRequest.model_validate(payload[TRANSCRIPTION_DELIVERY_KEY]) == command
    )
    async with queue._sessionmaker() as session:
        assert (
            await session.scalar(
                text("SELECT status FROM speech_jobs WHERE id = :id"), {"id": job.id}
            )
            == "running"
        )
        assert await session.scalar(text("SELECT count(*) FROM speech_transcripts")) == 0
        assert (
            await session.scalar(text("SELECT count(*) FROM speech_jobs WHERE kind = 'analyze'"))
            == 0
        )


async def test_defer_preserves_result_and_reclaim_mints_nonce(queue, tmp_path) -> None:
    """Отсрочка освобождает старый захват, новая доставка переиспользует ASR для того же аудио."""
    job, command, service, deps = await _ready(queue, tmp_path)
    assert await queue.defer(job.id, "gpu-test", delay_seconds=0, claim_token=job.claim_token)
    deferred = await _payload(queue, job.id)
    assert CLAIM_TOKEN_KEY not in deferred
    assert deferred[TRANSCRIPTION_DELIVERY_KEY] == command.model_dump(mode="json")
    reclaimed = await queue.claim([JobLane.DAILY], "gpu-restarted", kinds=[JobKind.TRANSCRIBE])
    assert reclaimed is not None and reclaimed.claim_token != job.claim_token
    assert reclaimed.attempts == job.attempts
    await service.execute(reclaimed, deps)
    deps.transcription.transcribe.assert_awaited_once()
    assert (await _payload(queue, job.id))[TRANSCRIPTION_DELIVERY_KEY]["claim"][
        "worker_id"
    ] == "gpu-restarted"


async def test_old_nonce_cannot_heartbeat_fail_defer_or_replace_buffer(queue, tmp_path) -> None:
    """Повторное имя воркера и счётчик попыток не разрешают мутировать новый захват."""
    job, command, _, _ = await _ready(queue, tmp_path)
    await queue.defer(job.id, "gpu-test", delay_seconds=0, claim_token=job.claim_token)
    current = await queue.claim([JobLane.DAILY], "gpu-test", kinds=[JobKind.TRANSCRIBE])
    assert current is not None and current.attempts == job.attempts
    assert not await queue.heartbeat(job.id, "gpu-test", claim_token=job.claim_token)
    assert (
        await queue.fail(
            job.id, "gpu-test", JobErrorClass.INVALID_OUTPUT, claim_token=job.claim_token
        )
        is None
    )
    assert not await queue.defer(job.id, "gpu-test", delay_seconds=0, claim_token=job.claim_token)
    assert not await PostgresTranscriptionDeliveryBuffer(queue._sessionmaker).save(command)
    assert await queue.heartbeat(job.id, "gpu-test", claim_token=current.claim_token)
    assert (await _payload(queue, job.id))[CLAIM_TOKEN_KEY] == str(current.claim_token)


async def test_same_claim_cannot_replace_prepared_result(queue, tmp_path) -> None:
    """Под тем же ключом повтора допускается только уже сохранённый DTO."""
    job, command, _, _ = await _ready(queue, tmp_path)
    buffer = PostgresTranscriptionDeliveryBuffer(queue._sessionmaker)
    assert await buffer.save(command)
    changed = command.model_copy(
        update={"result": command.result.model_copy(update={"model_name": "different"})}
    )
    assert not await buffer.save(changed)
    assert (await _payload(queue, job.id))[TRANSCRIPTION_DELIVERY_KEY] == command.model_dump(
        mode="json"
    )


@pytest.mark.parametrize("change", ["audio", "lease"])
async def test_stale_source_or_expired_lease_cannot_write_buffer(queue, tmp_path, change) -> None:
    """Актуальность проверяется в самой записи SQL, после всех долгих операций."""
    job, command, _, _ = await _ready(queue, tmp_path)
    async with queue._sessionmaker() as session, session.begin():
        if change == "audio":
            await session.execute(
                text("UPDATE speech_calls SET audio_object_key = 'changed.wav' WHERE id = :id"),
                {"id": job.call_id},
            )
        else:
            await session.execute(
                text(
                    "UPDATE speech_jobs SET locked_at = now() - interval '2 hours' WHERE id = :id"
                ),
                {"id": job.id},
            )
    assert not await PostgresTranscriptionDeliveryBuffer(queue._sessionmaker).save(command)


async def test_reaper_keeps_prepared_result_even_on_last_attempt(queue, tmp_path) -> None:
    """Авария после ASR возвращает доставку, а не объявляет готовое распознавание погибшим."""
    job, command, service, deps = await _ready(queue, tmp_path, max_attempts=1)
    async with queue._sessionmaker() as session, session.begin():
        await session.execute(
            text("UPDATE speech_jobs SET locked_at = now() - interval '2 hours' WHERE id = :id"),
            {"id": job.id},
        )
    expired = await queue.requeue_expired()
    assert len(expired) == 1 and expired[0].status is JobStatus.QUEUED
    assert CLAIM_TOKEN_KEY not in await _payload(queue, job.id)
    next_job = await queue.claim([JobLane.DAILY], "after-crash", kinds=[JobKind.TRANSCRIBE])
    assert next_job is not None and next_job.attempts == 1
    await service.execute(next_job, deps)
    deps.transcription.transcribe.assert_awaited_once()
    assert command.result == service._delivery.deliver.await_args.args[0].result


async def test_terminal_failure_removes_private_pending_result(queue, tmp_path) -> None:
    """Неповторяемый отказ снимает аренду и не оставляет копию текста в терминальном задании."""
    job, _, _, _ = await _ready(queue, tmp_path)
    status = await queue.fail(
        job.id, "gpu-test", JobErrorClass.INVALID_OUTPUT, claim_token=job.claim_token
    )
    assert status is JobStatus.DEAD
    payload = await _payload(queue, job.id)
    assert TRANSCRIPTION_DELIVERY_KEY not in payload and CLAIM_TOKEN_KEY not in payload


async def test_backend_outage_defers_without_exhausting_asr_attempts(queue, tmp_path) -> None:
    """Основной цикл сохраняет DTO при 503, следующий захват не расходует GPU снова."""
    job, _, service, deps = await _ready(queue, tmp_path, max_attempts=1)
    await queue.defer(job.id, "gpu-test", delay_seconds=0, claim_token=job.claim_token)
    service._delivery.deliver.side_effect = TranscriptionDeliveryError(
        JobErrorClass.PROVIDER_ERROR, "http_503", retryable=True
    )
    resources = WorkerResources(**deps.__dict__, sessionmaker=queue._sessionmaker)
    worker = Worker(
        queue,
        build_registry([JobKind.TRANSCRIBE], transcription_service=service),
        resources,
        lanes=[JobLane.DAILY],
        worker_id="gpu-test",
    )
    assert await worker.run_once()
    async with queue._sessionmaker() as session:
        row = (
            await session.execute(
                text("SELECT status, attempts, payload FROM speech_jobs WHERE id = :id"),
                {"id": job.id},
            )
        ).one()
        assert row.status == "queued" and row.attempts == 0
        assert TRANSCRIPTION_DELIVERY_KEY in row.payload and CLAIM_TOKEN_KEY not in row.payload
    deps.transcription.transcribe.assert_awaited_once()
