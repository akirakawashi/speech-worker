"""Контракт, повторы, восстановление DTO и очистка ASR без внешних сервисов."""

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import httpx
import pytest

from src.application.dto.pipeline_input import CallForTranscription
from src.application.dto.transcription import TranscriptionResult, TranscriptionSegment
from src.application.dto.transcription_delivery import TranscriptionReceipt
from src.application.pipeline.stages import StageDeferred, StageDeps
from src.application.ports.media import MediaProcessor
from src.application.ports.pipeline import StageRepository
from src.application.ports.queue import CLAIM_TOKEN_KEY, ClaimedJob
from src.application.ports.recordings import RecordingStorage, RecordingTranscriber
from src.application.ports.transcription_delivery import (
    TRANSCRIPTION_DELIVERY_KEY,
    TranscriptionDeliveryError,
)
from src.application.transcription_delivery import TranscriptionDeliveryService
from src.di.pipeline import build_registry
from src.domain.calls.contracts import JobErrorClass, JobKind, JobLane
from src.infrastructure.clients.transcription_delivery import BackendTranscriptionDelivery
from src.infrastructure.media.workspace import TemporaryRecordingWorkspace
from src.infrastructure.queue.resources import WorkerResources
from src.infrastructure.queue.worker import Worker


def _job() -> ClaimedJob:
    """Создать захват, идентификатор которого выдала очередь."""
    return ClaimedJob(
        id=uuid4(),
        call_id=uuid4(),
        kind=JobKind.TRANSCRIBE,
        lane=JobLane.DAILY,
        attempts=1,
        max_attempts=3,
        payload={CLAIM_TOKEN_KEY: str(uuid4())},
        lease_seconds=900,
        locked_by="gpu-test",
    )


def _receipt(command) -> TranscriptionReceipt:
    """Вернуть синтетическую квитанцию с правильными идентификаторами входа."""
    claim = command.claim
    return TranscriptionReceipt(
        contract_version="1.0",
        call_id=claim.call_id,
        job_id=claim.job_id,
        claim_token=claim.claim_token,
        attempt=claim.attempt,
        transcript_id=uuid4(),
        analysis_job_id=uuid4(),
        accepted_at=datetime.now(UTC),
    )


def _dependencies(tmp_path: Path) -> StageDeps:
    """Собрать настоящий временный каталог и подставные порты аудио/ASR."""
    repository = Mock(spec=StageRepository)
    repository.load_transcription = AsyncMock(
        return_value=CallForTranscription(
            session_id="test",
            audio_status="stored",
            audio_object_key="calls/test.wav",
            audio_channels=1,
            duration_s=2,
        )
    )
    storage = Mock(spec=RecordingStorage)

    async def fetch(key, destination):
        destination.write_bytes(b"synthetic original audio")

    storage.fetch = AsyncMock(side_effect=fetch)
    media = Mock(spec=MediaProcessor)

    async def downmix(source, destination):
        destination.write_bytes(b"RIFF-synthetic-mono")
        return destination

    media.downmix = AsyncMock(side_effect=downmix)
    transcriber = Mock(spec=RecordingTranscriber)
    transcriber.transcribe = AsyncMock(
        return_value=TranscriptionResult(
            provider_kind="openai_compatible",
            model="large-v3",
            language="ru",
            duration_seconds=2,
            text="SYNTHETIC_PRIVATE_TRANSCRIPT",
            segments=(
                TranscriptionSegment(start_s=0, end_s=1.5, text="SYNTHETIC_PRIVATE_TRANSCRIPT"),
            ),
        )
    )
    return StageDeps(
        repository=repository,
        workspace=TemporaryRecordingWorkspace(root=tmp_path / "audio"),
        storage=storage,
        media=media,
        transcription=transcriber,
    )


@pytest.fixture
async def prepared(tmp_path):
    """Выполнить настоящее ASR-преобразование портов и сохранить исходный DTO для тестов."""
    deps = _dependencies(tmp_path)
    buffer, delivery = (
        SimpleNamespace(save=AsyncMock(return_value=True)),
        SimpleNamespace(deliver=AsyncMock(side_effect=_receipt)),
    )
    service = TranscriptionDeliveryService(buffer, delivery)
    job = _job()
    await service.execute(job, deps)
    command = buffer.save.await_args.args[0]
    return SimpleNamespace(
        deps=deps, buffer=buffer, delivery=delivery, service=service, job=job, command=command
    )


async def test_saves_before_http_and_cleans_only_own_audio(tmp_path) -> None:
    """DTO зафиксирован до передачи; исходник в MinIO не удаляется, анализ не вызывается."""
    deps = _dependencies(tmp_path)
    order = []

    async def save(command):
        order.append("buffer-committed")
        return True

    async def deliver(command):
        assert order == ["buffer-committed"]
        assert not list((tmp_path / "audio").glob("speech-transcribe-*"))
        return _receipt(command)

    service = TranscriptionDeliveryService(
        SimpleNamespace(save=save), SimpleNamespace(deliver=deliver)
    )
    receipt = await service.execute(_job(), deps)
    assert receipt.contract_version == "1.0"
    cast(Any, deps.transcription).transcribe.assert_awaited_once()
    cast(Any, deps.storage).put_file.assert_not_called()
    assert not hasattr(deps, "analysis") and not hasattr(deps, "evaluation")


async def test_restart_reuses_asr_with_new_claim_for_unchanged_audio(prepared) -> None:
    """Новая попытка получает новый nonce и владельца; Whisper повторно не вызывается."""
    job = prepared.job.model_copy(
        update={
            "payload": {
                CLAIM_TOKEN_KEY: str(uuid4()),
                TRANSCRIPTION_DELIVERY_KEY: prepared.command.model_dump(mode="json"),
            },
            "locked_by": "restarted-gpu",
            "attempts": 2,
        }
    )
    await prepared.service.execute(job, prepared.deps)
    prepared.deps.transcription.transcribe.assert_awaited_once()
    delivered = prepared.delivery.deliver.await_args.args[0]
    assert delivered.result == prepared.command.result
    assert delivered.claim.claim_token == job.claim_token != prepared.command.claim.claim_token
    assert delivered.claim.worker_id == "restarted-gpu"
    assert delivered.claim.attempt == 2


async def test_changed_audio_is_recognised_again(prepared) -> None:
    """Буфер старого аудио не выдаётся за результат новой записи."""
    job = prepared.job.model_copy(
        update={
            "payload": {
                CLAIM_TOKEN_KEY: str(uuid4()),
                TRANSCRIPTION_DELIVERY_KEY: prepared.command.model_dump(mode="json"),
            }
        }
    )
    prepared.deps.repository.load_transcription.return_value = CallForTranscription(
        session_id="test",
        audio_status="stored",
        audio_object_key="calls/replaced.wav",
        audio_channels=1,
    )
    await prepared.service.execute(job, prepared.deps)
    assert prepared.deps.transcription.transcribe.await_count == 2
    assert prepared.delivery.deliver.await_args.args[0].source.object_key == "calls/replaced.wav"


async def test_rejected_buffer_never_sends_result(prepared) -> None:
    """Потеря права записи до HTTP не посылает устаревший результат и не завершает задание."""
    prepared.buffer.save.return_value = False
    prepared.delivery.deliver.reset_mock()
    with pytest.raises(StageDeferred):
        await prepared.service.execute(prepared.job, prepared.deps)
    prepared.delivery.deliver.assert_not_awaited()


async def test_transient_delivery_keeps_buffer_and_returns_retry(prepared) -> None:
    """Неподтверждённый DTO остаётся после таймаута; попытка ASR не объявляется неудачной."""
    prepared.delivery.deliver.side_effect = TranscriptionDeliveryError(
        JobErrorClass.TIMEOUT, "timeout", retryable=True
    )
    with pytest.raises(StageDeferred) as error:
        await prepared.service.execute(prepared.job, prepared.deps)
    assert error.value.error_class is JobErrorClass.TIMEOUT
    assert prepared.buffer.save.await_count == 2


async def test_cancel_after_prepare_does_not_remove_durable_dto(prepared) -> None:
    """Отмена после подготовки прекращает HTTP, но не стирает буфер и не оставляет WAV."""
    prepared.delivery.deliver.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await prepared.service.execute(prepared.job, prepared.deps)
    assert prepared.buffer.save.await_count == 2


@pytest.mark.parametrize(
    "change",
    [
        {"claim_token": str(uuid4())},
        {"call_id": str(uuid4())},
        {"job_id": str(uuid4())},
        {"attempt": 7},
        {"contract_version": "2.0"},
        {"unexpected": "PRIVATE"},
    ],
)
async def test_wrong_receipt_is_not_accepted(prepared, monkeypatch, change) -> None:
    """Успешный HTTP без правильной версии/захвата не является подтверждением commit."""
    reply = {**_receipt(prepared.command).model_dump(mode="json"), **change}
    original = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: original(
            **kwargs, transport=httpx.MockTransport(lambda request: httpx.Response(200, json=reply))
        ),
    )
    client = BackendTranscriptionDelivery(base_url="http://backend", api_token="SECRET")
    with pytest.raises(TranscriptionDeliveryError) as error:
        await client.deliver(prepared.command)
    assert error.value.error_class is JobErrorClass.INVALID_OUTPUT
    assert not error.value.retryable
    assert "PRIVATE" not in str(error.value)


async def test_missing_receipt_version_is_rejected(prepared, monkeypatch) -> None:
    """Версия ответа обязательна, даже если текущий контракт имеет единственное значение."""
    reply = _receipt(prepared.command).model_dump(mode="json")
    del reply["contract_version"]
    original = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: original(
            **kwargs, transport=httpx.MockTransport(lambda request: httpx.Response(200, json=reply))
        ),
    )
    with pytest.raises(TranscriptionDeliveryError):
        await BackendTranscriptionDelivery(base_url="http://backend", api_token="key").deliver(
            prepared.command
        )


async def test_lost_response_repeats_identical_dto_and_checks_receipt(
    prepared, monkeypatch
) -> None:
    """Потерянный после commit ответ не меняет тело следующего запроса или ключ повтора."""
    requests = []
    reply = _receipt(prepared.command)

    async def handle(request):
        requests.append(request)
        assert request.headers["authorization"] == "Bearer worker-key"
        assert request.url.path == "/internal/v1/transcription-results"
        if len(requests) == 1:
            raise httpx.ReadTimeout("private external error", request=request)
        return httpx.Response(200, json=reply.model_dump(mode="json"))

    original = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: original(**kwargs, transport=httpx.MockTransport(handle)),
    )
    client = BackendTranscriptionDelivery(
        base_url="http://backend/", api_token="worker-key", retry_backoff_seconds=0
    )
    assert await client.deliver(prepared.command) == reply
    assert len(requests) == 2 and requests[0].content == requests[1].content
    assert json.loads(requests[0].content)["claim"]["claim_token"] == str(prepared.job.claim_token)
    assert "worker-key" not in repr(client)


@pytest.mark.parametrize(
    "status,retryable",
    [
        (401, False),
        (403, False),
        (404, False),
        (422, False),
        (302, False),
        (408, True),
        (409, True),
        (429, True),
        (503, True),
    ],
)
async def test_http_failure_policy_is_bounded_and_safe(
    prepared, monkeypatch, status, retryable
) -> None:
    """Повторы ограничены, статус и класс доступны, тело отказа/токен не выходят из адаптера."""
    sent = []

    def handle(request):
        sent.append(request)
        return httpx.Response(status, text="SYNTHETIC_PRIVATE_TRANSCRIPT and secret")

    original = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: original(**kwargs, transport=httpx.MockTransport(handle)),
    )
    with pytest.raises(TranscriptionDeliveryError) as error:
        await BackendTranscriptionDelivery(
            base_url="http://backend", api_token="secret", max_attempts=3, retry_backoff_seconds=0
        ).deliver(prepared.command)
    assert error.value.retryable is retryable
    assert len(sent) == (3 if retryable else 1)
    assert len({request.content for request in sent}) == 1
    assert "PRIVATE" not in str(error.value) and "secret" not in str(error.value)


async def test_worker_uses_backend_ack_without_local_store_or_complete(
    prepared, monkeypatch
) -> None:
    """Владелец бизнес-записи один: подтверждённый этап пропускает транзакцию старого store."""
    queue = Mock()
    queue.fail = AsyncMock()
    queue.complete = AsyncMock()
    queue.enqueue = AsyncMock()
    registry = build_registry([JobKind.TRANSCRIBE], transcription_service=prepared.service)
    handler = registry.get(JobKind.TRANSCRIBE)
    monkeypatch.setattr(handler, "work", AsyncMock(return_value=_receipt(prepared.command)))
    monkeypatch.setattr(handler, "slot_limit", Mock(return_value=None))
    monkeypatch.setattr(
        handler, "store", AsyncMock(side_effect=AssertionError("local store must not run"))
    )
    worker = Worker(
        queue,
        registry,
        WorkerResources(**prepared.deps.__dict__, sessionmaker=Mock()),
        lanes=[JobLane.DAILY],
        worker_id="gpu-test",
    )
    await worker._process(prepared.job)
    handler.store.assert_not_awaited()
    queue.complete.assert_not_awaited()
    queue.enqueue.assert_not_awaited()
    queue.fail.assert_not_awaited()


@pytest.mark.parametrize("kind", [JobKind.ANALYZE, JobKind.EVALUATE])
def test_main_registry_cannot_take_llm_jobs(kind) -> None:
    """Сохранённые переходные модули не доступны основному процессу воркера."""
    with pytest.raises(ValueError, match="only ingest and transcribe"):
        build_registry([kind])
