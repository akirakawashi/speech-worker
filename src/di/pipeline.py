"""Сборка обработчиков конвейера и их заменяемых внешних портов.

Реестр основного воркера содержит только ingest и transcribe.
Выбранные этапы обязаны иметь обработчики и настроенные зависимости до начала работы.
"""

from __future__ import annotations

from collections.abc import Collection
from typing import Any

from src.application.dto.pipeline_settings import PipelineSettings
from src.application.ingestion.recording import RecordingIngestionService
from src.application.pipeline.stages import StageHandler, StageRegistry
from src.application.ports.recordings import (
    RecordingStorage,
    RecordingTranscriber,
)
from src.application.transcription_delivery import TranscriptionDeliveryService
from src.domain.calls.contracts import JobKind
from src.infrastructure.media.processor import FfmpegMediaProcessor
from src.infrastructure.media.workspace import TemporaryRecordingWorkspace
from src.infrastructure.persistence.stages.repository import PostgresStageRepository, SessionFactory
from src.infrastructure.queue.resources import WorkerResources
from src.infrastructure.sources.fetcher import AudioStreamSource, SourceRecordingFetcher
from src.infrastructure.storage.pipeline import ObjectStorageTransport, PipelineStorage
from src.presentation.jobs.ingest import IngestHandler
from src.presentation.jobs.transcribe import TranscriptionHandler
from src.setting.config import Settings


def build_stage_deps(
    *,
    sessionmaker: SessionFactory | None,
    settings: Settings | None = None,
    source: AudioStreamSource | None = None,
    storage: ObjectStorageTransport | RecordingStorage | None = None,
    transcription: RecordingTranscriber | None = None,
    runtime_settings: dict[str, Any] | None = None,
) -> WorkerResources:
    """Собрать порты этапов и отдельно сохранить фабрику транзакций исполнителя.

    Args:
        sessionmaker: Фабрика сессий PostgreSQL или подставная фабрика теста.
        settings: Настройки вызывающего, уже применённые при сборке клиентов.
        source: Источник потока записи, если выполняется приём.
        storage: Транспорт хранилища записей, если он нужен выбранным этапам.
        transcription: Реализация распознавания подготовленного файла.
        runtime_settings: Проверенный снимок настроек обработки из базы.

    Returns:
        Зависимости этапов через порты и ресурсы их технического исполнителя.
    """
    workspace = TemporaryRecordingWorkspace(root=getattr(settings, "speech_worker_temp_dir", None))
    workspace.cleanup_abandoned()
    return WorkerResources(
        sessionmaker=sessionmaker,
        repository=PostgresStageRepository(sessionmaker),
        workspace=workspace,
        media=FfmpegMediaProcessor(),
        fetcher=SourceRecordingFetcher(source) if source is not None else None,
        storage=(storage if isinstance(storage, RecordingStorage) else PipelineStorage(storage))
        if storage is not None
        else None,
        transcription=transcription,
        runtime_settings=PipelineSettings.model_validate(runtime_settings or {}),
    )


def build_registry(
    stages: Collection[JobKind] | None = None,
    *,
    transcription_service: TranscriptionDeliveryService | None = None,
    ingest_service: RecordingIngestionService | None = None,
) -> StageRegistry:
    """Собрать получение и ASR; при выборе LLM или отсутствии доставки отказать до claim.

    Выход: реестр только технических заданий получения и распознавания.
    Распознавание завершается подтверждением бэкенда.
    """
    supported = {JobKind.INGEST, JobKind.TRANSCRIBE}
    wanted = supported if stages is None else set(stages)
    unknown = wanted - supported
    if unknown:
        raise ValueError("speech-worker supports only ingest and transcribe")
    handlers: dict[JobKind, StageHandler[Any]] = {}
    if JobKind.INGEST in wanted:
        if ingest_service is None:
            raise RuntimeError("ingest control service is not configured")
        handlers[JobKind.INGEST] = IngestHandler(ingest_service)
    if JobKind.TRANSCRIBE in wanted:
        if transcription_service is None:
            raise RuntimeError("transcription delivery service is not configured")
        handlers[JobKind.TRANSCRIBE] = TranscriptionHandler(transcription_service)

    registry = StageRegistry()
    for kind, handler in handlers.items():
        if kind in wanted:
            registry.register(handler)
    return registry
