"""Сборка обработчиков конвейера и их заменяемых внешних портов.

Реестр содержит ingest, transcribe и analyze. Выбранные этапы обязаны иметь
обработчики и настроенные зависимости до начала обработки заданий.
"""

from __future__ import annotations

from collections.abc import Collection
from typing import Any

from src.application.dto.pipeline_settings import PipelineSettings
from src.application.pipeline.analyze import AnalyzeHandler
from src.application.pipeline.ingest import IngestHandler
from src.application.pipeline.stages import StageHandler, StageRegistry
from src.application.pipeline.transcribe import TranscribeHandler
from src.application.ports.recordings import (
    AnalysisBackend,
    RecordingStorage,
    RecordingTranscriber,
)
from src.domain.calls.contracts import JobKind
from src.infrastructure.media.processor import FfmpegMediaProcessor
from src.infrastructure.media.workspace import TemporaryRecordingWorkspace
from src.infrastructure.persistence.stages.repository import PostgresStageRepository, SessionFactory
from src.infrastructure.queue.resources import WorkerResources
from src.infrastructure.sources.fetcher import AudioStreamSource, SourceRecordingFetcher
from src.infrastructure.storage.pipeline import ObjectStorageTransport, PipelineStorage
from src.setting.config import Settings


def build_stage_deps(
    *,
    sessionmaker: SessionFactory | None,
    settings: Settings | None = None,
    source: AudioStreamSource | None = None,
    storage: ObjectStorageTransport | RecordingStorage | None = None,
    transcription: RecordingTranscriber | None = None,
    analysis: AnalysisBackend | None = None,
    runtime_settings: dict[str, Any] | None = None,
) -> WorkerResources:
    """Собрать порты этапов и отдельно сохранить фабрику транзакций исполнителя.

    Args:
        sessionmaker: Фабрика сессий PostgreSQL или подставная фабрика теста.
        settings: Настройки вызывающего, уже применённые при сборке клиентов.
        source: Источник потока записи, если выполняется приём.
        storage: Транспорт хранилища записей, если он нужен выбранным этапам.
        transcription: Реализация распознавания подготовленного файла.
        analysis: Реализация модели разбора разговора.
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
        analysis=analysis,
        runtime_settings=PipelineSettings.model_validate(runtime_settings or {}),
    )


def build_registry(stages: Collection[JobKind] | None = None) -> StageRegistry:
    """Реестр этапов для боевого воркера.

    `stages` ограничивает набор. По умолчанию — все три, и это правильное
    умолчание: воркер, который молча не берёт какой-то этап, оставляет суточный
    пакет наполовину обработанным без единого сообщения.

    Ограничение существует для случая, когда этап **нельзя** выполнить и это
    известно заранее: у проекта ещё нет приложения LLM, а расшифровать
    записи уже нужно. Тогда отказ становится решением оператора, а
    не молчанием: он назвал этапы сам.
    """
    handlers: dict[JobKind, StageHandler[Any]] = {
        JobKind.INGEST: IngestHandler(),
        JobKind.TRANSCRIBE: TranscribeHandler(),
        JobKind.ANALYZE: AnalyzeHandler(),
    }
    wanted = set(handlers) if stages is None else set(stages)
    unknown = wanted - set(handlers)
    if unknown:
        raise ValueError(f"unknown stages: {sorted(kind for kind in unknown)}")

    registry = StageRegistry()
    for kind, handler in handlers.items():
        if kind in wanted:
            registry.register(handler)
    return registry
