"""Сборка самостоятельного воркера и управление временем жизни ресурсов."""

from __future__ import annotations

import asyncio

from src.application.dto.calls import JobKind
from src.application.dto.maintenance import MaintenanceAction
from src.application.dto.worker import WorkerOptions
from src.di.maintenance import build_maintenance
from src.di.pipeline import build_registry, build_stage_deps
from src.di.sources import build_worker_source
from src.di.transcription_delivery import build_transcription_delivery
from src.infrastructure.clients.whisper import build_whisper_client
from src.infrastructure.persistence.session import (
    SPEECH_REQUIRED_SCHEMA_REVISION,
    get_speech_ms_engine,
    get_speech_ms_sessionmaker,
    speech_schema_is_ready,
)
from src.infrastructure.queue.postgres import PostgresJobQueue
from src.infrastructure.queue.worker import Worker, _install_signal_handlers
from src.infrastructure.runtime.logs import configure_logging
from src.infrastructure.runtime.service import WorkerService
from src.infrastructure.storage.recordings import build_storage
from src.setting.config import Settings, get_settings


async def _run_worker(options: WorkerOptions, settings: Settings) -> None:
    """Собрать выбранные этапы, проверить схему и освободить пул после остановки.

    Args:
        options: Проверенные полосы, этапы и режим однократного запуска.
        settings: Настройки данного процесса без чтения окружения внутри этапов.

    Raises:
        RuntimeError: Выбранный этап не настроен или схема базы несовместима.
        Exception: Внешние ресурсы невозможно собрать или проверить.
    """
    # Пул создаётся в том же event loop, где используется и закрывается.
    engine = get_speech_ms_engine(settings)
    try:
        sessionmaker = get_speech_ms_sessionmaker(settings)
        stages = set(options.stages)
        resources = build_stage_deps(
            settings=settings,
            sessionmaker=sessionmaker,
            source=build_worker_source(settings) if JobKind.INGEST in stages else None,
            storage=build_storage(settings)
            if {JobKind.INGEST, JobKind.TRANSCRIBE} & stages
            else None,
            transcription=build_whisper_client(settings) if JobKind.TRANSCRIBE in stages else None,
        )
        async with sessionmaker() as session:
            if not await speech_schema_is_ready(session):
                raise RuntimeError(
                    "Speech database schema is not ready. Apply speech-backend migrations "
                    f"to {SPEECH_REQUIRED_SCHEMA_REVISION} before starting the worker."
                )
        queue = PostgresJobQueue(sessionmaker, engine=engine)
        worker = Worker(
            queue,
            build_registry(
                options.stages,
                transcription_service=build_transcription_delivery(settings, sessionmaker)
                if JobKind.TRANSCRIBE in stages
                else None,
            ),
            resources,
            lanes=options.lanes,
        )
        print(
            f"speech worker {worker.worker_id} "
            f"lanes={','.join(lane.value for lane in options.lanes)} "
            f"stages={','.join(kind.value for kind in options.stages)}"
        )
        if options.once:
            _install_signal_handlers(worker)
            await worker.run_once()
        else:
            actions: list[MaintenanceAction] = []
            if not options.queue_only:
                if JobKind.INGEST in stages:
                    actions.extend(source.value for source in options.sources)
            if actions:
                service = WorkerService(
                    worker, build_maintenance(settings, sessionmaker, queue), actions
                )
                _install_signal_handlers(service)
                await service.run_forever()
            else:
                _install_signal_handlers(worker)
                await worker.run_forever()
    finally:
        await engine.dispose()


def run_worker(options: WorkerOptions) -> int:
    """Прочитать настройки в DI, запустить последовательный цикл и вернуть ноль."""
    configure_logging()
    asyncio.run(_run_worker(options, get_settings()))
    return 0
