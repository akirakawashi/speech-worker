"""Сборка служебного клиента и приёма метаданных для единого воркера."""

from datetime import date

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.application.ingestion.scheduled import ScheduledIntakeService
from src.application.maintenance import MaintenanceService
from src.di.sources import build_ingest_source
from src.infrastructure.clients.worker_control import BackendWorkerControl
from src.infrastructure.persistence.backfill import PostgresBackfillRepository
from src.infrastructure.persistence.daily_ingest import PostgresDailyIngestRepository
from src.infrastructure.queue.postgres import PostgresJobQueue
from src.setting.config import Settings


def build_worker_control(settings: Settings) -> BackendWorkerControl:
    """Проверить обязательные адрес и ключ и собрать служебный HTTP-клиент."""
    if not settings.speech_backend_base_url.strip() or not settings.speech_worker_api_token:
        raise RuntimeError(
            "worker control needs SPEECH_BACKEND_BASE_URL and SPEECH_WORKER_API_TOKEN"
        )
    return BackendWorkerControl(
        base_url=settings.speech_backend_base_url.rstrip("/"),
        api_token=settings.speech_worker_api_token,
        timeout_seconds=settings.speech_backend_timeout_seconds,
    )


def build_maintenance(
    settings: Settings, sessions: async_sessionmaker[AsyncSession], queue: PostgresJobQueue
) -> MaintenanceService:
    """Внедрить служебные команды и приём, использующий тот же пул, что и очередь."""
    history_from = settings.speech_telphin_history_from.strip()
    return MaintenanceService(
        build_worker_control(settings),
        ScheduledIntakeService(
            PostgresDailyIngestRepository(
                sessions, queue, phone_pepper=settings.speech_phone_hash_pepper
            ),
            PostgresBackfillRepository(sessions),
            lambda source: build_ingest_source(source.value, settings),
            telphin_history_from=date.fromisoformat(history_from) if history_from else None,
            history_days=settings.speech_telphin_history_days_per_run,
        ),
    )
