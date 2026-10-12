"""Ресурсы технического исполнителя конвейера."""

from src.application.pipeline.stages import StageDeps
from src.infrastructure.persistence.stages.repository import SessionFactory


class WorkerResources(StageDeps):
    """Зависимости этапов и отдельная фабрика транзакций исполнителя."""

    sessionmaker: SessionFactory | None = None
