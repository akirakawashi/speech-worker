"""Сборка постановки оценки в очередь и исполнения готовых модельных шагов."""

from src.application.quality.execution import QualityExecutionService
from src.application.quality.queue_command import QueuedEvaluationService
from src.di.maintenance import build_worker_control
from src.infrastructure.clients.quality import PreparedQualityModelClient, build_quality_client
from src.setting.config import Settings, get_settings


def build_evaluation() -> QueuedEvaluationService:
    """Собрать ручную команду, которая ставит задания через API бэкенда."""
    return QueuedEvaluationService(build_worker_control(get_settings()))


def build_queue_evaluation(settings: Settings) -> QualityExecutionService:
    """Внедрить подготовку бэкендом и HTTP-вызов модели без локальных промптов."""
    return QualityExecutionService(
        build_worker_control(settings), PreparedQualityModelClient(build_quality_client(settings))
    )
