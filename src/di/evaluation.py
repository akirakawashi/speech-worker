"""Сборка пакетной оценки из PostgreSQL и клиента выбранной модели."""

from src.application.quality.batch import EvaluationService
from src.infrastructure.clients.quality import build_quality_client
from src.infrastructure.persistence.evaluation import PostgresEvaluationRepository
from src.infrastructure.persistence.session import get_speech_ms_sessionmaker
from src.setting.config import get_settings


def build_evaluation() -> EvaluationService:
    """Прочитать настройки при запуске, собрать хранение и отложенную модель оценки."""
    settings = get_settings()
    return EvaluationService(
        PostgresEvaluationRepository(get_speech_ms_sessionmaker(settings)),
        lambda: build_quality_client(settings),
        timezone=settings.speech_display_timezone,
    )
