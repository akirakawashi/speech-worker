"""Сборка технического получения записи через служебные решения бэкенда."""

from src.application.ingestion.recording import RecordingIngestionService
from src.infrastructure.clients.ingest_control import BackendIngestControl
from src.setting.config import Settings


def build_ingest_control(settings: Settings) -> RecordingIngestionService:
    """Проверить обязательные адрес/ключ и внедрить HTTP-порт без модельных клиентов."""
    if not settings.speech_backend_base_url.strip() or not settings.speech_worker_api_token:
        raise RuntimeError(
            "ingest control needs SPEECH_BACKEND_BASE_URL and SPEECH_WORKER_API_TOKEN"
        )
    return RecordingIngestionService(
        BackendIngestControl(
            base_url=settings.speech_backend_base_url.rstrip("/"),
            api_token=settings.speech_worker_api_token,
            timeout_seconds=settings.speech_backend_timeout_seconds,
        )
    )
