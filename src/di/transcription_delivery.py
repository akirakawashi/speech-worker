"""Сборка долговременного буфера и служебного клиента без зависимости от LLM."""

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.application.transcription_delivery import TranscriptionDeliveryService
from src.infrastructure.clients.transcription_delivery import BackendTranscriptionDelivery
from src.infrastructure.persistence.transcription_delivery import (
    PostgresTranscriptionDeliveryBuffer,
)
from src.setting.config import Settings


def build_transcription_delivery(
    settings: Settings, sessions: async_sessionmaker[AsyncSession]
) -> TranscriptionDeliveryService:
    """Проверить доступ к бэкенду и собрать сценарий; отсутствие настроек останавливает старт."""
    base_url = settings.speech_backend_base_url.strip().rstrip("/")
    token = settings.speech_worker_api_token.strip()
    if not base_url or not token:
        raise RuntimeError(
            "transcription requires SPEECH_BACKEND_BASE_URL and SPEECH_WORKER_API_TOKEN"
        )
    return TranscriptionDeliveryService(
        PostgresTranscriptionDeliveryBuffer(sessions),
        BackendTranscriptionDelivery(
            base_url=base_url,
            api_token=token,
            timeout_seconds=settings.speech_backend_timeout_seconds,
        ),
    )
