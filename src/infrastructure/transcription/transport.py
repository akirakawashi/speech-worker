"""Внутренний контракт HTTP-провайдера распознавания."""

from typing import Protocol

import httpx

from src.application.dto.transcription import (
    TranscriptionProviderKind,
    TranscriptionRequest,
    TranscriptionResult,
)
from src.infrastructure.http.execution import AdapterExecution


class TranscriptionProvider(Protocol):
    """Провайдер, работающий в бюджете времени своего HTTP-адаптера."""

    kind: TranscriptionProviderKind
    model: str
    audio_window_seconds: float | None

    async def transcribe(
        self,
        request: TranscriptionRequest,
        *,
        execution: AdapterExecution,
        client: httpx.AsyncClient,
    ) -> TranscriptionResult:
        """Отправить запись через клиент и вернуть DTO распознавания."""
        ...
