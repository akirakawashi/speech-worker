"""Shared HTTP transport for transcription providers.

Each concrete provider only decides how to build the outbound request and how
to parse the response body; the deadline, retry policy, error normalization
and truncation heuristic live here exactly once.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import httpx
from pydantic import BaseModel, ConfigDict

from src.application.dto.transcription import (
    TranscriptionError,
    TranscriptionProviderKind,
    TranscriptionRequest,
    TranscriptionResult,
    TruncationCheck,
)
from src.infrastructure.http.execution import AdapterExecution, AdapterRequestError
from src.setting.transcription import TranscriptionConfig

# A transcript this close to the declared processing window is treated as
# possibly truncated rather than exactly at the boundary by coincidence.
_TRUNCATION_MARGIN_SECONDS = 0.5


class TranscriptionHttpRequest(BaseModel):
    """Технический HTTP-запрос провайдера распознавания."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    url: str
    headers: dict[str, str]
    json_body: dict[str, object] | None = None
    data: dict[str, str] | None = None
    files: dict[str, tuple[str, bytes, str]] | None = None


class HttpTranscriptionProvider(ABC):
    # Not ClassVar: the TranscriptionProvider Protocol declares `kind` as an
    # instance attribute, and mypy treats ClassVar as structurally
    # incompatible with that even though subclasses only ever set it once
    # at class level.
    """Общий HTTP-контракт технического провайдера распознавания."""

    kind: TranscriptionProviderKind

    def __init__(self, config: TranscriptionConfig) -> None:
        """Настроить HttpTranscriptionProvider из переданных параметров и зависимостей."""
        self.config = config
        self.model = config.model
        self.audio_window_seconds = config.audio_window_seconds

    @abstractmethod
    def build_http_request(self, request: TranscriptionRequest) -> TranscriptionHttpRequest:
        """Преобразовать DTO записи в технический HTTP-запрос провайдера."""
        ...

    @abstractmethod
    def parse_payload(
        self,
        payload: object,
        request: TranscriptionRequest,
    ) -> TranscriptionResult:
        """Преобразовать ответ провайдера в DTO результата распознавания."""
        ...

    def auth_headers(self) -> dict[str, str]:
        """Собрать заголовки авторизации провайдера; без ключа вернуть пустой набор."""
        if not self.config.api_key:
            return {}
        return {"Authorization": f"Bearer {self.config.api_key}"}

    def evaluate_truncation(self, duration_seconds: float | None) -> TruncationCheck:
        """Проверить признаки усечения ответа относительно объявленного окна движка."""
        window = self.audio_window_seconds
        if window is None:
            return "not_applicable"
        if duration_seconds is None:
            return "unavailable"
        if duration_seconds >= window - _TRUNCATION_MARGIN_SECONDS:
            return "suspected"
        return "clear"

    async def transcribe(
        self,
        request: TranscriptionRequest,
        *,
        execution: AdapterExecution,
        client: httpx.AsyncClient,
    ) -> TranscriptionResult:
        """Выполнить распознавание и преобразовать ответ провайдера в DTO."""
        http_request = self.build_http_request(request)

        try:
            response = await execution.post(
                client,
                http_request.url,
                headers=http_request.headers,
                json=http_request.json_body,
                data=http_request.data,
                files=http_request.files,
            )
        except AdapterRequestError as exc:
            raise TranscriptionError(exc.reason, status_code=exc.status_code) from exc
        except TimeoutError as exc:
            raise TranscriptionError("timeout") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise TranscriptionError(
                "provider_error",
                status_code=response.status_code,
                detail_code="malformed_response",
            ) from exc

        return self.parse_payload(payload, request)
