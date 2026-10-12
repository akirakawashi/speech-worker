"""Получение промпта и параметров через служебный контракт бэкенда 1.0."""

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from src.application.dto.model_execution import ModelPreparationRequest, PreparedModelRequest
from src.application.errors.external import AnalysisError


class BackendModelRequestPreparer(BaseModel):
    """Запросить готовый DTO; исходники и промпты бэкенда воркер не читает."""

    model_config = ConfigDict(frozen=True)
    base_url: str
    api_token: str = Field(repr=False)
    timeout_seconds: float = Field(default=30.0, gt=0)

    async def prepare(self, request: ModelPreparationRequest) -> PreparedModelRequest:
        """Отправить входной DTO и проверить версию и назначение ответа.

        Отказы нормализуются в AnalysisError без текста разговора или токена.
        Неизвестная версия и несовпавший сценарий не отправляются модели.
        """
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.post(
                    f"{self.base_url}/internal/v1/model-requests",
                    headers={"Authorization": f"Bearer {self.api_token}"},
                    json=request.model_dump(mode="json"),
                )
        except httpx.TimeoutException as exc:
            raise AnalysisError("backend_timeout", retryable=True) from exc
        except httpx.HTTPError as exc:
            raise AnalysisError("backend_unreachable", retryable=True) from exc
        if response.status_code >= 400:
            raise AnalysisError(
                f"backend_http_{response.status_code}",
                retryable=response.status_code in (408, 429) or response.status_code >= 500,
            )
        try:
            prepared = PreparedModelRequest.model_validate_json(response.content)
        except ValidationError:
            raise AnalysisError("invalid_model_request", retryable=False) from None
        if prepared.scenario != request.scenario or prepared.profile != request.profile:
            raise AnalysisError("model_request_mismatch", retryable=False)
        return prepared
