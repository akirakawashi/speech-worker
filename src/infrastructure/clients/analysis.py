"""Прямой HTTP-вызов LLM по готовому запросу; промпты здесь не формируются."""

import httpx
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from src.application.dto.model_execution import ModelPreparationRequest, ModelProfile, ModelScenario
from src.application.errors.external import (
    AnalysisError as AnalysisError,
    AnalysisNotConfigured as AnalysisNotConfigured,
)
from src.application.ports.model_requests import ModelRequestPreparer


class SpeechAnalysisClient(BaseModel):
    """Исполнить подготовленный запрос и вернуть текст модели для бизнес-проверки."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    base_url: str
    api_key: str = Field(default="", repr=False)
    preparer: ModelRequestPreparer = Field(repr=False)
    scenario: ModelScenario = "analysis"
    profile: ModelProfile = "production"
    timeout_seconds: float = Field(default=120.0, gt=0)

    async def run(self, inputs: dict[str, JsonValue]) -> dict[str, JsonValue]:
        """Получить готовый DTO и вызвать /chat/completions.

        Вход — именованные данные сценария, выход — текст под ключом answer.
        Сетевые ошибки и 408/429/5xx допускают повтор. Неверная конфигурация,
        отказ модели и обрыв по лимиту ответа требуют отдельного решения.
        Тело разговора, ответа и секреты в сообщения ошибок не включаются.
        """
        prepared = await self.preparer.prepare(
            ModelPreparationRequest(scenario=self.scenario, profile=self.profile, inputs=inputs)
        )
        payload = {
            "model": prepared.model,
            "messages": [message.model_dump() for message in prepared.messages],
            "temperature": prepared.temperature,
            "max_tokens": prepared.max_tokens,
        }
        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.post(
                    f"{self.base_url}/chat/completions", headers=headers, json=payload
                )
        except httpx.TimeoutException as exc:
            raise AnalysisError("timeout", retryable=True) from exc
        except httpx.HTTPError as exc:
            raise AnalysisError("unreachable", retryable=True) from exc
        if response.status_code >= 400:
            raise AnalysisError(
                f"http_{response.status_code}",
                retryable=response.status_code in (408, 429) or response.status_code >= 500,
            )
        try:
            body = response.json()
        except ValueError:
            raise AnalysisError("malformed_response", retryable=True) from None
        if not isinstance(body, dict):
            raise AnalysisError("malformed_response", retryable=True)
        choices = body.get("choices")
        if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
            raise AnalysisError("malformed_response", retryable=True)
        choice = choices[0]
        if choice.get("finish_reason") == "length":
            raise AnalysisError("output_truncated", retryable=False)
        message = choice.get("message")
        if not isinstance(message, dict):
            raise AnalysisError("malformed_response", retryable=True)
        if message.get("refusal"):
            raise AnalysisError("model_refused", retryable=False)
        content = message.get("content")
        if not isinstance(content, str) or not content.strip():
            raise AnalysisError("empty_response", retryable=True)
        return {"answer": content}
