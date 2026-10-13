"""Служебная доставка распознавания: ограниченные повторы одного и того же DTO."""

import asyncio

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from src.application.dto.calls import JobErrorClass
from src.application.dto.transcription_delivery import (
    TranscriptionDeliveryRequest,
    TranscriptionReceipt,
)
from src.application.ports.transcription_delivery import TranscriptionDeliveryError


class BackendTranscriptionDelivery(BaseModel):
    """Отправить DTO по Bearer-токену; не раскрывать тело запроса и внешних ошибок."""

    model_config = ConfigDict(frozen=True)
    base_url: str
    api_token: str = Field(repr=False)
    timeout_seconds: float = Field(default=30, gt=0)
    max_attempts: int = Field(default=3, ge=1)
    retry_backoff_seconds: float = Field(default=1, ge=0)

    async def deliver(self, command: TranscriptionDeliveryRequest) -> TranscriptionReceipt:
        """Повторять неизменный запрос и вернуть соответствующую ему квитанцию 1.0.

        Таймаут, сетевой отказ, 408/409/429 и 5xx повторяются ограниченно,
        затем возвращают нормализованный повторяемый отказ. Прочие HTTP-коды
        и неверная квитанция требуют исправления. Отмена не подавляется.
        """
        payload = command.model_dump(mode="json")
        async with httpx.AsyncClient(
            timeout=self.timeout_seconds, follow_redirects=False
        ) as client:
            for attempt in range(1, self.max_attempts + 1):
                error: TranscriptionDeliveryError | None = None
                try:
                    response = await client.post(
                        f"{self.base_url.rstrip('/')}/internal/v1/transcription-results",
                        headers={"Authorization": f"Bearer {self.api_token}"},
                        json=payload,
                    )
                except httpx.TimeoutException:
                    error = TranscriptionDeliveryError(
                        JobErrorClass.TIMEOUT, "timeout", retryable=True
                    )
                except httpx.HTTPError:
                    error = TranscriptionDeliveryError(
                        JobErrorClass.PROVIDER_ERROR, "unavailable", retryable=True
                    )
                else:
                    if response.status_code == 200:
                        return self._receipt(command, response.content)
                    status = response.status_code
                    retryable = status in (408, 409, 429) or status >= 500
                    error = TranscriptionDeliveryError(
                        JobErrorClass.PROVIDER_ERROR
                        if retryable
                        else (
                            JobErrorClass.NOT_FOUND
                            if status == 404
                            else JobErrorClass.INVALID_OUTPUT
                        ),
                        f"http_{status}",
                        retryable=retryable,
                    )
                if not error.retryable or attempt == self.max_attempts:
                    raise error from None
                await asyncio.sleep(self.retry_backoff_seconds * attempt)
        raise RuntimeError("transcription delivery did not execute")

    @staticmethod
    def _receipt(command: TranscriptionDeliveryRequest, content: bytes) -> TranscriptionReceipt:
        """Проверить версию и идентификаторы подтверждения; неверный ответ не завершает задание."""
        try:
            receipt = TranscriptionReceipt.model_validate_json(content)
        except ValidationError:
            raise TranscriptionDeliveryError(
                JobErrorClass.INVALID_OUTPUT, "invalid_receipt", retryable=False
            ) from None
        claim = command.claim
        if (receipt.call_id, receipt.job_id, receipt.claim_token, receipt.attempt) != (
            claim.call_id,
            claim.job_id,
            claim.claim_token,
            claim.attempt,
        ):
            raise TranscriptionDeliveryError(
                JobErrorClass.INVALID_OUTPUT, "receipt_mismatch", retryable=False
            )
        return receipt
