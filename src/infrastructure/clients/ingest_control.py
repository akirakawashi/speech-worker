"""HTTP-адаптер технических команд ingest без локальных бизнес-решений."""

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from src.application.dto.calls import JobErrorClass
from src.application.dto.ingest_control import (
    IngestClaim,
    IngestPlan,
    IngestResolution,
    ResolveIngest,
)
from src.application.pipeline.stages import StageDeferred, StageError


class BackendIngestControl(BaseModel):
    """Проверять HTTP-контракт и связь ответа с захватом, не раскрывая URL записи/ключи."""

    model_config = ConfigDict(frozen=True)
    base_url: str
    api_token: str = Field(repr=False)
    timeout_seconds: float = Field(default=30, gt=0)

    async def _post[T: BaseModel](self, route: str, command: BaseModel, result_type: type[T]) -> T:
        """Отправить DTO; временный HTTP-отказ отложить без расхода попытки скачивания."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.post(
                    f"{self.base_url}/internal/v1/ingest/{route}",
                    headers={"Authorization": f"Bearer {self.api_token}"},
                    json=command.model_dump(mode="json"),
                )
        except httpx.TimeoutException:
            raise StageDeferred(JobErrorClass.TIMEOUT, "ingest_control_timeout") from None
        except httpx.HTTPError:
            raise StageDeferred(
                JobErrorClass.PROVIDER_ERROR, "ingest_control_unavailable"
            ) from None
        status = response.status_code
        if status in (408, 409, 429) or status >= 500:
            raise StageDeferred(JobErrorClass.PROVIDER_ERROR, f"ingest_control_http_{status}")
        if status >= 400:
            error = JobErrorClass.NOT_FOUND if status == 404 else JobErrorClass.INVALID_OUTPUT
            raise StageError(error, f"ingest_control_http_{status}")
        try:
            return result_type.model_validate_json(response.content)
        except ValidationError:
            raise StageError(JobErrorClass.INVALID_OUTPUT, "invalid_ingest_command") from None

    async def prepare(self, claim: IngestClaim) -> IngestPlan:
        """Получить команду конкретного захвата; ответ другого задания отклонить."""
        result = await self._post("prepare", claim, IngestPlan)
        if result.claim != claim:
            raise StageError(JobErrorClass.INVALID_OUTPUT, "ingest_claim_mismatch")
        return result

    async def resolve(self, command: ResolveIngest) -> IngestResolution:
        """Передать факты и получить решение для того же nonce."""
        result = await self._post("resolve", command, IngestResolution)
        if result.claim != command.claim:
            raise StageError(JobErrorClass.INVALID_OUTPUT, "ingest_claim_mismatch")
        return result
