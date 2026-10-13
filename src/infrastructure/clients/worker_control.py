"""Служебный HTTP-клиент бэкенда: проверенные DTO и нормализованные отказы."""

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from src.application.dto.calls import JobErrorClass
from src.application.dto.maintenance import (
    MaintenancePlan,
    MaintenanceRequest,
)
from src.application.pipeline.stages import StageError


class BackendWorkerControl(BaseModel):
    """Обращаться к бэкенду по служебному ключу, не раскрывая его и тело разговора."""

    model_config = ConfigDict(frozen=True)
    base_url: str
    api_token: str = Field(repr=False)
    timeout_seconds: float = Field(default=30, gt=0)

    async def _post[T: BaseModel](self, route: str, command: BaseModel, result_type: type[T]) -> T:
        """Отправить DTO, проверить ответ; в StageError оставить только устойчивый класс."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.post(
                    f"{self.base_url}/internal/v1/{route}",
                    headers={"Authorization": f"Bearer {self.api_token}"},
                    json=command.model_dump(mode="json"),
                )
        except httpx.TimeoutException as exc:
            raise StageError(JobErrorClass.TIMEOUT, "backend request timed out") from exc
        except httpx.HTTPError as exc:
            raise StageError(JobErrorClass.PROVIDER_ERROR, "backend is unavailable") from exc
        status = response.status_code
        if status >= 400:
            if status == 404:
                error = JobErrorClass.NOT_FOUND
            elif status == 409 or status in (408, 429) or status >= 500:
                error = JobErrorClass.PROVIDER_ERROR
            else:
                error = JobErrorClass.INVALID_OUTPUT
            raise StageError(error, f"backend refused the request: http_{status}")
        try:
            return result_type.model_validate_json(response.content)
        except ValidationError:
            raise StageError(JobErrorClass.INVALID_OUTPUT, "invalid backend response") from None

    async def plan(self, command: MaintenanceRequest) -> MaintenancePlan:
        """Получить срок и команду; несовпавшее действие не выполняется."""
        plan = await self._post("worker-plan", command, MaintenancePlan)
        if plan.action != command.action:
            raise StageError(JobErrorClass.INVALID_OUTPUT, "maintenance action mismatch")
        return plan
