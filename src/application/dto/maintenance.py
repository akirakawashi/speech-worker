"""Контракт 1.0: бэкенд планирует обслуживание, воркер выполняет команды."""

from typing import Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from src.application.dto.calls import CallSource
from src.application.dto.daily_ingest import DayShare
from src.application.dto.quality_batch import RunEvaluation

MaintenanceAction = Literal["ncc", "telphin", "quality"]


class MaintenanceRequest(BaseModel):
    """Назвать действие и момент его последнего начала; при первом запуске момент пустой."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    contract_version: Literal["1.0"] = "1.0"
    action: MaintenanceAction
    last_started: AwareDatetime | None = None


class CatchUpCommand(BaseModel):
    """Параметры приёма свежих звонков, выбранные бэкендом."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    source: CallSource
    timezone: str = Field(min_length=1)
    day_share: DayShare | None = None
    priority: int | None = Field(default=None, ge=0)


class MaintenancePlan(BaseModel):
    """Время и одна команда; ожидание не занимает обработчик общей очереди."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    contract_version: Literal["1.0"] = "1.0"
    action: MaintenanceAction
    run_at: AwareDatetime
    wait_seconds: float = Field(default=0, ge=0)
    ingest: CatchUpCommand | None = None
    evaluation: RunEvaluation | None = None

    @model_validator(mode="after")
    def command_matches_action(self) -> Self:
        """Отклонить план без команды или с командой другого источника."""
        if self.action == "quality":
            if self.ingest is not None or self.evaluation is None:
                raise ValueError("quality plan needs an evaluation command")
        elif (
            self.evaluation is not None or self.ingest is None or self.ingest.source != self.action
        ):
            raise ValueError("ingestion plan needs the matching source")
        return self


class QualityQueueRequest(BaseModel):
    """Проверка охвата либо постановка оценки в очередь; сеть модели здесь не нужна."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    contract_version: Literal["1.0"] = "1.0"
    selection: RunEvaluation


class QualityQueueReport(BaseModel):
    """Число кандидатов, реально поставленных заданий либо устаревших оценок."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    contract_version: Literal["1.0"] = "1.0"
    candidates: int = Field(default=0, ge=0)
    enqueued: int = Field(default=0, ge=0)
    stale: int | None = Field(default=None, ge=0)
    applied: bool = False
