"""Данные регистрации звонков, отчёт прогона и параметры команды приёма."""

from datetime import date
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from src.domain.calls.scheduling import (
    DEFERRED_PRIORITY_STEP as DEFERRED_PRIORITY_STEP,
    MAX_RUN_PRIORITY as MAX_RUN_PRIORITY,
    DayShare as DayShare,
)


class DailyRunReport(BaseModel):
    """Чем закончился прогон. Печатается оператору и ложится в курсор."""

    model_config = ConfigDict(frozen=True)
    day: date
    discovered: int
    enqueued: int
    already_known: int
    skipped: bool = False
    skip_reason: str | None = None
    limited_to: int | None = None
    collection_off: int = 0
    classifier_created: int = 0
    deferred: int = 0

    def describe(self) -> str:
        """Вернуть читаемую сводку результата для вывода оператору."""
        if self.skipped:
            return f"{self.day}: пропущен — {self.skip_reason}"
        described = (
            f"{self.day}"
            ": найдено "
            f"{self.discovered}"
            ", поставлено в очередь "
            f"{self.enqueued}"
            ", уже было "
            f"{self.already_known}"
        )
        if self.deferred:
            described += f"; из поставленных до ночи отложено {self.deferred}"
        if self.collection_off:
            described += f"; не заведено {self.collection_off} — сбор проекта выключен"
        if self.classifier_created:
            described += f"; новых узлов классификатора {self.classifier_created}"
        if self.limited_to is not None:
            described += f"; взято первых {self.limited_to} — сутки не отмечены обработанными"
        return described


class ProjectForIngest(BaseModel):
    """Проект, сопоставленный источнику, и его разрешение на приём."""

    model_config = ConfigDict(frozen=True)
    id: UUID
    ingest_enabled: bool


class RegisteredCall(BaseModel):
    """Идентификатор зарегистрированного звонка и признак новой записи."""

    model_config = ConfigDict(frozen=True)
    call_id: UUID
    is_new: bool


class IngestSettings(BaseModel):
    """Проверенный снимок пределов попыток для постановки этапов."""

    model_config = ConfigDict(frozen=True)
    max_attempts: dict[str, int]
