"""Параметры и сводка постановки звонков на повторный анализ."""

from datetime import datetime
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict

MISSING_LABEL: Final[dict[str, str]] = {"path": "без пути ИИ", "reason": "без причины обращения"}


class ReanalyzeReport(BaseModel):
    """Число кандидатов, поставленных звонков и причин пропуска."""

    model_config = ConfigDict(frozen=True)

    candidates: int
    queued: int = 0
    #: Конвейер звонка оказался занят: задание появилось после выборки.
    busy: int = 0
    #: Звонок удалён между выборкой и постановкой.
    gone: int = 0
    applied: bool = False
    missing: str = "path"

    def describe(self) -> str:
        """Вернуть читаемую сводку результата для вывода оператору."""
        what = MISSING_LABEL[self.missing]
        if not self.applied:
            return f"звонков {what}: {self.candidates}; поставлено было бы: {self.candidates}"
        return (
            f"звонков {what}: {self.candidates}; поставлено на анализ: {self.queued}; "
            f"пропущено, конвейер занят: {self.busy}; звонок удалён: {self.gone}"
        )


class ReanalyzeSelection(BaseModel):
    """Период разговора и отсутствующий результат анализа."""

    model_config = ConfigDict(frozen=True)
    date_from: datetime
    date_to: datetime
    missing: Literal["path", "reason"] = "path"
    limit: int | None = None
