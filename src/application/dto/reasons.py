"""Причины обращения и вход пакетного заполнения."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from src.domain.calls.reasons import (
    CallReason as CallReason,
    ReasonAnswer as ReasonAnswer,
)


class FillReport(BaseModel):
    """Число заполненных причин, отказов и ответов без подходящего кода."""

    model_config = ConfigDict(frozen=False)

    candidates: int
    filled: int = 0
    #: Модель трижды не дала кода из справочника — звонок останется в следующий раз.
    unanswered: int = 0
    #: LLM недоступен после повторов.
    failed: int = 0
    applied: bool = False
    seconds: float = 0.0

    def describe(self) -> str:
        """Вернуть читаемую сводку результата для вывода оператору."""
        if not self.applied:
            return f"звонков без причины: {self.candidates}; взяла бы все"
        return (
            f"звонков без причины: {self.candidates}; записано: {self.filled}; "
            f"без ответа модели: {self.unanswered}; сбой LLM: {self.failed}; "
            f"{self.seconds:.0f} с"
        )


class ReasonSelection(BaseModel):
    """Отбор по периоду разговора либо по времени расшифровки."""

    model_config = ConfigDict(frozen=True)
    date_from: datetime | None = None
    date_to: datetime | None = None
    transcribed_since: datetime | None = None
    limit: int | None = None


class ReasonCandidates(BaseModel):
    """Снимок звонков без причины и доступного модели справочника."""

    model_config = ConfigDict(frozen=True)
    call_ids: tuple[UUID, ...]
    reasons: tuple[CallReason, ...]
