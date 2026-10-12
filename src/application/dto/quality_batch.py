"""Запросы и отчёты пакетной оценки разговоров."""

from __future__ import annotations

import uuid
from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field

from src.application.dto.quality import CURRENT_SCRIPT, CallScript, TranscriptLine


class CallRef(BaseModel):
    """Идентификатор звонка и его сессии для пакетной оценки."""

    model_config = ConfigDict(frozen=True)
    call_id: uuid.UUID
    session_id: str


class EvaluateReport(BaseModel):
    """Число оценённых, неприменимых и неудавшихся разговоров."""

    model_config = ConfigDict(frozen=False)
    candidates: int
    evaluated: int = 0
    not_applicable: int = 0
    failed: int = 0
    applied: bool = False
    failures: list[str] = Field(default_factory=list)

    def describe(self) -> str:
        """Вернуть читаемую сводку результата для вывода оператору."""
        if not self.applied:
            return f"звонков к оценке: {self.candidates}"
        return (
            f"звонков к оценке: {self.candidates}; оценено по скрипту: {self.evaluated}; "
            f"скрипт не применим: {self.not_applicable}; не оценено: {self.failed}"
        )


class LineSnapshot(BaseModel):
    """Прочитанные реплики и серверный момент чтения для защиты от устаревшей записи."""

    model_config = ConfigDict(frozen=True)
    lines: tuple[TranscriptLine, ...]
    read_at: datetime


class CandidateSelection(BaseModel):
    """Охват оценки по периоду, редакции скрипта и режиму повторной обработки."""

    model_config = ConfigDict(frozen=True)
    date_from: datetime | None = None
    date_to: datetime | None = None
    force: bool = False
    skip_failed: bool = False
    limit: int | None = None
    script: CallScript = CURRENT_SCRIPT


class RunEvaluation(BaseModel):
    """Параметры команды оценки после разбора аргументов точки входа."""

    model_config = ConfigDict(frozen=True)
    first: date | None = None
    last: date | None = None
    call: str | None = None
    everything: bool = False
    limit: int | None = None
    force: bool = False
    skip_failed: bool = False
    drop_stale: bool = False
    apply: bool = False


class BatchResult(BaseModel):
    """Отчёт оценки либо число устаревших записей сверки."""

    model_config = ConfigDict(frozen=True)
    report: EvaluateReport | None = None
    stale: int | None = None
