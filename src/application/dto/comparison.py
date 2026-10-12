"""Снимки звонков и результатов сравнения моделей без текста в отчёте."""

import uuid

from pydantic import BaseModel, ConfigDict

from src.application.pipeline.analyze import AnalyzeWork


class Sampled(BaseModel):
    """Звонок выборки и то, что о нём уже записано."""

    model_config = ConfigDict(frozen=True)

    call_id: uuid.UUID
    operator_node_id: uuid.UUID
    ai_node_id: uuid.UUID
    sentiment: str | None
    reason_code: str | None


class Outcome(BaseModel):
    """Один разбор одного звонка одним приложением."""

    model_config = ConfigDict(frozen=True)

    work: AnalyzeWork | None
    error: str | None
    seconds: float


class Row(BaseModel):
    """Сохранённый анализ звонка и результаты сравниваемых моделей."""

    model_config = ConfigDict(frozen=True)

    call: Sampled
    candidate: Outcome | None
    control: Outcome | None


class ReasonOutcome(BaseModel):
    """Причина одного звонка от одного приложения."""

    model_config = ConfigDict(frozen=True)

    code: str | None
    #: LLM не ответил и после повторов `ask_reason`.
    failed: bool
    seconds: float

    @property
    def status(self) -> str:
        """Вернуть результат причины: код, отсутствие кода либо отказ модели."""
        if self.failed:
            return "failed"
        return "ok" if self.code else "no_code"


class ReasonRow(BaseModel):
    """Сохранённый код причины и результаты кандидата и контроля."""

    model_config = ConfigDict(frozen=True)

    call_id: uuid.UUID
    stored: str
    candidate: ReasonOutcome
    control: ReasonOutcome | None
