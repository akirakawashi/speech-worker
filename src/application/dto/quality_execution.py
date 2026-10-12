"""Служебный контракт 1.0: продолжение оценки и её атомарный результат."""

from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.application.dto.model_execution import ModelMessage
from src.application.dto.quality import Evaluation
from src.application.dto.quality_batch import LineSnapshot

MAX_QUALITY_COMPLETIONS = 1024


class QualityModelRequest(BaseModel):
    """Готовые сообщения и параметры модели, выбранные бэкендом."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    model: str = Field(min_length=1)
    messages: tuple[ModelMessage, ...] = Field(min_length=1, repr=False)
    temperature: Literal[0] = 0
    max_tokens: int = Field(ge=512, le=32768)
    prompt_version: str = Field(min_length=1)


class QualityTotals(BaseModel):
    """Готовые итоги расчёта бэкенда; воркер сохраняет значения без повторного подсчёта."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    score: int | None = Field(default=None, ge=0, le=100)
    passed_count: int | None = Field(default=None, ge=0)
    stage_count: int | None = Field(default=None, ge=0)
    all_passed: bool | None = None


class QualityWork(BaseModel):
    """Проверенный итог для сохранения вместе с завершением задания очереди."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    snapshot: LineSnapshot = Field(repr=False)
    model_name: str = Field(min_length=1)
    script_version: str = Field(min_length=1)
    evaluation: Evaluation | None = Field(default=None, repr=False)
    totals: QualityTotals | None = None
    failure_reason: Literal["invalid_evaluation", "roles_undetermined", "model_error"] | None = None
    roles: tuple[tuple[int, str], ...] = Field(default=(), repr=False)

    @model_validator(mode="after")
    def one_outcome(self) -> Self:
        """Отклонить результат без исхода или одновременно с оценкой и отказом."""
        if (self.evaluation is None) == (self.failure_reason is None):
            raise ValueError("exactly one quality outcome is required")
        if (self.evaluation is None) != (self.totals is None):
            raise ValueError("quality totals are required exactly for an evaluation")
        if self.evaluation is not None and (
            self.evaluation.model_name != self.model_name
            or self.evaluation.script_version != self.script_version
        ):
            raise ValueError("quality outcome metadata differs")
        return self


class QualityStepRequest(BaseModel):
    """Передать снимок и уже полученные ответы; первый запрос содержит только UUID.

    Бэкенд воспроизводит свой сценарий по ответам, не вызывая модель повторно.
    model_error завершает последнюю неудавшуюся сетевую попытку.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")
    contract_version: Literal["1.0"] = "1.0"
    call_id: UUID
    snapshot: LineSnapshot | None = Field(default=None, repr=False)
    responses: tuple[str, ...] = Field(default=(), max_length=MAX_QUALITY_COMPLETIONS, repr=False)
    model_error: bool = False

    @model_validator(mode="after")
    def continuation_needs_snapshot(self) -> Self:
        """Не принимать ответы, пока бэкенд не выдал исходный снимок разговора."""
        if self.snapshot is None and (self.responses or self.model_error):
            raise ValueError("quality continuation needs a snapshot")
        return self


class QualityStep(BaseModel):
    """Следующий вызов модели либо окончательный результат; оба исхода исключаются."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    contract_version: Literal["1.0"] = "1.0"
    snapshot: LineSnapshot = Field(repr=False)
    request: QualityModelRequest | None = Field(default=None, repr=False)
    result: QualityWork | None = Field(default=None, repr=False)

    @model_validator(mode="after")
    def one_step(self) -> Self:
        """Разрешить только один следующий шаг, с исходным снимком результата."""
        if (self.request is None) == (self.result is None):
            raise ValueError("exactly one quality step is required")
        if self.result is not None and self.result.snapshot != self.snapshot:
            raise ValueError("quality snapshots differ")
        return self
