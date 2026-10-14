"""Служебный контракт 1.0: техническое получение записи по решению бэкенда."""

from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.application.dto.calls import JobKind
from src.application.dto.pipeline import IngestWork, StageResult
from src.application.dto.sources import CallRef


class IngestClaim(BaseModel):
    """Опознать конкретный живой захват ingest; служебный ключ передаётся отдельно."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    contract_version: Literal["1.0"] = "1.0"
    call_id: UUID
    job_id: UUID
    claim_token: UUID
    worker_id: str = Field(min_length=1, max_length=255)
    attempt: int = Field(ge=1)


class IngestDecision(BaseModel):
    """Готовая запись технического итога и перехода; воркер применяет их одной транзакцией."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    work: IngestWork
    stage: StageResult

    @model_validator(mode="after")
    def technical_stage_only(self) -> Self:
        """Не принимать команду постановки LLM-задания в технический воркер."""
        if self.stage.next_kind not in (None, JobKind.TRANSCRIBE):
            raise ValueError("ingest control accepts only a technical next stage")
        return self


class FetchRecording(BaseModel):
    """Откуда скачать, куда положить и нужно ли сначала проверить уже сохранённый объект."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    ref: CallRef = Field(repr=False)
    object_key: str = Field(min_length=1)
    check_stored: bool = False


class IngestPlan(BaseModel):
    """Одна команда скачивания либо готовое завершение без скачивания."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    contract_version: Literal["1.0"] = "1.0"
    claim: IngestClaim
    fetch: FetchRecording | None = None
    completion: IngestDecision | None = None

    @model_validator(mode="after")
    def one_command(self) -> Self:
        """Отклонить отсутствие команды и неоднозначный ответ."""
        if (self.fetch is None) == (self.completion is None):
            raise ValueError("ingest plan needs exactly one command")
        return self


class IngestObservation(BaseModel):
    """Факты скачивания; отсутствие записи ещё не означает конец её ожидания."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    outcome: Literal["stored", "missing"]
    object_key: str | None = Field(default=None, min_length=1)
    audio_bytes: int | None = Field(default=None, ge=1)
    channels: int | None = Field(default=None, ge=1)
    duration_s: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def measurements_match_outcome(self) -> Self:
        """Потребовать ключ сохранённого файла и запретить измерения отсутствующего."""
        values = (self.object_key, self.audio_bytes, self.channels, self.duration_s)
        if self.outcome == "stored" and self.object_key is None:
            raise ValueError("stored observation needs an object key")
        if self.outcome == "missing" and any(value is not None for value in values):
            raise ValueError("missing observation cannot contain measurements")
        return self


class ResolveIngest(BaseModel):
    """Передать факты скачивания для решения бэкенда о повторе или следующем этапе."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    contract_version: Literal["1.0"] = "1.0"
    claim: IngestClaim
    observation: IngestObservation


class IngestResolution(BaseModel):
    """Повторить попытку получения либо применить готовый итог; без выбора правил воркером."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    contract_version: Literal["1.0"] = "1.0"
    claim: IngestClaim
    retry: bool = False
    completion: IngestDecision | None = None

    @model_validator(mode="after")
    def one_outcome(self) -> Self:
        """Запретить пустой ответ и одновременные повтор/завершение."""
        if self.retry == (self.completion is not None):
            raise ValueError("ingest resolution needs retry or completion")
        return self
