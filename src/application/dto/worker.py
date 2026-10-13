"""Проверенные параметры запуска последовательного исполнителя заданий."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.domain.calls.contracts import CallSource, JobKind, JobLane


class WorkerOptions(BaseModel):
    """Проверить полосы и этапы; вернуть неизменяемые параметры запуска."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    lanes: tuple[JobLane, ...] = Field(default=(JobLane.DAILY, JobLane.BACKFILL), min_length=1)
    stages: tuple[JobKind, ...] = Field(default=(JobKind.INGEST, JobKind.TRANSCRIBE), min_length=1)
    once: bool = False
    queue_only: bool = False
    sources: tuple[CallSource, ...] = Field(
        default=(CallSource.NCC, CallSource.TELPHIN), min_length=1
    )

    @field_validator("sources", mode="before")
    @classmethod
    def parse_sources(cls, value: Any) -> Any:
        """Проверить явно выбранные источники автоматического приёма."""
        if isinstance(value, str):
            return tuple(dict.fromkeys(part.strip() for part in value.split(",") if part.strip()))
        return value

    @field_validator("lanes", mode="before")
    @classmethod
    def parse_lanes(cls, value: Any) -> Any:
        """Прочитать строку полос через запятую или передать последовательность в Pydantic."""
        if isinstance(value, str):
            return tuple(part.strip() for part in value.split(",") if part.strip())
        return value

    @field_validator("stages", mode="before")
    @classmethod
    def parse_stages(cls, value: Any) -> Any:
        """Прочитать этапы; пустая строка CLI включает только получение и распознавание."""
        if isinstance(value, str):
            names = tuple(part.strip() for part in value.split(",") if part.strip())
            return names or (JobKind.INGEST, JobKind.TRANSCRIBE)
        return value

    @field_validator("lanes")
    @classmethod
    def unique_lanes(cls, value: tuple[JobLane, ...]) -> tuple[JobLane, ...]:
        """Убрать повторные полосы, сохранив порядок входных параметров."""
        return tuple(dict.fromkeys(value))

    @field_validator("stages")
    @classmethod
    def ordered_stages(cls, value: tuple[JobKind, ...]) -> tuple[JobKind, ...]:
        """Оставить технические этапы в порядке; LLM-задания принадлежат исполнителю бэкенда."""
        if set(value) - {JobKind.INGEST, JobKind.TRANSCRIBE}:
            raise ValueError(
                "speech-worker supports only ingest and transcribe; LLM belongs to backend"
            )
        return tuple(kind for kind in JobKind if kind in value)
