"""Проверенные параметры запуска последовательного исполнителя заданий."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.domain.calls.contracts import JobKind, JobLane


class WorkerOptions(BaseModel):
    """Проверить полосы и этапы; вернуть неизменяемые параметры запуска."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    lanes: tuple[JobLane, ...] = Field(default=(JobLane.DAILY,), min_length=1)
    stages: tuple[JobKind, ...] = Field(default=tuple(JobKind), min_length=1)
    once: bool = False

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
        """Прочитать строку этапов; пустая строка CLI включает все существующие этапы."""
        if isinstance(value, str):
            names = tuple(part.strip() for part in value.split(",") if part.strip())
            return names or tuple(JobKind)
        return value

    @field_validator("lanes")
    @classmethod
    def unique_lanes(cls, value: tuple[JobLane, ...]) -> tuple[JobLane, ...]:
        """Убрать повторные полосы, сохранив порядок входных параметров."""
        return tuple(dict.fromkeys(value))

    @field_validator("stages")
    @classmethod
    def ordered_stages(cls, value: tuple[JobKind, ...]) -> tuple[JobKind, ...]:
        """Вернуть выбранные этапы в порядке конвейера без повторений."""
        return tuple(kind for kind in JobKind if kind in value)
