"""Данные этапов конвейера и результат перехода к следующему этапу."""

from __future__ import annotations

from typing import ClassVar

from pydantic import BaseModel, ConfigDict

from src.domain.calls.contracts import JobKind, ProcessingStatus, RoleMethod, Speaker


class IngestWork(BaseModel):
    """Что долгая фаза выяснила о записи.

    Один тип на все три исхода, а не три: короткая фаза обязана записать что-то
    в любом случае, и развилка по `outcome` в ней читается лучше, чем три
    обработчика с общей половиной.
    """

    model_config = ConfigDict(frozen=True)

    outcome: str
    object_key: str | None = None
    audio_bytes: int | None = None
    channels: int | None = None
    duration_s: int | None = None
    #: Что о звонке известно из его проекта. `None` — проект не сопоставлен, и
    #: значение выведет этап анализа.
    segment: str | None = None
    product: str | None = None

    STORED: ClassVar[str] = "stored"
    SKIPPED_SHORT: ClassVar[str] = "skipped_short"
    NO_AUDIO: ClassVar[str] = "no_audio"


class Reply(BaseModel):
    """Реплика после атрибуции."""

    model_config = ConfigDict(frozen=True)

    seq: int
    speaker: Speaker
    start_ms: int
    end_ms: int
    text: str


class TranscribeWork(BaseModel):
    """Что долгая фаза передаёт короткой."""

    model_config = ConfigDict(frozen=True)

    engine: str
    model_name: str
    language: str
    role_method: RoleMethod
    duration_s: float | None
    replies: tuple[Reply, ...]


class StageResult(BaseModel):
    """Что этап сделал со звонком и что делать дальше."""

    model_config = ConfigDict(frozen=True)

    #: Новый статус звонка в транзакции результата. None сохраняет текущий статус.
    processing_status: ProcessingStatus | None
    #: Следующий этап. `None` — цепочка на этом звонке закончена, и это
    #: нормальный исход не только для последнего этапа: короткий звонок
    #: заканчивает её на первом.
    next_kind: JobKind | None = None
