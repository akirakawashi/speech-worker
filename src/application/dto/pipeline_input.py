"""Снимки данных, необходимые обработчикам этапов конвейера."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from src.domain.calls.reasons import CallReason


class CallForIngest(BaseModel):
    """Метаданные звонка и проекта для получения записи."""

    model_config = ConfigDict(frozen=True)
    session_id: str
    duration_s: int | None
    started_at: datetime
    audio_status: str
    audio_object_key: str | None
    source_snapshot: dict[str, JsonValue] = Field(default_factory=dict)
    source: str = "ncc"
    source_audio_url: str | None = None
    project_segment: str | None = None
    project_product: str | None = None


class CallForTranscription(BaseModel):
    """Сведения о сохранённой записи, которую предстоит распознать."""

    model_config = ConfigDict(frozen=True)
    session_id: str
    audio_status: str
    audio_object_key: str | None
    audio_channels: int | None = None
    duration_s: float | None = None


class CallForAnalysis(BaseModel):
    """Наличие звонка и метод определения ролей его расшифровки."""

    model_config = ConfigDict(frozen=True)
    id: UUID
    role_method: str | None


class AnalysisSegment(BaseModel):
    """Реплика расшифровки для разбора разговора."""

    model_config = ConfigDict(frozen=True)
    seq: int
    speaker: str
    text_raw: str


class ClassifierRecord(BaseModel):
    """Снимок узла классификатора, включая его доступность для модели."""

    model_config = ConfigDict(frozen=True)
    id: UUID
    parent_id: UUID | None
    level: int
    label: str
    key: str
    sort_order: int = 0
    is_active: bool = True


class AnalysisInput(BaseModel):
    """Согласованные исходные данные одного разбора разговора."""

    model_config = ConfigDict(frozen=True)
    call: CallForAnalysis
    segments: tuple[AnalysisSegment, ...]
    classifier: tuple[ClassifierRecord, ...]
    reasons: tuple[CallReason, ...]
