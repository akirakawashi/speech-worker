"""Снимки данных, необходимые обработчикам этапов конвейера."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, JsonValue


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
