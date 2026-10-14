"""Снимки данных, необходимые обработчикам этапов конвейера."""

from pydantic import BaseModel, ConfigDict


class CallForTranscription(BaseModel):
    """Сведения о сохранённой записи, которую предстоит распознать."""

    model_config = ConfigDict(frozen=True)
    session_id: str
    audio_status: str
    audio_object_key: str | None
    audio_channels: int | None = None
    duration_s: float | None = None
