"""Снимки просроченных записей и политики хранения."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from src.domain.retention.policy import RetentionPolicy as RetentionPolicy


class ExpiredRecording(BaseModel):
    """Метаданные записи, которой закончился срок хранения."""

    model_config = ConfigDict(frozen=True)
    id: UUID
    session_id: str
    started_at: datetime
    audio_object_key: str | None
    audio_bytes: int | None


class AudioRetentionStats(BaseModel):
    """Число, объём и дата самой старой просроченной записи."""

    model_config = ConfigDict(frozen=True)
    calls: int
    bytes: int
    oldest: datetime | None
