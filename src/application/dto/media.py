"""Измеренные параметры записи и данные её каналов."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from src.domain.transcription.channels import ChannelEnvelope as ChannelEnvelope


class AudioProbe(BaseModel):
    """Измеренные свойства записи."""

    model_config = ConfigDict(frozen=True)

    duration_s: float
    channels: int
    sample_rate: int | None
    bit_rate: int | None
    codec: str | None

    @property
    def is_stereo(self) -> bool:
        """Проверить наличие как минимум двух каналов у записи."""
        return self.channels >= 2
