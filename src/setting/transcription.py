"""Параметры HTTP-транспорта Whisper из настроек SPEECH_WHISPER_*."""

from collections.abc import Mapping
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class TranscriptionConfig(BaseModel):
    """Принять параметры HTTP-вызова и ответов существующего Whisper."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["openai_compatible"]
    base_url: str
    api_key: str = Field(repr=False)
    model: str
    timeout_seconds: float
    audio_window_seconds: float | None
    response_format: str
    extra_options: Mapping[str, str] = Field(default_factory=dict)
