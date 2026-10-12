"""Проверенные параметры обработки одного задания без секретов окружения."""

from copy import deepcopy
from typing import cast

from pydantic import BaseModel, ConfigDict, Field

from src.application.dto.calls import SPEECH_DEFAULT_SETTINGS


class PipelineSettings(BaseModel):
    """Снимок администраторских настроек для обработчиков этапов."""

    model_config = ConfigDict(frozen=True)
    retention_days: int = Field(default=cast(int, SPEECH_DEFAULT_SETTINGS["retention_days"]), ge=1)
    short_call_threshold_s: int = Field(
        default=cast(int, SPEECH_DEFAULT_SETTINGS["short_call_threshold_s"]), ge=0
    )
    stereo_operator_channel: int = Field(
        default=cast(int, SPEECH_DEFAULT_SETTINGS["stereo_operator_channel"]), ge=0, le=1
    )
    stereo_role_method: str = cast(str, SPEECH_DEFAULT_SETTINGS["stereo_role_method"])
    transcribe_concurrency: int = Field(
        default=cast(int, SPEECH_DEFAULT_SETTINGS["transcribe_concurrency"]), ge=1
    )
    backfill_window_days: int = Field(
        default=cast(int, SPEECH_DEFAULT_SETTINGS["backfill_window_days"]), ge=1
    )
    daily_ingest_at: str = cast(str, SPEECH_DEFAULT_SETTINGS["daily_ingest_at"])
    max_attempts: dict[str, int] = Field(
        default_factory=lambda: deepcopy(
            cast(dict[str, int], SPEECH_DEFAULT_SETTINGS["max_attempts"])
        )
    )
