"""Resolution of the active transcription provider from settings."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Literal, cast

from pydantic import BaseModel, ConfigDict, Field

from src.setting.config import Settings
from src.setting.providers import TranscriptionProviderKind

logger = logging.getLogger(__name__)

_LEGACY_WARNING_EMITTED = False


class TranscriptionConfig(BaseModel):
    """Проверенные настройки выбранного провайдера распознавания."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    kind: TranscriptionProviderKind
    base_url: str
    api_key: str = Field(repr=False)
    model: str
    timeout_seconds: float
    audio_window_seconds: float | None
    response_format: str
    source: Literal["transcription", "legacy_openrouter"]
    base_url_setting: str
    # Engine-specific request parameters passed through untouched, e.g.
    # {"vad_filter": "true"} for faster-whisper. Kept in configuration rather
    # than in the providers because the same `openai_compatible` transport
    # serves engines that accept different knobs.
    extra_options: Mapping[str, str] = Field(default_factory=dict)


def _warn_once_about_legacy_settings() -> None:
    """Однократно сообщить об использовании прежних настроек провайдера."""
    global _LEGACY_WARNING_EMITTED
    if _LEGACY_WARNING_EMITTED:
        return
    _LEGACY_WARNING_EMITTED = True
    logger.warning(
        "transcription: OPENROUTER_* settings are deprecated, migrate to TRANSCRIPTION_*"
    )


def _resolve_enabled(settings: Settings) -> bool:
    """Разрешить флаг доступности движка по явным настройкам."""
    if settings.transcription_enabled is not None:
        return settings.transcription_enabled
    return settings.openrouter_enabled


def resolve_transcription_config(settings: Settings) -> TranscriptionConfig | None:
    """Return the active provider configuration, or None when unavailable.

    Legacy and explicit modes never mix field by field: a half-filled
    TRANSCRIPTION_* block fails closed instead of silently inheriting a base URL
    or model from the deprecated OPENROUTER_* block, which would produce a
    hybrid nobody configured on purpose.
    """

    from src.setting.providers import require_provider_spec

    if not _resolve_enabled(settings):
        return None

    provider = settings.transcription_provider.strip()
    if provider:
        kind = cast(TranscriptionProviderKind, provider)
        base_url = settings.transcription_base_url.strip()
        api_key = settings.transcription_api_key.strip()
        model = settings.transcription_model.strip()
        timeout_seconds = (
            settings.transcription_timeout_seconds
            if settings.transcription_timeout_seconds is not None
            else float(settings.openrouter_timeout_seconds)
        )
        window = settings.transcription_audio_window_seconds
        audio_window_seconds = window if window > 0 else None
        response_format = settings.transcription_response_format.strip() or "verbose_json"
        extra_options = dict(settings.transcription_extra_options)
        source: Literal["transcription", "legacy_openrouter"] = "transcription"
        base_url_setting = "transcription_base_url"
    else:
        kind = "openrouter"
        base_url = str(settings.openrouter_base_url).strip()
        api_key = settings.openrouter_api_key.strip()
        model = settings.openrouter_transcription_model.strip()
        timeout_seconds = float(settings.openrouter_timeout_seconds)
        audio_window_seconds = None
        response_format = "verbose_json"
        extra_options = {}
        source = "legacy_openrouter"
        base_url_setting = "openrouter_base_url"

    if not base_url or not model:
        return None

    spec = require_provider_spec(kind)
    if spec.requires_api_key and not api_key:
        return None

    if source == "legacy_openrouter":
        _warn_once_about_legacy_settings()

    return TranscriptionConfig(
        kind=kind,
        base_url=base_url.rstrip("/"),
        api_key=api_key,
        model=model,
        timeout_seconds=timeout_seconds,
        audio_window_seconds=audio_window_seconds,
        response_format=response_format,
        source=source,
        base_url_setting=base_url_setting,
        extra_options=extra_options,
    )


def transcription_available(settings: Settings) -> bool:
    """Проверить, настроен ли доступный провайдер распознавания."""
    return resolve_transcription_config(settings) is not None


def transcription_enabled(settings: Settings) -> bool:
    """Whether transcription is switched on, independent of provider config.

    Lets callers distinguish "the feature is off" from "the feature is on but
    misconfigured" without duplicating the OPENROUTER_ENABLED fallback rule.
    """

    return _resolve_enabled(settings)
