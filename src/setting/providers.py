"""Описание поддерживаемых провайдеров без создания HTTP-клиентов."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

TranscriptionProviderKind = Literal["openrouter", "openai_compatible"]
AdapterFailureReason = Literal[
    "not_configured", "unauthorized", "forbidden", "timeout", "provider_error", "unexpected_error"
]

TRANSCRIPTION_FAILURE_REASONS: tuple[AdapterFailureReason, ...] = (
    "not_configured",
    "unauthorized",
    "forbidden",
    "timeout",
    "provider_error",
    "unexpected_error",
)


class TranscriptionProviderSpec(BaseModel):
    """Поддерживаемые возможности и формат одного провайдера распознавания."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    kind: TranscriptionProviderKind
    requires_api_key: bool
    transcription_path: str
    health_probe_path: str
    # Where health_probe_path is anchored. "base" appends it to the configured
    # base URL (which ends in /v1); "origin" drops the base path and probes the
    # scheme+host root instead -- self-hosted ASR servers put /health there,
    # outside the OpenAI-compatible /v1 namespace.
    health_probe_scope: Literal["base", "origin"]
    network_zone: Literal["external", "self_hosted"]
    auth_mode: Literal["service_credential", "optional_service_credential"]
    owner: str = "aicistent"
    version: str = "1.0.0"
    future_service: Literal["adapter-transcription"] = "adapter-transcription"
    operation_type: Literal["read"] = "read"
    retry_mode: Literal["none"] = "none"
    failure_mode: Literal["fail_closed"] = "fail_closed"
    failure_reasons: tuple[AdapterFailureReason, ...] = TRANSCRIPTION_FAILURE_REASONS


PROVIDER_SPECS: dict[TranscriptionProviderKind, TranscriptionProviderSpec] = {
    "openrouter": TranscriptionProviderSpec(
        kind="openrouter",
        requires_api_key=True,
        transcription_path="/audio/transcriptions",
        health_probe_path="/models",
        health_probe_scope="base",
        network_zone="external",
        auth_mode="service_credential",
    ),
    "openai_compatible": TranscriptionProviderSpec(
        kind="openai_compatible",
        # A self-hosted vLLM endpoint reached over a tunnel carries no
        # credential. Requiring one here would make a valid deployment
        # unconfigurable.
        requires_api_key=False,
        transcription_path="/audio/transcriptions",
        # Deliberately not /v1/models: on faster-whisper-server that endpoint
        # enumerates models through the HuggingFace API and hangs when egress
        # is blocked, so a working server would be reported as timed out.
        health_probe_path="/health",
        health_probe_scope="origin",
        network_zone="self_hosted",
        auth_mode="optional_service_credential",
    ),
}


def require_provider_spec(kind: TranscriptionProviderKind) -> TranscriptionProviderSpec:
    """Вернуть описание зарегистрированного провайдера; неизвестный ключ отклонить."""
    spec = PROVIDER_SPECS.get(kind)
    if spec is None:
        raise ValueError(f"Unknown transcription provider kind: {kind!r}")
    return spec
