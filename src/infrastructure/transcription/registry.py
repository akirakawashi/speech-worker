"""Provider registry for speech transcription.

Transcription deliberately stays out of `CORPORATE_ADAPTERS`: `AdapterSpec`
requires a Robot Fedor tool name and a `fail_open` contract, and
`app/adapters/health.py` treats an adapter without a service credential as
unconfigured -- which is exactly the valid self-hosted case here. The execution
layer (budget, telemetry, error classification, audit shape) is still reused;
only the registry entry is separate.
"""

from __future__ import annotations

from src.infrastructure.transcription.transport import TranscriptionProvider
from src.setting.providers import (
    PROVIDER_SPECS as PROVIDER_SPECS,
    TRANSCRIPTION_FAILURE_REASONS as TRANSCRIPTION_FAILURE_REASONS,
    TranscriptionProviderSpec as TranscriptionProviderSpec,
    require_provider_spec as require_provider_spec,
)
from src.setting.transcription import TranscriptionConfig


def build_transcription_provider(config: TranscriptionConfig) -> TranscriptionProvider:
    """Выбрать технический адаптер провайдера по проверенной конфигурации."""
    from src.infrastructure.transcription.providers import (
        OpenAICompatibleAudioProvider,
        OpenRouterAudioProvider,
    )

    if config.kind == "openrouter":
        return OpenRouterAudioProvider(config)
    return OpenAICompatibleAudioProvider(config)
