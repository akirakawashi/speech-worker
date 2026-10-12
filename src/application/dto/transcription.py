"""Domain contract for speech transcription.

Nothing in this module knows which provider is configured. Callers depend on
`TranscriptionProvider`, so swapping OpenRouter for a self-hosted OpenAI
compatible endpoint is a configuration change, not a code change.
"""

from __future__ import annotations

from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from src.application.dto.adapter import AdapterCallStatus, AdapterExecutionTelemetry

TRANSCRIPTION_CONTRACT_VERSION: Final = "1.0"
TRANSCRIPTION_ADAPTER_NAME: Final = "speech_transcription"

TranscriptionProviderKind = Literal["openrouter", "openai_compatible"]

# Deliberately identical to AdapterFailureReason: the normalized audit `reason`
# must not introduce a vocabulary of its own.
TranscriptionFailureReason = Literal[
    "not_configured",
    "unauthorized",
    "forbidden",
    "timeout",
    "provider_error",
    "unexpected_error",
]

TranscriptionDetailCode = Literal["malformed_response", "empty_transcript"]

# `unavailable` and `suspected` both mean the transcript may be incomplete; they
# differ in whether the provider told us the audio duration at all.
TruncationCheck = Literal["not_applicable", "unavailable", "clear", "suspected"]


class TranscriptionRequest(BaseModel):
    """An already validated audio payload ready to be sent to a provider."""

    model_config = ConfigDict(frozen=True)

    filename: str
    content: bytes = Field(repr=False)
    mime_type: str
    audio_format: str
    size_bytes: int
    language: str | None = None


class TranscriptionSegment(BaseModel):
    """Одна реплика с таймкодами — то, что даёт `response_format=verbose_json`.

    Три необязательных признака качества приходят от `faster-whisper` и здесь
    сохраняются, а не отбрасываются: по ним реплику можно пометить сомнительной,
    не переслушивая запись. Восстановить их потом нельзя — второй прогон стоит
    столько же, сколько первый.
    """

    model_config = ConfigDict(frozen=True)

    start_s: float
    end_s: float
    text: str = Field(repr=False)
    #: Средняя логарифмическая вероятность токенов. Чем ближе к нулю, тем
    #: увереннее модель.
    avg_logprob: float | None = None
    #: Вероятность того, что в окне вообще нет речи.
    no_speech_prob: float | None = None
    #: Отношение сжатия текста. Высокое означает зацикливание модели на
    #: повторяющейся фразе — характерный дефект распознавания тишины.
    compression_ratio: float | None = None


class TranscriptionResult(BaseModel):
    """Текст, сегменты, язык и метаданные результата распознавания."""

    model_config = ConfigDict(frozen=True)

    text: str = Field(repr=False)
    provider_kind: TranscriptionProviderKind
    model: str
    language: str | None = None
    confidence: float | None = None
    duration_seconds: float | None = None
    truncation_check: TruncationCheck = "not_applicable"
    #: Пусто, если движок отдал только текст (`response_format=json`) либо
    #: провайдер сегментов не поддерживает. Пустота здесь означает «не
    #: запрашивали или не отдали», а не «реплик нет».
    segments: tuple[TranscriptionSegment, ...] = ()


class TranscriptionError(Exception):
    """Normalized transcription failure.

    `str(exc)` carries the reason and nothing else: no status text, no provider
    body, no URL, no credential. The extra attributes exist for audit and for
    mapping to an HTTP status, never for user-facing messages.
    """

    def __init__(
        self,
        reason: TranscriptionFailureReason,
        *,
        status_code: int | None = None,
        detail_code: TranscriptionDetailCode | None = None,
        outcome: TranscriptionOutcome | None = None,
    ) -> None:
        """Настроить TranscriptionError из переданных параметров и зависимостей."""
        self.reason = reason
        self.status_code = status_code
        self.detail_code = detail_code
        self.outcome = outcome
        super().__init__(f"transcription_failed:{reason}")


class TranscriptionMetadata(BaseModel):
    """Публичные метаданные настроенного движка без адреса и ключа."""

    model_config = ConfigDict(frozen=True)
    kind: TranscriptionProviderKind
    model: str
    source: str
    base_url_setting: str
    audio_window_seconds: float | None
    network_zone: str
    auth_mode: str
    owner: str
    version: str


_UNCONFIGURED_TARGET_KEY = "transcription_base_url"

_UNCONFIGURED_NETWORK_ZONE = "external"

_UNCONFIGURED_AUTH_MODE = "optional_service_credential"

_UNCONFIGURED_OWNER = "aicistent"

_UNCONFIGURED_VERSION = "1.0.0"


class TranscriptionOutcome(BaseModel):
    """Итог вызова распознавания для безопасного журнала и метрик."""

    model_config = ConfigDict(frozen=True)

    status: AdapterCallStatus
    reason: TranscriptionFailureReason | None
    elapsed_ms: int
    request_id: str | None
    telemetry: AdapterExecutionTelemetry
    config: TranscriptionMetadata | None
    request: TranscriptionRequest | None
    result: TranscriptionResult | None
    status_code: int | None = None
    detail_code: TranscriptionDetailCode | None = None

    def audit_payload(self, *, extra: dict[str, object] | None = None) -> dict[str, object]:
        """Вернуть метаданные распознавания для журнала без аудио и секретов."""
        if self.config is not None:
            configuration_key = self.config.base_url_setting
            network_zone: str = self.config.network_zone
            auth_mode: str = self.config.auth_mode
            owner = self.config.owner
            version = self.config.version
        else:
            configuration_key = _UNCONFIGURED_TARGET_KEY
            network_zone = _UNCONFIGURED_NETWORK_ZONE
            auth_mode = _UNCONFIGURED_AUTH_MODE
            owner = _UNCONFIGURED_OWNER
            version = _UNCONFIGURED_VERSION

        payload: dict[str, object] = {
            "adapter": TRANSCRIPTION_ADAPTER_NAME,
            "status": self.status,
            "elapsed_ms": self.elapsed_ms,
            "request_id": self.request_id,
            "request_attempts": self.telemetry.request_attempts,
            "retry_count": self.telemetry.retry_count,
            "max_attempts_per_request": self.telemetry.max_attempts,
            "total_timeout_ms": self.telemetry.total_timeout_ms,
            "adapter_owner": owner,
            "adapter_version": version,
            "future_service": "adapter-transcription",
            "operation_type": "read",
            "failure_mode": "fail_closed",
            "target": {
                "origin": "[redacted]",
                "configured": self.config is not None,
                "configuration_key": configuration_key,
                "network_zone": network_zone,
                "auth_mode": auth_mode,
            },
            "transcription_contract_version": TRANSCRIPTION_CONTRACT_VERSION,
        }

        if self.reason is not None:
            payload["reason"] = self.reason
        if self.config is not None:
            payload["provider_kind"] = self.config.kind
            payload["config_source"] = self.config.source
            payload["transcription_model"] = self.config.model
            payload["audio_window_seconds"] = self.config.audio_window_seconds
        if self.request is not None:
            payload["audio_format"] = self.request.audio_format
            payload["language_requested"] = self.request.language
        if self.result is not None:
            payload["language_detected"] = self.result.language
            payload["audio_duration_seconds"] = self.result.duration_seconds
            payload["truncation_check"] = self.result.truncation_check
            payload["transcript_chars"] = len(self.result.text)
        if self.status_code is not None:
            payload["provider_status_code"] = self.status_code
        if self.detail_code is not None:
            payload["provider_detail_code"] = self.detail_code

        if extra:
            collisions = payload.keys() & extra.keys()
            if collisions:
                names = ", ".join(sorted(collisions))
                raise ValueError(
                    f"Transcription audit extras cannot override normalized fields: {names}"
                )
            payload.update(extra)
        return payload
