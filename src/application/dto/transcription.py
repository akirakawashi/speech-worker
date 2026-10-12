"""DTO записи, сегментов, результата и нормализованной ошибки распознавания."""

from __future__ import annotations

from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field

TRANSCRIPTION_CONTRACT_VERSION: Final = "1.0"
TRANSCRIPTION_ADAPTER_NAME: Final = "speech_transcription"

TranscriptionProviderKind = Literal["openai_compatible"]

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
    ) -> None:
        """Настроить TranscriptionError из переданных параметров и зависимостей."""
        self.reason = reason
        self.status_code = status_code
        self.detail_code = detail_code
        super().__init__(f"transcription_failed:{reason}")
