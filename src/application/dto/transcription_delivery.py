"""Независимые DTO служебного контракта доставки распознавания 1.0."""

from typing import Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from src.application.dto.pipeline import Reply, TranscribeWork
from src.domain.transcription.delivery import validate_asr_attribution


class TranscriptionClaim(BaseModel):
    """Идентификаторы звонка, задания и выданного очередью захвата."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    call_id: UUID
    job_id: UUID
    claim_token: UUID
    worker_id: str = Field(min_length=1, max_length=128)
    attempt: int = Field(strict=True, ge=1, le=32767)


class TranscriptionSource(BaseModel):
    """Сохранённое аудио, по которому выполнено распознавание."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    object_key: str = Field(min_length=1, max_length=1024)
    channels: int | None = Field(default=None, strict=True, ge=1)


class ReceivedReply(Reply):
    """Реплика доставки с индексом и интервалом в пределах схемы бэкенда."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    seq: int = Field(strict=True, ge=0, le=2147483647)
    start_ms: int = Field(strict=True, ge=0, le=2147483647)
    end_ms: int = Field(strict=True, ge=0, le=2147483647)

    @model_validator(mode="after")
    def ordered_interval(self) -> Self:
        """Отклонить интервал, конец которого раньше начала; вернуть проверенную реплику."""
        if self.end_ms < self.start_ms:
            raise ValueError("reply interval is reversed")
        return self


class ReceivedTranscription(TranscribeWork):
    """Распознавание без полей анализа, совместимое с контрактом приёма бэкенда."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    engine: str = Field(min_length=1, max_length=64)
    model_name: str = Field(min_length=1, max_length=128)
    model_digest: str | None = Field(default=None, max_length=128)
    language: str = Field(min_length=1, max_length=16)
    duration_s: float | None = Field(ge=0, lt=10000000, allow_inf_nan=False)
    replies: tuple[ReceivedReply, ...] = Field(max_length=50000, repr=False)

    @model_validator(mode="after")
    def valid_asr_result(self) -> Self:
        """Проверить роли, последовательность и объём текста; вернуть допустимый результат."""
        validate_asr_attribution(self.role_method, tuple(reply.speaker for reply in self.replies))
        sequences = [reply.seq for reply in self.replies]
        if sequences != sorted(set(sequences)):
            raise ValueError("reply sequences must be unique and increasing")
        if sum(len(reply.text) for reply in self.replies) > 5000000:
            raise ValueError("transcription text is too large")
        return self


class TranscriptionDeliveryRequest(BaseModel):
    """Неизменяемый запрос; внутри одного захвата повторяется тот же DTO."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    contract_version: Literal["1.0"] = "1.0"
    claim: TranscriptionClaim
    source: TranscriptionSource
    result: ReceivedTranscription = Field(repr=False)


class TranscriptionReceipt(BaseModel):
    """Проверяемая квитанция commit; версия ответа должна присутствовать явно."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    contract_version: Literal["1.0"]
    call_id: UUID
    job_id: UUID
    claim_token: UUID
    attempt: int = Field(strict=True, ge=1, le=32767)
    transcript_id: UUID
    analysis_job_id: UUID
    accepted_at: AwareDatetime
