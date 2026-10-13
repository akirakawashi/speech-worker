"""Распознавание и восстанавливаемая доставка без бизнес-записи результата в воркере."""

from pydantic import ValidationError

from src.application.dto.pipeline_input import CallForTranscription
from src.application.dto.transcription_delivery import (
    ReceivedTranscription,
    TranscriptionClaim,
    TranscriptionDeliveryRequest,
    TranscriptionReceipt,
    TranscriptionSource,
)
from src.application.pipeline.stages import StageDeferred, StageDeps, StageError
from src.application.pipeline.transcribe import TranscribeHandler
from src.application.ports.queue import ClaimedJob
from src.application.ports.transcription_delivery import (
    TRANSCRIPTION_DELIVERY_KEY,
    TranscriptionDelivery,
    TranscriptionDeliveryBuffer,
    TranscriptionDeliveryError,
)
from src.domain.calls.contracts import AudioStatus, JobErrorClass


class TranscriptionDeliveryService:
    """Подготовить один результат, сохранить буфер и получить квитанцию бэкенда."""

    def __init__(
        self, buffer: TranscriptionDeliveryBuffer, delivery: TranscriptionDelivery
    ) -> None:
        """Внедрить долговременный буфер и HTTP-порт; клиенты моделей здесь не создаются."""
        self._buffer = buffer
        self._delivery = delivery
        self._recognition = TranscribeHandler()

    async def execute(self, job: ClaimedJob, deps: StageDeps) -> TranscriptionReceipt:
        """Доставить распознавание действующего задания без локального complete или анализа.

        Принимает захват и порты этапа. Подготовленный DTO сохраняется до HTTP;
        повторный захват переиспользует его результат только для того же аудио.
        Сетевой отказ даёт StageDeferred без расхода попытки Whisper, прочие
        известные ошибки — StageError. Отмена сохраняет уже записанный буфер.
        """
        if job.claim_token is None or not job.locked_by:
            raise StageError(JobErrorClass.INVALID_OUTPUT, "transcription claim has no identity")
        call = await self._load_call(job, deps)
        source = TranscriptionSource(
            object_key=str(call.audio_object_key), channels=call.audio_channels
        )
        pending = self._pending(job)
        if pending is not None and pending.source == source:
            result = pending.result
        else:
            work = await self._recognition.work_for_call(job, deps, call)
            try:
                result = ReceivedTranscription.model_validate(work.model_dump(mode="python"))
            except ValidationError:
                raise StageError(
                    JobErrorClass.INVALID_OUTPUT, "invalid transcription result"
                ) from None
        command = TranscriptionDeliveryRequest(
            claim=TranscriptionClaim(
                call_id=job.call_id,
                job_id=job.id,
                claim_token=job.claim_token,
                worker_id=job.locked_by,
                attempt=job.attempts,
            ),
            source=source,
            result=result,
        )
        if not await self._buffer.save(command):
            raise StageDeferred(JobErrorClass.PROVIDER_ERROR, "transcription claim changed")
        try:
            return await self._delivery.deliver(command)
        except TranscriptionDeliveryError as exc:
            if exc.retryable:
                raise StageDeferred(exc.error_class, str(exc)) from None
            raise StageError(exc.error_class, str(exc)) from None

    @staticmethod
    async def _load_call(job: ClaimedJob, deps: StageDeps) -> CallForTranscription:
        """Прочитать вход через порт; отсутствующее сохранённое аудио даёт NOT_FOUND."""
        if deps.repository is None:
            raise RuntimeError("pipeline repository is not configured")
        call = await deps.repository.load_transcription(job.call_id)
        if (
            call is None
            or call.audio_status != AudioStatus.STORED.value
            or not call.audio_object_key
        ):
            raise StageError(JobErrorClass.NOT_FOUND, "call has no stored audio")
        return call

    @staticmethod
    def _pending(job: ClaimedJob) -> TranscriptionDeliveryRequest | None:
        """Прочитать сохранённый запрос; повреждённый/чужой буфер не отправляется."""
        value = job.payload.get(TRANSCRIPTION_DELIVERY_KEY)
        if value is None:
            return None
        try:
            pending = TranscriptionDeliveryRequest.model_validate(value)
        except ValidationError:
            raise StageError(JobErrorClass.INVALID_OUTPUT, "invalid transcription buffer") from None
        if pending.claim.call_id != job.call_id or pending.claim.job_id != job.id:
            raise StageError(
                JobErrorClass.INVALID_OUTPUT, "transcription buffer belongs to another job"
            )
        return pending
