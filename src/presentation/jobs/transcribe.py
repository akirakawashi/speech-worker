"""Обработчик распознавания, завершение которого подтверждает бэкенд."""

from src.application.dto.calls import JobKind
from src.application.dto.transcription_delivery import TranscriptionReceipt
from src.application.pipeline.stages import StageDeps, StageHandler, StageResult
from src.application.ports.pipeline import StagePersistence
from src.application.ports.queue import ClaimedJob
from src.application.transcription_delivery import TranscriptionDeliveryService


class TranscriptionHandler(StageHandler[TranscriptionReceipt]):
    """Доставить ASR через application; локальная запись и complete запрещены."""

    kind = JobKind.TRANSCRIBE
    completes_externally = True

    def __init__(self, application: TranscriptionDeliveryService) -> None:
        """Внедрить собранный DI сценарий распознавания и доставки."""
        self._application = application

    def slot_limit(self, deps: StageDeps) -> int | None:
        """Сохранить общий предел Whisper из проверенного снимка настроек."""
        return deps.runtime_settings.transcribe_concurrency

    async def work(self, job: ClaimedJob, deps: StageDeps) -> TranscriptionReceipt:
        """Передать захват и технические порты сценарию; вернуть подтверждение бэкенда."""
        return await self._application.execute(job, deps)

    async def store(
        self, persistence: StagePersistence, job: ClaimedJob, work: TranscriptionReceipt
    ) -> StageResult:
        """Отказать при попытке локальной записи: этот этап завершает только бэкенд."""
        raise RuntimeError("transcription completion belongs to the backend")
