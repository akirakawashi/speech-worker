"""Порты чтения аудио и записи технического результата получения."""

from typing import Protocol, runtime_checkable
from uuid import UUID

from src.application.dto.pipeline import IngestWork
from src.application.dto.pipeline_input import CallForTranscription
from src.application.ports.queue import ClaimedJob


@runtime_checkable
class StageRepository(Protocol):
    """Прочитать метаданные записи без открытой транзакции во время HTTP."""

    async def load_transcription(self, call_id: UUID) -> CallForTranscription | None:
        """Вернуть сведения о сохранённом аудио для распознавания либо None."""
        ...


class StagePersistence(Protocol):
    """Сохранить получение аудио в транзакции очереди; ASR принимает бэкенд."""

    async def save_ingest(self, job: ClaimedJob, work: IngestWork) -> None:
        """Записать сведения о получении файла без самостоятельного commit."""
        ...
