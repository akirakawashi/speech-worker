"""Контракты чтения и записи данных этапов конвейера."""

from typing import Protocol, runtime_checkable
from uuid import UUID

from src.application.dto.pipeline import AnalyzeWork, IngestWork, TranscribeWork
from src.application.dto.pipeline_input import (
    AnalysisInput,
    CallForIngest,
    CallForTranscription,
)
from src.application.ports.queue import ClaimedJob


@runtime_checkable
class StageRepository(Protocol):
    """Загрузить типизированный снимок данных для долгой фазы этапа."""

    async def load_ingest(self, call_id: UUID) -> CallForIngest | None:
        """Вернуть данные записи и проекта; None означает удалённый звонок."""
        ...

    async def load_transcription(self, call_id: UUID) -> CallForTranscription | None:
        """Вернуть сведения о записи для распознавания или None."""
        ...

    async def load_analysis(self, call_id: UUID) -> AnalysisInput | None:
        """Вернуть расшифровку со справочниками для разбора или None."""
        ...


class StagePersistence(Protocol):
    """Записать результат в уже открытой транзакции вызывающего."""

    async def save_ingest(self, job: ClaimedJob, work: IngestWork) -> None:
        """Сохранить метаданные записи и проекта, не завершая транзакцию."""
        ...

    async def save_transcription(self, job: ClaimedJob, work: TranscribeWork) -> None:
        """Целиком заменить расшифровку в текущей транзакции."""
        ...

    async def save_analysis(self, job: ClaimedJob, work: AnalyzeWork) -> None:
        """Сохранить разбор и снять оценку при изменении ролей."""
        ...
