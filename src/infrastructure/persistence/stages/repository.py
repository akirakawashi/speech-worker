"""Репозиторий снимков и адаптер записи в общей транзакции конвейера."""

from collections.abc import Callable
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from src.application.dto.pipeline import AnalyzeWork, IngestWork, TranscribeWork
from src.application.dto.pipeline_input import AnalysisInput, CallForIngest, CallForTranscription
from src.application.dto.quality_execution import QualityWork
from src.application.ports.queue import ClaimedJob
from src.infrastructure.persistence.stages import analyze, evaluate, ingest, transcribe

SessionFactory = Callable[[], AsyncSession]


class PostgresStageRepository:
    """Открывать короткую сессию только для загрузки DTO этапа."""

    def __init__(self, sessionmaker: SessionFactory | None) -> None:
        """Принять фабрику сессий; None допустим до первого чтения данных."""
        self._sessions = sessionmaker

    def _session(self) -> AsyncSession:
        """Создать сессию или сообщить об отсутствующей зависимости."""
        if self._sessions is None:
            raise RuntimeError("pipeline session factory is not configured")
        return self._sessions()

    async def load_ingest(self, call_id: UUID) -> CallForIngest | None:
        """Загрузить метаданные звонка для приёма записи и закрыть сессию."""
        async with self._session() as session:
            return await ingest.load(session, call_id)

    async def load_transcription(self, call_id: UUID) -> CallForTranscription | None:
        """Загрузить метаданные сохранённой записи и закрыть сессию."""
        async with self._session() as session:
            return await transcribe.load(session, call_id)

    async def load_analysis(self, call_id: UUID) -> AnalysisInput | None:
        """Загрузить расшифровку со справочниками и закрыть сессию."""
        async with self._session() as session:
            return await analyze.load(session, call_id)


class PostgresStagePersistence:
    """Сохранить результат этапа в сессии, принадлежащей вызывающему."""

    def __init__(self, session: AsyncSession) -> None:
        """Принять сессию общей транзакции; её завершает вызывающий."""
        self._session = session

    async def save_ingest(self, job: ClaimedJob, work: IngestWork) -> None:
        """Записать сведения о получении записи без коммита."""
        await ingest.IngestStatements().store(self._session, job, work)

    async def save_transcription(self, job: ClaimedJob, work: TranscribeWork) -> None:
        """Целиком заменить расшифровку и её реплики без коммита."""
        await transcribe.TranscribeStatements().store(self._session, job, work)

    async def save_analysis(self, job: ClaimedJob, work: AnalyzeWork) -> None:
        """Записать разбор и снять оценку при изменении ролей без коммита."""
        await analyze.AnalyzeStatements().store(self._session, job, work)

    async def save_evaluation(self, job: ClaimedJob, work: QualityWork) -> None:
        """Записать итог оценки в текущей транзакции очереди без коммита."""
        await evaluate.store(self._session, job, work)
