"""Обработчик получения: выполняет сценарий и применяет готовое решение бэкенда."""

from src.application.dto.calls import JobKind
from src.application.dto.ingest_control import IngestDecision
from src.application.ingestion.recording import RecordingIngestionService
from src.application.pipeline.stages import StageDeps, StageHandler, StageResult
from src.application.ports.pipeline import StagePersistence
from src.application.ports.queue import ClaimedJob


class IngestHandler(StageHandler[IngestDecision]):
    """Получить файл через application и атомарно применить готовые данные/переход."""

    kind = JobKind.INGEST

    def __init__(self, application: RecordingIngestionService) -> None:
        """Внедрить технический сценарий, собранный в DI."""
        self._application = application

    async def work(self, job: ClaimedJob, deps: StageDeps) -> IngestDecision:
        """Передать живое задание сценарию скачивания; вернуть решение бэкенда."""
        return await self._application.execute(job, deps)

    async def store(
        self, persistence: StagePersistence, job: ClaimedJob, work: IngestDecision
    ) -> StageResult:
        """Сохранить выданные бэкендом факты в транзакции очереди и вернуть его переход."""
        await persistence.save_ingest(job, work.work)
        return work.stage
