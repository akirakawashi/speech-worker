"""Независимый этап оценки в общей последовательной очереди."""

from typing import ClassVar

from src.application.dto.calls import JobErrorClass, JobKind
from src.application.dto.pipeline import StageResult
from src.application.dto.quality_execution import QualityWork
from src.application.pipeline.stages import StageDeps, StageError, StageHandler
from src.application.ports.pipeline import StagePersistence
from src.application.ports.queue import ClaimedJob


class EvaluateHandler(StageHandler[QualityWork]):
    """Выполнить выданные бэкендом запросы и атомарно сохранить итог оценки."""

    kind: ClassVar[JobKind] = JobKind.EVALUATE

    async def work(self, job: ClaimedJob, deps: StageDeps) -> QualityWork:
        """Получить итог по UUID без открытой транзакции и локальных промптов."""
        if deps.evaluation is None:
            raise StageError(JobErrorClass.PROVIDER_ERROR, "quality executor is not configured")
        return await deps.evaluation.evaluate(
            job.call_id, attempts=job.attempts, max_attempts=job.max_attempts
        )

    async def store(
        self, persistence: StagePersistence, job: ClaimedJob, work: QualityWork
    ) -> StageResult:
        """Сохранить оценку в транзакции очереди, сохранив статус расшифрованного звонка."""
        await persistence.save_evaluation(job, work)
        return StageResult(processing_status=None)
