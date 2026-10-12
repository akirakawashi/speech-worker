"""Ручная команда оценки ставит задания в общую очередь."""

from collections.abc import Callable

from src.application.dto.maintenance import QualityQueueReport, QualityQueueRequest
from src.application.dto.quality_batch import RunEvaluation
from src.application.ports.maintenance import BackendMaintenance


class QueuedEvaluationService:
    """Передать охват бэкенду; модель выполнит обычный обработчик очереди."""

    def __init__(self, backend: BackendMaintenance) -> None:
        """Принять порт постановки заданий без клиента LLM или соединения с базой."""
        self._backend = backend

    async def run(
        self, query: RunEvaluation, *, on_line: Callable[[str], None] = lambda _line: None
    ) -> QualityQueueReport:
        """Проверить охват либо поставить задания и вернуть счёт; не ждать готовых оценок."""
        return await self._backend.enqueue(QualityQueueRequest(selection=query))
