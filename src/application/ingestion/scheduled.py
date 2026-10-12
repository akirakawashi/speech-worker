"""Запуск существующего приёма по параметрам, полученным от бэкенда."""

from collections.abc import Callable
from datetime import date, datetime
from zoneinfo import ZoneInfo

from src.application.dto.calls import CallSource
from src.application.dto.maintenance import CatchUpCommand
from src.application.ingestion.daily import catch_up
from src.application.ports.daily_ingest import BackfillRepository, DailyIngestRepository
from src.application.ports.sources import DailyBatchSource


class ScheduledIntakeService:
    """Собирать свежие метаданные и настроенную историю через заменяемые порты."""

    def __init__(
        self,
        repository: DailyIngestRepository,
        history: BackfillRepository,
        source_factory: Callable[[CallSource], DailyBatchSource],
        *,
        telphin_history_from: date | None,
        history_days: int,
    ) -> None:
        """Принять хранение, фабрику источников и прежние параметры догона истории."""
        self._repository = repository
        self._history = history
        self._sources = source_factory
        self._history_from = telphin_history_from
        self._history_days = history_days

    async def collect(self, command: CatchUpCommand) -> None:
        """Собрать вчера, сегодня и порцию истории без скачивания или обработки аудио."""
        await catch_up(
            source=self._sources(command.source),
            repository=self._repository,
            history=self._history,
            call_source=command.source.value,
            today=datetime.now(ZoneInfo(command.timezone)).date(),
            history_from=self._history_from if command.source is CallSource.TELPHIN else None,
            history_days=self._history_days,
            day_share=command.day_share,
            priority=command.priority,
        )
