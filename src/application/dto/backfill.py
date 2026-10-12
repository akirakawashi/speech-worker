"""Данные окна и отчёт порции исторического приёма."""

from pydantic import BaseModel, ConfigDict

from src.application.dto.daily_ingest import DailyRunReport
from src.domain.calls.backfill import (
    BackfillNotStarted as BackfillNotStarted,
    BackfillState as BackfillState,
)


class PortionReport(BaseModel):
    """Что сделала одна порция."""

    model_config = ConfigDict(frozen=True)
    days: tuple[DailyRunReport, ...] = ()
    state: BackfillState | None = None
    stopped_because: str | None = None

    @property
    def enqueued(self) -> int:
        """Вернуть общее число поставленных заданий обработанных дней."""
        return sum(day.enqueued for day in self.days)

    def describe(self) -> str:
        """Вернуть читаемую сводку результата для вывода оператору."""
        if self.stopped_because:
            return self.stopped_because
        processed = ", ".join(str(day.day) for day in self.days)
        head = f"обработано суток {len(self.days)} ({processed}), заданий {self.enqueued}"
        return f"{head}. {self.state.describe()}" if self.state else head
