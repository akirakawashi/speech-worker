"""Чтение выборки и реплик для сравнения без изменения звонков."""

from datetime import datetime
from typing import Protocol
from uuid import UUID

from src.application.dto.calls import CallSource
from src.application.dto.classifier import NodePaths
from src.application.dto.comparison import Sampled
from src.application.dto.pipeline_input import AnalysisSegment
from src.application.dto.reasons import CallReason


class ComparisonRepository(Protocol):
    """Снимки исходных данных для прогонов моделей."""

    async def sample_calls(
        self, *, date_from: datetime, date_to: datetime, limit: int
    ) -> list[Sampled]:
        """Вернуть детерминированную выборку уже разобранных звонков."""
        ...

    async def sample_reason_calls(
        self, *, date_from: datetime, date_to: datetime, source: CallSource, limit: int
    ) -> list[tuple[UUID, str]]:
        """Вернуть детерминированную выборку звонков с сохранённым кодом причины."""
        ...

    async def reasons(self) -> tuple[CallReason, ...]:
        """Прочитать включённые причины, доступные модели."""
        ...

    async def segments(self, call_id: UUID) -> tuple[AnalysisSegment, ...]:
        """Прочитать реплики и закрыть сессию до вызова модели."""
        ...

    async def paths(self) -> NodePaths:
        """Вернуть независимый от SQL снимок дерева для сравнения результатов."""
        ...
