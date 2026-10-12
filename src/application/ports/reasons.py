"""Операции хранения, необходимые заполнению причин."""

from typing import Protocol
from uuid import UUID

from src.application.dto.pipeline_input import AnalysisSegment
from src.application.dto.reasons import ReasonAnswer, ReasonCandidates, ReasonSelection


class ReasonRepository(Protocol):
    """Короткие чтения и атомарная запись причины одного звонка."""

    async def candidates(self, selection: ReasonSelection) -> ReasonCandidates:
        """Вернуть снимок выбранных звонков и включённых причин."""
        ...

    async def segments(self, call_id: UUID) -> tuple[AnalysisSegment, ...]:
        """Вернуть реплики по порядку для указанного звонка."""
        ...

    async def save(self, call_id: UUID, answer: ReasonAnswer) -> None:
        """Записать ответ, только если причина всё ещё не заполнена."""
        ...
