"""Порт хранения оценки и защиты записи от изменения исходной расшифровки."""

from collections.abc import Sequence
from typing import Protocol
from uuid import UUID

from src.application.dto.quality import CURRENT_SCRIPT, CallScript, Evaluation, TranscriptLine
from src.application.dto.quality_batch import CallRef, CandidateSelection, LineSnapshot


class EvaluationRepository(Protocol):
    """Короткие операции чтения и атомарной записи одной оценки."""

    async def find_candidates(self, selection: CandidateSelection) -> list[CallRef]:
        """Вернуть звонки, подходящие под переданный охват."""
        ...

    async def find_call(self, ref: str, script: CallScript = CURRENT_SCRIPT) -> CallRef:
        """Найти один доступный к оценке звонок по сессии или UUID."""
        ...

    async def load_lines(self, call_id: UUID) -> LineSnapshot:
        """Прочитать реплики с моментом чтения без долгой транзакции."""
        ...

    async def save_roles(
        self, call_id: UUID, snapshot: LineSnapshot, prepared: Sequence[TranscriptLine]
    ) -> None:
        """Проверить исходный снимок, записать роли и снять прежнюю оценку атомарно."""
        ...

    async def save_evaluation(
        self, call_id: UUID, evaluation: Evaluation, snapshot: LineSnapshot
    ) -> None:
        """Проверить актуальность входа и записать результат одной транзакцией."""
        ...

    async def save_failure(
        self,
        call_id: UUID,
        snapshot: LineSnapshot,
        model_name: str,
        script: CallScript,
        reason: str,
    ) -> None:
        """Записать отказ по прежнему входу, сохранив действующую успешную оценку."""
        ...

    async def drop_stale(self, *, apply: bool) -> int:
        """Сосчитать расходящиеся оценки; с apply удалить их в одной транзакции."""
        ...
