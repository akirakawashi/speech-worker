"""Порты чтения классификаций и изменения дерева."""

from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from datetime import date
from typing import Protocol
from uuid import UUID

from src.application.dto.sources import CallRef


class ClassificationSource(Protocol):
    """Классификации и локаторы звонков источника за день."""

    async def list_classifications(self, day: date) -> Mapping[tuple[str, ...], int]:
        """Вернуть пути и число разговоров на каждом пути за указанный день."""
        ...

    async def list_calls(self, day: date) -> Sequence[CallRef]:
        """Вернуть метаданные звонков за день без загрузки записей."""
        ...


class ClassifierBackfillTransaction(Protocol):
    """Операции одной транзакции пополнения дерева."""

    @property
    def created(self) -> int:
        """Число созданных или запланированных новых узлов."""
        ...

    async def node(self, path: Sequence[str]) -> UUID | None:
        """Найти или создать путь и вернуть конечный узел."""
        ...

    async def unlinked(self, session_ids: Sequence[str]) -> frozenset[str]:
        """Найти принятые звонки без операторского узла."""
        ...

    async def link(self, session_id: str, node_id: UUID | None, disposition: str | None) -> None:
        """Связать только ещё не связанный звонок с узлом и подписью пути."""
        ...


class ClassifierBackfillRepository(Protocol):
    """Создать атомарную операцию дерева либо её режим проверки."""

    def transaction(
        self, *, apply: bool
    ) -> AbstractAsyncContextManager[ClassifierBackfillTransaction]:
        """Открыть транзакцию; apply=False не должен изменять хранилище."""
        ...
