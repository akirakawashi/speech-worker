"""Хранилища справочников, используемые сценариями загрузки."""

from typing import Protocol
from uuid import UUID

from src.application.dto.catalogues import (
    ClassifierNode,
    ClassifierNodeWrite,
    ReasonRecord,
    ReasonWrite,
)


class ClassifierCatalogue(Protocol):
    """Изменять дерево в транзакции, которой управляет вызывающий адаптер."""

    async def nodes(self) -> tuple[ClassifierNode, ...]:
        """Вернуть снимок дерева для планирования изменений."""
        ...

    async def create(self, node: ClassifierNodeWrite) -> UUID:
        """Создать узел из DTO и вернуть его идентификатор."""
        ...

    async def update(self, node_id: UUID, node: ClassifierNodeWrite) -> None:
        """Обновить и включить узел с указанным идентификатором."""
        ...


class ReasonsCatalogue(Protocol):
    """Записи причин обращения с неизменными кодами."""

    async def reasons(self) -> tuple[ReasonRecord, ...]:
        """Вернуть снимок справочника вместе с выключенными кодами."""
        ...

    async def upsert(self, reason: ReasonWrite) -> None:
        """Создать или обновить и включить запись из DTO."""
        ...

    async def disable(self, code: str) -> None:
        """Выключить отсутствующий в файле код, сохранив историю звонков."""
        ...
