"""Внешние операции синхронизации сотрудников и известных проектов."""

from typing import Literal, Protocol

from src.application.dto.directories import DirectoryEmployee, DirectoryProject, OperatorWrite

WriteResult = Literal["created", "updated", "unchanged"]


class DirectorySource(Protocol):
    """Читать списки сотрудников и один проект; отказ — DirectorySourceError."""

    async def employees(self, *, removed: bool) -> tuple[DirectoryEmployee, ...]:
        """Вернуть сотрудников выбранного состояния; оба списка читаются отдельно."""
        ...

    async def project(self, uuid: str) -> DirectoryProject:
        """Вернуть известный проект по UUID либо сообщить нормализованный отказ."""
        ...


class DirectoryRepository(Protocol):
    """Записывать изменения без удаления сотрудников и проектов с историей звонков."""

    async def upsert_operator(self, employee: OperatorWrite) -> WriteResult:
        """Записать согласованные поля по логину; вернуть характер изменения."""
        ...

    async def mark_missing_operators(self, logins: tuple[str, ...]) -> int:
        """Пометить отсутствующих сотрудников NCC уволенными, сохранив операторов Телфина."""
        ...

    async def known_projects(self) -> tuple[str, ...]:
        """Вернуть UUID известных проектов; записи без UUID источника не включать."""
        ...

    async def update_project(self, project: DirectoryProject) -> bool:
        """Обновить поля источника и момент сверки; вернуть True при изменении содержимого."""
        ...
