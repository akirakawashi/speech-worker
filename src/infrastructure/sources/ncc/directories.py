"""Преобразование моделей и ошибок REST NCC в контракт справочников приложения."""

from src.application.dto.directories import DirectoryEmployee, DirectoryProject
from src.application.errors.directories import DirectorySourceError
from src.infrastructure.sources.ncc.rest import NccError, NccRestClient


class NccDirectorySource:
    """Читать справочники через клиент NCC, возвращая только DTO приложения."""

    def __init__(self, client: NccRestClient) -> None:
        """Принять настроенный транспорт без создания дополнительного клиента."""
        self._client = client

    async def employees(self, *, removed: bool) -> tuple[DirectoryEmployee, ...]:
        """Прочитать выбранный список, преобразовать поля и нормализовать отказ источника."""
        try:
            rows = await self._client.employees(removed=removed)
        except NccError as exc:
            raise DirectorySourceError(exc.reason) from exc
        return tuple(
            DirectoryEmployee(
                login=row.login,
                uuid=row.uuid,
                display_name=row.display_name,
                department=row.department,
                removed=row.removed,
            )
            for row in rows
        )

    async def project(self, uuid: str) -> DirectoryProject:
        """Прочитать проект по UUID; вернуть DTO метаданных либо DirectorySourceError."""
        try:
            row = await self._client.project(uuid)
        except NccError as exc:
            raise DirectorySourceError(exc.reason) from exc
        return DirectoryProject(
            uuid=row.uuid, title=row.title, data_channel=row.data_channel, direction=row.direction
        )
