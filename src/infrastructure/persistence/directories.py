"""SQL-адаптер синхронизации в транзакции вызывающего процесса или HTTP-запроса."""

from typing import Any, cast

from sqlalchemy import CursorResult, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.dto.directories import DirectoryProject, OperatorWrite
from src.application.ports.directories import WriteResult

_UPSERT_OPERATOR = text(
    """
    INSERT INTO speech_operators (
        id, ncc_uuid, login, full_name, department, removed, synced_at, created_at, updated_at
    )
    VALUES (gen_random_uuid(), :ncc_uuid, :login, :full_name, :department, :removed,
            now(), now(), now())
    ON CONFLICT (login) DO UPDATE
       SET ncc_uuid = EXCLUDED.ncc_uuid,
           full_name = EXCLUDED.full_name,
           department = EXCLUDED.department,
           removed = EXCLUDED.removed,
           synced_at = now(),
           updated_at = now()
     -- Строка переписывается, только если содержимое отличается. Иначе
     -- `RETURNING` не вернёт ничего — это и есть «без изменений», и отличить
     -- его от «обновлено» иначе нечем: `DO UPDATE` пишет всегда.
     WHERE (speech_operators.ncc_uuid, speech_operators.full_name,
            speech_operators.department, speech_operators.removed)
           IS DISTINCT FROM
           (EXCLUDED.ncc_uuid, EXCLUDED.full_name,
            EXCLUDED.department, EXCLUDED.removed)
     RETURNING (xmax = 0) AS created
    """
)

_MARK_MISSING_AS_REMOVED = text(
    """
    UPDATE speech_operators
       SET removed = TRUE, synced_at = now(), updated_at = now()
     WHERE removed = FALSE
       AND login <> ALL(CAST(:logins AS text[]))
       AND position('*' IN login) = 0
    """
)

_KNOWN_PROJECTS = text(
    "SELECT ncc_uuid FROM speech_ncc_projects WHERE ncc_uuid IS NOT NULL ORDER BY ncc_uuid"
)

_UPDATE_PROJECT = text(
    """
    UPDATE speech_ncc_projects
       SET title = :title,
           data_channel = :data_channel,
           direction = :direction,
           synced_at = now(),
           updated_at = now()
     WHERE ncc_uuid = :ncc_uuid
       AND (title, data_channel, direction) IS DISTINCT FROM
           (CAST(:title AS varchar), CAST(:data_channel AS varchar), CAST(:direction AS varchar))
    """
)

_TOUCH_PROJECT = text("UPDATE speech_ncc_projects SET synced_at = now() WHERE ncc_uuid = :ncc_uuid")


class PostgresDirectoryRepository:
    """Сохранить только поля источника; commit и rollback выполняет владелец сессии."""

    def __init__(self, session: AsyncSession) -> None:
        """Принять открытую сессию отдельной базы речи."""
        self._session = session

    async def upsert_operator(self, employee: OperatorWrite) -> WriteResult:
        """Вставить или обновить сотрудника из DTO; вернуть состояние изменения строки."""
        written = (
            await self._session.execute(_UPSERT_OPERATOR, employee.model_dump())
        ).scalar_one_or_none()
        if written is None:
            return "unchanged"
        return "created" if written else "updated"

    async def mark_missing_operators(self, logins: tuple[str, ...]) -> int:
        """По отсутствию логина пометить сотрудника NCC уволенным; вернуть число строк."""
        result = cast(
            CursorResult[Any],
            await self._session.execute(_MARK_MISSING_AS_REMOVED, {"logins": list(logins)}),
        )
        return result.rowcount or 0

    async def known_projects(self) -> tuple[str, ...]:
        """Прочитать существующие UUID NCC, исключая проекты других источников."""
        return tuple(str(row) for row in (await self._session.scalars(_KNOWN_PROJECTS)).all())

    async def update_project(self, project: DirectoryProject) -> bool:
        """Сохранить метаданные и время сверки; вернуть True при изменении содержимого."""
        params = {
            "ncc_uuid": project.uuid,
            "title": project.title,
            "data_channel": project.data_channel,
            "direction": project.direction or "unknown",
        }
        result = cast(CursorResult[Any], await self._session.execute(_UPDATE_PROJECT, params))
        if result.rowcount:
            return True
        await self._session.execute(_TOUCH_PROJECT, {"ncc_uuid": project.uuid})
        return False
