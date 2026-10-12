"""Транзакционный SQL-адаптер связывания операторских классификаций."""

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.application.dto.calls import ClassifierSource
from src.application.ports.classifier_backfill import ClassifierBackfillTransaction
from src.infrastructure.persistence.classifier import ClassifierPaths

_UNLINKED_SQL = text(
    "SELECT session_id FROM speech_calls "
    "WHERE session_id = ANY(CAST(:session_ids AS text[])) AND operator_node_id IS NULL"
)

_LINK_SQL = text(
    """
    UPDATE speech_calls
       SET operator_node_id = CAST(:node_id AS uuid), disposition = :disposition,
           updated_at = now()
     WHERE session_id = :session_id AND operator_node_id IS NULL
    """
)


class PostgresClassifierBackfillTransaction:
    """Связывать звонки и учитывать новые узлы в одной транзакции."""

    def __init__(self, session: AsyncSession, *, apply: bool) -> None:
        """Принять сессию и режим записи либо проверки."""
        self._session = session
        self._paths = ClassifierPaths(session, source=ClassifierSource.NCC, dry_run=not apply)

    @property
    def created(self) -> int:
        """Вернуть число новых узлов, вычисленное адаптером дерева."""
        return self._paths.created

    async def node(self, path: Sequence[str]) -> UUID | None:
        """Найти или создать путь, вернуть UUID конечного узла."""
        return await self._paths.node(path)

    async def unlinked(self, session_ids: Sequence[str]) -> frozenset[str]:
        """Вернуть идентификаторы принятых звонков без операторского узла."""
        found = await self._session.execute(_UNLINKED_SQL, {"session_ids": list(session_ids)})
        return frozenset(str(value) for value in found.scalars())

    async def link(self, session_id: str, node_id: UUID | None, disposition: str | None) -> None:
        """Связать ещё не связанный звонок с узлом и подписью пути."""
        await self._session.execute(
            _LINK_SQL, {"session_id": session_id, "node_id": node_id, "disposition": disposition}
        )


class PostgresClassifierBackfillRepository:
    """Создавать транзакции пополнения классификатора."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        """Принять фабрику сессий без открытия соединений."""
        self._sessions = sessions

    @asynccontextmanager
    async def transaction(self, *, apply: bool) -> AsyncIterator[ClassifierBackfillTransaction]:
        """Открыть атомарную транзакцию и закрыть её после сценария."""
        async with self._sessions() as session, session.begin():
            yield PostgresClassifierBackfillTransaction(session, apply=apply)
