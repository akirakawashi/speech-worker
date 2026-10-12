"""SQL-адаптеры справочников; commit остаётся у владельца транзакции."""

from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.dto.catalogues import (
    ClassifierNode,
    ClassifierNodeWrite,
    ReasonRecord,
    ReasonWrite,
)

_SELECT_SQL = text(
    "SELECT id, parent_id, label, key, source, sort_order, is_active FROM speech_classifier_nodes"
)

_INSERT_SQL = text(
    """
    INSERT INTO speech_classifier_nodes (
        id, parent_id, level, label, key, source, sort_order, is_active,
        first_seen_at, created_at, updated_at
    )
    VALUES (
        gen_random_uuid(), CAST(:parent_id AS uuid), :level, :label, :key, :source,
        :sort_order, TRUE, now(), now(), now()
    )
    RETURNING id
    """
)

_UPDATE_SQL = text(
    """
    UPDATE speech_classifier_nodes
       SET label = :label, source = :source, sort_order = :sort_order, is_active = TRUE,
           updated_at = now()
     WHERE id = :id
    """
)

_REASONS_SELECT_SQL = text(
    "SELECT code, label, hint, sort_order, is_active FROM speech_call_reasons"
)

_UPSERT_SQL = text(
    """
    INSERT INTO speech_call_reasons (
        code, label, hint, sort_order, is_active, created_at, updated_at
    )
    VALUES (:code, :label, :hint, :sort_order, TRUE, now(), now())
    ON CONFLICT (code) DO UPDATE
       SET label = EXCLUDED.label, hint = EXCLUDED.hint, sort_order = EXCLUDED.sort_order,
           is_active = TRUE, updated_at = now()
    """
)

_DISABLE_SQL = text(
    "UPDATE speech_call_reasons SET is_active = FALSE, updated_at = now() WHERE code = :code"
)


class PostgresClassifierCatalogue:
    """Хранить дерево в переданной транзакции PostgreSQL."""

    def __init__(self, session: AsyncSession) -> None:
        """Сохранить сессию; самостоятельно транзакцию не завершает."""
        self._session = session

    async def nodes(self) -> tuple[ClassifierNode, ...]:
        """Прочитать узлы и вернуть DTO без объектов SQLAlchemy."""
        rows = (await self._session.execute(_SELECT_SQL)).mappings()
        return tuple(ClassifierNode.model_validate(dict(row)) for row in rows)

    async def create(self, node: ClassifierNodeWrite) -> UUID:
        """Записать DTO нового узла и вернуть UUID, назначенный базой."""
        value = await self._session.scalar(_INSERT_SQL, node.model_dump())
        return UUID(str(value))

    async def update(self, node_id: UUID, node: ClassifierNodeWrite) -> None:
        """Обновить подпись и порядок узла из DTO и включить его."""
        await self._session.execute(_UPDATE_SQL, {**node.model_dump(), "id": node_id})


class PostgresReasonsCatalogue:
    """Хранить коды причин в переданной транзакции PostgreSQL."""

    def __init__(self, session: AsyncSession) -> None:
        """Сохранить сессию без открытия или завершения транзакции."""
        self._session = session

    async def reasons(self) -> tuple[ReasonRecord, ...]:
        """Прочитать все коды в DTO, включая выключенные."""
        rows = (await self._session.execute(_REASONS_SELECT_SQL)).mappings()
        return tuple(ReasonRecord.model_validate(dict(row)) for row in rows)

    async def upsert(self, reason: ReasonWrite) -> None:
        """Записать и включить причину из DTO."""
        await self._session.execute(_UPSERT_SQL, reason.model_dump())

    async def disable(self, code: str) -> None:
        """Выключить код без удаления его истории."""
        await self._session.execute(_DISABLE_SQL, {"code": code})
