"""SQL-загрузка дерева классификатора и условия отбора ветки."""

from __future__ import annotations

import uuid
from collections.abc import Sequence

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.dto.calls import ClassifierSource
from src.application.dto.classifier import (
    _INVISIBLE as _INVISIBLE,
    _PLACEHOLDER as _PLACEHOLDER,
    _PRODUCT_BY_KEY as _PRODUCT_BY_KEY,
    _SEGMENT_BY_KEY as _SEGMENT_BY_KEY,
    _SLASH as _SLASH,
    CLASSIFIER_DEPTH as CLASSIFIER_DEPTH,
    EMPTY_LEVEL_LABEL as EMPTY_LEVEL_LABEL,
    LABEL_LIMIT as LABEL_LIMIT,
    NCC_SORT_ORDER as NCC_SORT_ORDER,
    PATH_SEPARATOR as PATH_SEPARATOR,
    TOPIC_LEVEL as TOPIC_LEVEL,
    ClassifierIndex as ClassifierIndex,
    ClassifierNode as ClassifierNode,
    NodePaths as ClassifierPathsBase,
    ResolvedPath as ResolvedPath,
    _is_empty_level as _is_empty_level,
    classifier_levels as classifier_levels,
    clean_label as clean_label,
    dimensions as dimensions,
    label_key as label_key,
    level_match as level_match,
    path_caption as path_caption,
    present_levels as present_levels,
    suggestion_labels as suggestion_labels,
)


def branch_sql(column: str, param: str) -> str:
    """Условие «узел в `column` — узел `:param` или любой под ним»; пустой параметр не сужает.

    Ветка, а не узел: выбрав «Физ.лицо › ШПД», человек спрашивает про все
    звонки по ШПД, а не только про те, где классификация не пошла глубже.
    Имена колонки и параметра — из кода, не из запроса.
    """
    return (
        f"(CAST(:{param} AS uuid) IS NULL OR {column} IN ("
        "WITH RECURSIVE branch AS ("
        f"SELECT id FROM speech_classifier_nodes WHERE id = CAST(:{param} AS uuid) "
        "UNION ALL "
        "SELECT n.id FROM speech_classifier_nodes AS n JOIN branch AS b ON n.parent_id = b.id"
        ") SELECT id FROM branch))"
    )


class NodePaths(ClassifierPathsBase):
    """Дерево, загруженное репозиторием из PostgreSQL."""

    @classmethod
    async def load(cls, session: AsyncSession) -> NodePaths:
        """Получить узлы в переданной сессии и вернуть дерево их путей."""
        rows = (await session.execute(NODES_SQL)).mappings()
        return cls([dict(row) for row in rows])


_FIND_NODE_SQL = text(
    "SELECT id FROM speech_classifier_nodes "
    "WHERE parent_id IS NOT DISTINCT FROM CAST(:parent_id AS uuid) AND key = :key"
)


_INSERT_NODE_SQL = text(
    """
    INSERT INTO speech_classifier_nodes (
        id, parent_id, level, label, key, source, sort_order, is_active,
        first_seen_at, created_at, updated_at
    )
    VALUES (
        gen_random_uuid(), CAST(:parent_id AS uuid), :level, :label, :key, :source,
        :sort_order, TRUE, now(), now(), now()
    )
    ON CONFLICT DO NOTHING
    RETURNING id
    """
)


class ClassifierPaths:
    """Узлы дерева одного прогона: запрос к базе — на новый узел, а не на звонок.

    `dry_run` ищет, но не заводит: недостающий узел получает временный
    идентификатор и попадает в счёт `created`. Так команда без `--apply`
    показывает, что она добавила бы.
    """

    def __init__(
        self, session: AsyncSession, *, source: ClassifierSource, dry_run: bool = False
    ) -> None:
        """Настроить ClassifierPaths из переданных параметров и зависимостей."""
        self._session = session
        self._source = source
        self._dry_run = dry_run
        self._nodes: dict[tuple[uuid.UUID | None, str], uuid.UUID] = {}
        self.created = 0

    async def node(self, levels: Sequence[str]) -> uuid.UUID | None:
        """Узел последнего уровня пути; недостающие по дороге заводятся. `None` — пути нет."""
        parent: uuid.UUID | None = None
        for level, label in enumerate(levels[:CLASSIFIER_DEPTH], start=1):
            slot = (parent, label_key(label))
            if slot not in self._nodes:
                self._nodes[slot] = await self._ensure(slot, level, label)
            parent = self._nodes[slot]
        return parent

    async def _ensure(
        self, slot: tuple[uuid.UUID | None, str], level: int, label: str
    ) -> uuid.UUID:
        """Найти или создать узел дерева в переданной SQL-транзакции."""
        parent, key = slot
        found: uuid.UUID | None = await self._session.scalar(
            _FIND_NODE_SQL, {"parent_id": parent, "key": key}
        )
        if found is not None:
            return found
        if self._dry_run:
            self.created += 1
            return uuid.uuid4()
        inserted: uuid.UUID | None = await self._session.scalar(
            _INSERT_NODE_SQL,
            {
                "parent_id": parent,
                "level": level,
                "label": clean_label(label),
                "key": key,
                "source": self._source.value,
                "sort_order": NCC_SORT_ORDER,
            },
        )
        if inserted is not None:
            self.created += 1
            return inserted
        concurrent: uuid.UUID = await self._session.scalar(
            _FIND_NODE_SQL, {"parent_id": parent, "key": key}
        )
        return concurrent


NODES_SQL = text(
    "SELECT id, parent_id, level, label, sort_order, is_active FROM speech_classifier_nodes "
    "ORDER BY level, sort_order, label"
)
