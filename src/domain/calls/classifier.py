"""Чистые правила путей классификатора и сравнения классификаций."""

from __future__ import annotations

import json
import re
import unicodedata
import uuid
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Final

from pydantic import BaseModel, ConfigDict

from src.domain.calls.contracts import Product, Segment

CLASSIFIER_DEPTH: Final[int] = 5


LABEL_LIMIT: Final[int] = 255


NCC_SORT_ORDER: Final[int] = 100_000


EMPTY_LEVEL_LABEL: Final[str] = "(пусто)"


PATH_SEPARATOR: Final[str] = " › "


TOPIC_LEVEL: Final[int] = 4


_INVISIBLE: Final = dict.fromkeys(map(ord, "​‌‍⁠﻿­"))


_SLASH: Final[re.Pattern[str]] = re.compile(r"\s*/\s*")


def clean_label(value: object, limit: int | None = LABEL_LIMIT) -> str:
    """Название уровня: одна форма Unicode, без невидимых знаков, пробелы одинарные.

    `limit=None` — без обрезки: так читает файл справочника причин, где длинная
    строка — ошибка правки, которую надо показать, а не молча укоротить.
    """
    if value is None:
        return ""
    normalised = unicodedata.normalize("NFC", str(value)).translate(_INVISIBLE)
    joined = " ".join(normalised.split())
    return joined if limit is None else joined[:limit]


def label_key(value: object) -> str:
    """Ключ узла — название после выравнивания написания.

    Регистр не различается, «_» и «\\» считаются «/», пробелы вокруг «/» не
    считаются. Так «Недовольство_негатив» и «Недовольство/негатив», «Баланс\\
    расходы» и «Баланс_ расходы» — один узел.
    """
    folded = clean_label(value).casefold().replace("\\", "/").replace("_", "/")
    return _SLASH.sub("/", folded)


_PLACEHOLDER: Final[re.Pattern[str]] = re.compile(r"[-–—]+")


def _is_empty_level(label: str) -> bool:
    """Проверить, означает ли подпись отсутствие уровня классификации."""
    return not label or label == EMPTY_LEVEL_LABEL or bool(_PLACEHOLDER.fullmatch(label))


def classifier_levels(values: Iterable[object]) -> tuple[str, ...]:
    """Уровни классификации по местам: пустой в середине остаётся, пустой хвост снимается.

    Прочерк считается пустым уровнем. Глубже пяти уровней классификации не
    бывает, и лишнее отбрасывается: у выгрузки файлами путь задают папки, а их
    глубина не обещана.
    """
    levels = [
        "" if _PLACEHOLDER.fullmatch(label) else label
        for label in (clean_label(value) for value in values)
    ][:CLASSIFIER_DEPTH]
    while levels and not levels[-1]:
        levels.pop()
    return tuple(levels)


class ClassifierNode(BaseModel):
    """Узел классификатора с положением в дереве и доступностью."""

    model_config = ConfigDict(frozen=True)

    id: uuid.UUID
    parent_id: uuid.UUID | None
    level: int
    label: str
    key: str
    sort_order: int = 0


class ResolvedPath(BaseModel):
    """Докуда путь нашёлся в дереве: узел последнего найденного уровня и названия уровней."""

    model_config = ConfigDict(frozen=True)

    node_id: uuid.UUID | None
    labels: tuple[str, ...]


class ClassifierIndex:
    """Включённое дерево: что показать модели и как прочесть путь из её ответа.

    Узел, выключенный сам или под выключенным предком, сюда не попадает: модель
    его не видит, и в её ответе он не находится.
    """

    def __init__(self, rows: Iterable[Mapping[str, Any]]) -> None:
        """Настроить ClassifierIndex из переданных параметров и зависимостей."""
        by_parent: dict[uuid.UUID | None, list[ClassifierNode]] = {}
        for row in rows:
            if not row.get("is_active", True):
                continue
            node = ClassifierNode(
                id=row["id"],
                parent_id=row["parent_id"],
                level=int(row["level"]),
                label=str(row["label"]),
                key=str(row["key"]),
                sort_order=int(row.get("sort_order") or 0),
            )
            by_parent.setdefault(node.parent_id, []).append(node)

        self._ordered: dict[uuid.UUID | None, list[ClassifierNode]] = {}
        self._by_key: dict[uuid.UUID | None, dict[str, ClassifierNode]] = {}
        pending: list[uuid.UUID | None] = [None]
        while pending:
            parent = pending.pop()
            kids = sorted(by_parent.get(parent, ()), key=lambda n: (n.sort_order, n.label))
            if not kids:
                continue
            self._ordered[parent] = kids
            self._by_key[parent] = {kid.key: kid for kid in kids}
            pending.extend(kid.id for kid in kids)

    def __len__(self) -> int:
        """Вернуть число узлов в снимке классификатора."""
        return sum(len(kids) for kids in self._ordered.values())

    def prompt_tree(self) -> dict[str, Any]:
        """Дерево для модели: название уровня — его уточнения, `{}` — уточнений нет."""

        def branch(parent: uuid.UUID | None) -> dict[str, Any]:
            """Вернуть узлы указанной ветки снимка дерева."""
            return {
                (kid.label or EMPTY_LEVEL_LABEL): branch(kid.id)
                for kid in self._ordered.get(parent, ())
            }

        return branch(None)

    def resolve(self, labels: Sequence[object]) -> ResolvedPath:
        """Пройти путь от корня по ключам и остановиться там, где дерево кончилось.

        Путь, оборванный на уровне, которого в дереве нет, остаётся тем, что
        нашлось выше: «Физ.лицо › ШПД › Консультация» — честный ответ, когда
        уровень 4 модель назвала мимо дерева.
        """
        parent: uuid.UUID | None = None
        node_id: uuid.UUID | None = None
        found: list[str] = []
        for label in list(labels)[:CLASSIFIER_DEPTH]:
            cleaned = clean_label(label)
            key = "" if _is_empty_level(cleaned) else label_key(cleaned)
            child = self._by_key.get(parent, {}).get(key)
            if child is None:
                break
            found.append(child.label)
            node_id = parent = child.id
        return ResolvedPath(node_id=node_id, labels=tuple(found))


class NodePaths:
    """Узел — путь от корня: названия уровней, узел нужного уровня, место в дереве."""

    def __init__(self, rows: Iterable[Mapping[str, Any]]) -> None:
        """Настроить NodePaths из переданных параметров и зависимостей."""
        self._nodes: dict[uuid.UUID, Mapping[str, Any]] = {row["id"]: row for row in rows}

    def __contains__(self, node_id: object) -> bool:
        """Проверить, присутствует ли идентификатор в снимке дерева."""
        return node_id in self._nodes

    def node(self, node_id: uuid.UUID) -> Mapping[str, Any]:
        """Вернуть метаданные узла снимка дерева по его UUID."""
        return self._nodes[node_id]

    def ids(self) -> tuple[uuid.UUID, ...]:
        """Все узлы дерева."""
        return tuple(self._nodes)

    def lineage(self, node_id: uuid.UUID | None) -> tuple[uuid.UUID, ...]:
        """Узлы пути от корня до `node_id` включительно. Неизвестный узел — пустой путь."""
        chain: list[uuid.UUID] = []
        current = node_id
        while current is not None and current in self._nodes and len(chain) < CLASSIFIER_DEPTH:
            chain.append(current)
            current = self._nodes[current]["parent_id"]
        return tuple(reversed(chain))

    def labels(self, node_id: uuid.UUID | None) -> tuple[str, ...]:
        """Вернуть подписи пути от корня до узла; неизвестному узлу соответствует пустой путь."""
        return tuple(str(self._nodes[node]["label"]) for node in self.lineage(node_id))

    def ancestor(self, node_id: uuid.UUID | None, level: int) -> uuid.UUID | None:
        """Узел пути на уровне `level`; путь до него не доходит — `None`."""
        return next(
            (node for node in self.lineage(node_id) if int(self._nodes[node]["level"]) == level),
            None,
        )

    def order(self, node_id: uuid.UUID | None) -> tuple[tuple[int, str], ...]:
        """Ключ сортировки в порядке дерева: на каждом уровне — место в файле, затем название."""
        return tuple(
            (int(self._nodes[node]["sort_order"] or 0), str(self._nodes[node]["label"]))
            for node in self.lineage(node_id)
        )

    def rollup(self, counts: Mapping[uuid.UUID, int]) -> dict[uuid.UUID, int]:
        """Звонки узлов — в звонки веток: звонок считается у своего узла и у всех над ним."""
        branches: dict[uuid.UUID, int] = {}
        for node_id, calls in counts.items():
            for node in self.lineage(node_id):
                branches[node] = branches.get(node, 0) + calls
        return branches


def suggestion_labels(value: object) -> tuple[str, ...]:
    """Предложение ИИ помощника из `speech_calls.ai_suggestion` — названия уровней."""
    if isinstance(value, str):
        value = json.loads(value)
    return tuple(str(level) for level in value) if isinstance(value, list) else ()


def path_caption(labels: Sequence[str]) -> str:
    """Путь одной строкой. Пустой уровень в середине — «(пусто)», как в дереве для модели."""
    return PATH_SEPARATOR.join(label or EMPTY_LEVEL_LABEL for label in labels)


def present_levels(labels: Sequence[str]) -> tuple[bool, ...]:
    """Есть ли уровень на каждом из пяти мест. Пустой уровень и прочерк — нет."""
    return tuple(
        index < len(labels) and not _is_empty_level(clean_label(labels[index]))
        for index in range(CLASSIFIER_DEPTH)
    )


def level_match(operator: Sequence[str], ai: Sequence[str]) -> tuple[bool, ...]:
    """Совпал ли путь ИИ с путём оператора на каждом из пяти уровней.

    Правило владельца (2026-09-15, разведка отчёта TSS Volna): уровень совпал,
    если у оператора он есть и после выравнивания написания равен уровню ИИ на
    том же месте. Пустой уровень оператора или прочерк — не совпал. Совпадение
    звонка — число совпавших уровней из пяти. Уровни сравниваются каждый на своём
    месте — так же считал замер этапа 2.

    Пустой ответ — сравнивать не с чем: у звонка нет пути оператора или пути ИИ.
    """
    if not operator or not ai:
        return ()
    present = present_levels(operator)
    return tuple(
        present[index] and index < len(ai) and label_key(operator[index]) == label_key(ai[index])
        for index in range(CLASSIFIER_DEPTH)
    )


_SEGMENT_BY_KEY: Final[dict[str, Segment]] = {
    "физ.лицо": Segment.INDIVIDUAL,
    "в2в": Segment.B2B,
    "b2b": Segment.B2B,
}


_PRODUCT_BY_KEY: Final[dict[str, Product]] = {
    "кошелек": Product.WALLET,
    "кошелёк": Product.WALLET,
    "мобильная связь": Product.MOBILE,
    "шпд": Product.BROADBAND,
}


def dimensions(labels: Sequence[str]) -> tuple[str | None, str | None]:
    """Сегмент и продукт — уровни 1 и 2 пути (решение S-21 поверх SP-310).

    Уровень, которому в перечислении нет места («Сотрудник салона», пустой
    уровень), значения не даёт: «неизвестно» честнее подставленного.
    """
    segment = _SEGMENT_BY_KEY.get(label_key(labels[0])) if labels else None
    product = _PRODUCT_BY_KEY.get(label_key(labels[1])) if len(labels) > 1 else None
    return (segment.value if segment else None, product.value if product else None)


__all__ = [
    "CLASSIFIER_DEPTH",
    "EMPTY_LEVEL_LABEL",
    "PATH_SEPARATOR",
    "TOPIC_LEVEL",
    "ClassifierIndex",
    "ClassifierNode",
    "NodePaths",
    "ResolvedPath",
    "classifier_levels",
    "clean_label",
    "dimensions",
    "label_key",
    "level_match",
    "path_caption",
    "present_levels",
    "suggestion_labels",
]
