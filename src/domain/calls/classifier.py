"""Нормализовать уровни классификации из метаданных источника NCC."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from typing import Final

CLASSIFIER_DEPTH: Final[int] = 5


LABEL_LIMIT: Final[int] = 255


NCC_SORT_ORDER: Final[int] = 100_000


EMPTY_LEVEL_LABEL: Final[str] = "(пусто)"


PATH_SEPARATOR: Final[str] = " › "


_INVISIBLE: Final = dict.fromkeys(map(ord, "​‌‍⁠﻿­"))


_SLASH: Final[re.Pattern[str]] = re.compile(r"\s*/\s*")


def clean_label(value: object, limit: int | None = LABEL_LIMIT) -> str:
    """Название уровня: одна форма Unicode, без невидимых знаков, пробелы одинарные.

    Вход — значение источника и лимит; выход — нормализованная строка.
    `limit=None` сохраняет полную длину.
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
