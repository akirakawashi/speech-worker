"""Классификатор обращений без базы: написание, уровни по местам, файл владельца (SP-310).

Проверяется то, от чего зависит, станут ли два написания одного пути одним узлом
и не сдвинутся ли уровни, — и что файл в репозитории и есть дерево владельца.
"""

from __future__ import annotations

import unicodedata
import uuid
from typing import Any

import pytest

from src.application.dto.classifier import (
    CLASSIFIER_DEPTH,
    classifier_levels,
    clean_label,
    label_key,
)


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("Недовольство_негатив", "Недовольство/негатив"),
        ("Баланс\\ расходы", "Баланс_ расходы"),
        (
            "ЛК_МП – некорректное отображение информации",
            "ЛК/МП – некорректное отображение информации",
        ),
        ("Мобильный интернет", unicodedata.normalize("NFD", "Мобильный интернет")),
        ("  Wi-Fi ", "WI-FI"),
        ("Проводной интернет", "Проводной интернет"),
        ("Сорвался​/тишина/неконструктив", "Сорвался_тишина_неконструктив"),
    ],
)
def test_two_spellings_of_one_label_share_a_key(left: str, right: str) -> None:
    assert label_key(left) == label_key(right)


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("Роуминг", "Мобильный интернет"),
        ("ТВ", "Качество ТВ"),
        ("Консультация", "Недовольство/негатив"),
        ("", "-"),
    ],
)
def test_different_labels_keep_different_keys(left: str, right: str) -> None:
    assert label_key(left) != label_key(right)


def test_the_label_keeps_its_spelling_and_loses_only_noise() -> None:
    """Выравнивается ключ, а не название: «_» в названии узла из NCC остаётся."""
    assert clean_label("  Недовольство_негатив ") == "Недовольство_негатив"
    assert clean_label(unicodedata.normalize("NFD", "й")) == "й"
    assert clean_label(None) == ""
    assert len(clean_label("я" * 300)) == 255, "ширина колонки"


def test_levels_keep_their_places() -> None:
    assert classifier_levels(["", "Кошелек", "Консультация", "Финансы", None]) == (
        "",
        "Кошелек",
        "Консультация",
        "Финансы",
    ), "пустой уровень 1 остаётся, пустой хвост снимается"
    assert classifier_levels(["Физ.лицо", "ШПД", "  ", "Баланс", ""]) == (
        "Физ.лицо",
        "ШПД",
        "",
        "Баланс",
    )
    assert classifier_levels([None, None, None, None, None]) == ()
    assert len(classifier_levels(["1", "2", "3", "4", "5", "6"])) == CLASSIFIER_DEPTH


def test_a_dash_in_place_of_a_level_is_no_level() -> None:
    """Операторы ставят «-» пятым уровнем там, где уточнений нет: узлом он не становится."""
    assert classifier_levels(
        ["Физ.лицо", "Кошелек", "Консультация", "Как стать клиентом", "-"]
    ) == (
        "Физ.лицо",
        "Кошелек",
        "Консультация",
        "Как стать клиентом",
    )
    assert classifier_levels(["—", "Кошелек", "Консультация"]) == ("", "Кошелек", "Консультация")
    assert classifier_levels(["Физ.лицо", "ШПД", "Консультация", "Не работает", "Wi-Fi"])[-1] == (
        "Wi-Fi"
    ), "дефис внутри названия — не прочерк"


# --------------------------------------------------------------------------- #
# Файл владельца
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# Дерево в памяти этапа анализа
# --------------------------------------------------------------------------- #


def _node(
    node: uuid.UUID,
    parent: uuid.UUID | None,
    level: int,
    label: str,
    *,
    order: int = 0,
    active: bool = True,
) -> dict[str, Any]:
    return {
        "id": node,
        "parent_id": parent,
        "level": level,
        "label": label,
        "key": label_key(label),
        "sort_order": order,
        "is_active": active,
    }


# --------------------------------------------------------------------------- #
# Пути звонков: подписи, ветки, сравнение с оператором
# --------------------------------------------------------------------------- #
