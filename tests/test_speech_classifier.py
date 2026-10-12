"""Классификатор обращений без базы: написание, уровни по местам, файл владельца (SP-310).

Проверяется то, от чего зависит, станут ли два написания одного пути одним узлом
и не сдвинутся ли уровни, — и что файл в репозитории и есть дерево владельца.
"""

from __future__ import annotations

import json
import unicodedata
import uuid
from typing import Any

import pytest

from src.infrastructure.data.catalogues import CLASSIFIER_FILE, load_classifier
from src.infrastructure.persistence.classifier import (
    CLASSIFIER_DEPTH,
    EMPTY_LEVEL_LABEL,
    ClassifierIndex,
    NodePaths,
    branch_sql,
    classifier_levels,
    clean_label,
    dimensions,
    label_key,
    level_match,
    path_caption,
    present_levels,
    suggestion_labels,
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


def test_the_shipped_tree_is_the_owners_file() -> None:
    """«Тематики (финал) вариант 2», лист «Лист2»: 430 строк, 397 путей (2026-09-09)."""
    paths = load_classifier()

    assert len(paths) == 397
    assert {len(path) for path in paths} == {4, 5}
    assert {path[0] for path in paths} == {"Физ.лицо", "В2В"}
    assert {path[2] for path in paths} == {"Консультация", "Недовольство/негатив"}
    nodes = {
        tuple(label_key(level) for level in path[:depth])
        for path in paths
        for depth in range(1, len(path) + 1)
    }
    assert len(nodes) == 477


def test_no_two_paths_of_the_file_collapse_into_one() -> None:
    paths = load_classifier()

    assert len({tuple(label_key(level) for level in path) for path in paths}) == len(paths)


def test_the_file_lives_next_to_the_loader() -> None:
    assert CLASSIFIER_FILE.is_file()


@pytest.mark.parametrize(
    "content",
    [
        {"paths": []},
        {"paths": [["Физ.лицо", "", "Консультация"]]},
        {"paths": [["1", "2", "3", "4", "5", "6"]]},
        {"paths": [["Физ.лицо", 1]]},
        {"paths": [[" ", ""]]},
        [["Физ.лицо"]],
    ],
    ids=["no paths", "empty level inside", "six levels", "not labels", "blank", "not an object"],
)
def test_a_broken_file_is_refused_before_it_reaches_the_table(tmp_path: Any, content: Any) -> None:
    """Ошибка в файле должна быть видна до того, как половина дерева уедет в таблицу."""
    path = tmp_path / "classifier.json"
    path.write_text(json.dumps(content, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(ValueError):
        load_classifier(path)


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


def test_the_index_shows_only_switched_on_branches_in_file_order() -> None:
    fiz, shpd, mobile, b2b, b2b_shpd, empty, wallet = (uuid.uuid4() for _ in range(7))
    index = ClassifierIndex(
        [
            _node(mobile, fiz, 2, "Мобильная связь", order=2),
            _node(shpd, fiz, 2, "ШПД", order=1),
            _node(fiz, None, 1, "Физ.лицо", order=0),
            _node(b2b, None, 1, "В2В", order=3, active=False),
            _node(b2b_shpd, b2b, 2, "ШПД", order=4),
            _node(empty, None, 1, "", order=100_000),
            _node(wallet, empty, 2, "Кошелек", order=100_000),
        ]
    )

    tree = index.prompt_tree()

    assert tree == {
        "Физ.лицо": {"ШПД": {}, "Мобильная связь": {}},
        EMPTY_LEVEL_LABEL: {"Кошелек": {}},
    }
    assert list(tree["Физ.лицо"]) == ["ШПД", "Мобильная связь"], "порядок — место в файле"
    assert len(index) == 5, "выключенный узел уносит всю ветку под собой"


def test_a_path_is_resolved_by_keys_down_to_where_the_tree_ends() -> None:
    fiz, shpd, consultation = (uuid.uuid4() for _ in range(3))
    index = ClassifierIndex(
        [
            _node(fiz, None, 1, "Физ.лицо"),
            _node(shpd, fiz, 2, "ШПД"),
            _node(consultation, shpd, 3, "Консультация"),
        ]
    )

    full = index.resolve(["физ.лицо", "шпд", " консультация "])
    partial = index.resolve(["Физ.лицо", "ШПД", "Заявка", "Консультация"])
    lost = index.resolve(["Юрлицо", "ШПД"])

    assert (full.node_id, full.labels) == (consultation, ("Физ.лицо", "ШПД", "Консультация"))
    assert (partial.node_id, partial.labels) == (shpd, ("Физ.лицо", "ШПД")), (
        "за оборванным уровнем дальше не ищется"
    )
    assert (lost.node_id, lost.labels) == (None, ())


def test_the_empty_level_resolves_by_its_name_a_dash_or_a_blank() -> None:
    empty, wallet = uuid.uuid4(), uuid.uuid4()
    index = ClassifierIndex([_node(empty, None, 1, ""), _node(wallet, empty, 2, "Кошелек")])

    for first in (EMPTY_LEVEL_LABEL, "", "-"):
        assert index.resolve([first, "Кошелек"]).node_id == wallet


@pytest.mark.parametrize(
    ("labels", "expected"),
    [
        (("Физ.лицо", "ШПД", "Консультация"), ("individual", "broadband")),
        (("В2В", "Мобильная связь"), ("b2b", "mobile")),
        (("Физ.лицо", "Кошелек"), ("individual", "wallet")),
        (("Сотрудник салона", "ШПД"), (None, "broadband")),
        (("", "Кошелек"), (None, "wallet")),
        (("Физ.лицо",), ("individual", None)),
        ((), (None, None)),
    ],
)
def test_segment_and_product_are_the_first_two_levels(
    labels: tuple[str, ...], expected: tuple[str | None, str | None]
) -> None:
    """Решение S-21 поверх SP-310; уровню без места в перечислении значения нет."""
    assert dimensions(labels) == expected


# --------------------------------------------------------------------------- #
# Пути звонков: подписи, ветки, сравнение с оператором
# --------------------------------------------------------------------------- #


def _paths() -> tuple[NodePaths, dict[str, uuid.UUID]]:
    ids = {
        name: uuid.uuid4()
        for name in ("fiz", "shpd", "consult", "broken", "wifi", "mobile", "empty", "wallet")
    }
    rows = [
        _node(ids["fiz"], None, 1, "Физ.лицо", order=0),
        _node(ids["shpd"], ids["fiz"], 2, "ШПД", order=1),
        _node(ids["mobile"], ids["fiz"], 2, "Мобильная связь", order=2),
        _node(ids["consult"], ids["shpd"], 3, "Консультация", order=1),
        _node(ids["broken"], ids["consult"], 4, "Не работает", order=1),
        _node(ids["wifi"], ids["broken"], 5, "Wi-Fi", order=1, active=False),
        _node(ids["empty"], None, 1, "", order=100_000),
        _node(ids["wallet"], ids["empty"], 2, "Кошелек", order=100_000),
    ]
    return NodePaths(rows), ids


def test_a_node_is_captioned_by_its_whole_path_even_when_switched_off() -> None:
    paths, ids = _paths()

    assert paths.labels(ids["wifi"]) == (
        "Физ.лицо",
        "ШПД",
        "Консультация",
        "Не работает",
        "Wi-Fi",
    )
    assert paths.labels(ids["wallet"]) == ("", "Кошелек")
    assert paths.labels(None) == paths.labels(uuid.uuid4()) == ()
    assert path_caption(paths.labels(ids["wallet"])) == f"{EMPTY_LEVEL_LABEL} › Кошелек"
    assert paths.ancestor(ids["wifi"], 4) == ids["broken"]
    assert paths.ancestor(ids["consult"], 4) is None, "путь до уровня темы не дошёл"


def test_calls_roll_up_into_every_branch_above_their_node() -> None:
    paths, ids = _paths()

    branches = paths.rollup({ids["wifi"]: 2, ids["broken"]: 1, ids["mobile"]: 4})

    assert branches[ids["broken"]] == 3, "звонок с уточнением — звонок и про тему"
    assert branches[ids["fiz"]] == 7
    assert ids["wallet"] not in branches


def test_the_tree_order_puts_parents_first_and_the_file_before_ncc() -> None:
    paths, ids = _paths()
    names = {node: name for name, node in ids.items()}

    ordered = sorted(ids.values(), key=paths.order)

    assert [names[node] for node in ordered] == [
        "fiz",
        "shpd",
        "consult",
        "broken",
        "wifi",
        "mobile",
        "empty",
        "wallet",
    ]


@pytest.mark.parametrize(
    ("operator", "ai", "expected"),
    [
        (
            ("Физ.лицо", "ШПД", "Консультация", "Не работает", "Wi-Fi"),
            ("Физ.лицо", "ШПД", "Консультация", "Не работает", "Роутер"),
            (True, True, True, True, False),
        ),
        (
            ("", "Кошелек", "Консультация"),
            ("", "Кошелек", "Консультация", "Финансы"),
            (False, True, True, False, False),
        ),
        (
            ("Физ.лицо", "Мобильная связь", "Консультация", "Тарифы", "-"),
            ("В2В", "Мобильная связь", "консультация"),
            (False, True, True, False, False),
        ),
        (("Физ.лицо",), (), ()),
        ((), ("Физ.лицо",), ()),
    ],
    ids=["level five differs", "empty operator level", "dash and places", "no ai", "no operator"],
)
def test_levels_match_by_the_owners_rule(
    operator: tuple[str, ...], ai: tuple[str, ...], expected: tuple[bool, ...]
) -> None:
    """Пустой уровень оператора и прочерк не совпадают; уровни сравниваются на своих местах."""
    assert level_match(operator, ai) == expected


def test_present_levels_skip_blanks_and_dashes() -> None:
    assert present_levels(("", "Кошелек", "—", "Финансы")) == (False, True, False, True, False)


def test_the_suggestion_is_read_as_a_list_of_labels() -> None:
    assert suggestion_labels(["Физ.лицо", "ШПД"]) == ("Физ.лицо", "ШПД")
    assert suggestion_labels('["Физ.лицо", "Новая тема"]') == ("Физ.лицо", "Новая тема")
    assert suggestion_labels(None) == suggestion_labels({"path": []}) == ()


def test_the_branch_condition_binds_its_parameter_and_walks_down() -> None:
    condition = branch_sql("c.ai_node_id", "node_id")

    assert condition.startswith("(CAST(:node_id AS uuid) IS NULL OR c.ai_node_id IN (")
    assert "WITH RECURSIVE branch" in condition
    assert "n.parent_id = b.id" in condition
