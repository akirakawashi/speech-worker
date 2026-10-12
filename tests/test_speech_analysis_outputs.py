"""Разбор ответов модели: форма, роли, классификатор и причины обращения."""

from __future__ import annotations

import json
import uuid
from typing import Any

import pytest

from src.application.pipeline.analyze import (
    SUGGESTION_LABEL_LIMIT,
    SUMMARY_LIMIT,
    _interpret,
    _require_shape,
    describe_raw_problem,
    normalise_outputs,
)
from src.infrastructure.persistence.classifier import EMPTY_LEVEL_LABEL, ClassifierIndex, label_key

#: Перевод строки отдельным именем: этот файл не раз правился скриптами,
#: а обратный слэш в них теряется незаметно.
NEWLINE = chr(10)

WIFI_PATH = ["Физ.лицо", "ШПД", "Консультация", "Не работает", "Wi-Fi"]

_GOOD = {
    "path": WIFI_PATH,
    "sentiment": "negative",
    "summary": "Клиент третий день без интернета, заявка передана в бригаду.",
    "roles": [{"seq": 1, "speaker": "operator"}, {"seq": 2, "speaker": "client"}],
}


def _index() -> ClassifierIndex:
    """Ветка «Физ.лицо › ШПД › Консультация › Не работает › Wi-Fi» и «Кошелек» без уровня 1."""
    ids = [uuid.uuid4() for _ in range(7)]
    rows = [
        (ids[0], None, 1, "Физ.лицо"),
        (ids[1], ids[0], 2, "ШПД"),
        (ids[2], ids[1], 3, "Консультация"),
        (ids[3], ids[2], 4, "Не работает"),
        (ids[4], ids[3], 5, "Wi-Fi"),
        (ids[5], None, 1, ""),
        (ids[6], ids[5], 2, "Кошелек"),
    ]
    return ClassifierIndex(
        {"id": node, "parent_id": parent, "level": level, "label": label, "key": label_key(label)}
        for node, parent, level, label in rows
    )


def _answer(payload: Any, wrapper: str = "{body}") -> dict[str, Any]:
    """Ответ модели под ключом answer для проверки прикладным кодом."""
    body = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
    return {"answer": wrapper.format(body=body)}


# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #


#: Тем же правилом, каким LLM разбирает ссылки на переменные в промпте.
#: Скопировано дословно из `graphon/nodes/base/variable_template_parser.py`:
#: дефиса в наборе символов нет, и это не мелочь — см. тест ниже.


# --------------------------------------------------------------------------- #
# Разбор ответа модели
# --------------------------------------------------------------------------- #


def test_a_clean_answer_passes_through() -> None:
    outputs = normalise_outputs(_answer(_GOOD))

    _require_shape(outputs)
    work = _interpret(outputs, classifier=_index(), needs_roles=True)
    assert work.path == tuple(WIFI_PATH)
    assert work.node_id is not None
    assert work.sentiment is not None
    assert work.sentiment.value == "negative"
    assert len(work.roles) == 2


def test_the_reasoning_block_is_cut_before_parsing() -> None:
    """Рассуждающая модель пишет черновой JSON внутри `<think>`.

    Взято с настоящего прогона на боевом контуре: модель рассуждает вслух,
    набрасывает вариант ответа — и только потом даёт настоящий. Ища первую `{`,
    разбор взял бы скобку из черновика, а последнюю `}` — из ответа, и между
    ними осталась бы проза. `json.loads` на такое отказывает, то есть обе
    попытки этапа ушли бы на одно и то же.
    """
    draft = {"path": ["Физ.лицо"], "sentiment": "neutral", "summary": "нет данных", "roles": []}
    raw = (
        "<think>"
        + NEWLINE
        + "Надо вернуть JSON. Черновик:"
        + NEWLINE
        + json.dumps(draft, ensure_ascii=False)
        + NEWLINE
        + "Но ветка всё-таки глубже."
        + NEWLINE
        + "</think>"
        + NEWLINE
        + NEWLINE
        + json.dumps(_GOOD, ensure_ascii=False)
    )

    outputs = normalise_outputs({"answer": raw})

    _require_shape(outputs)
    assert outputs["path"] == WIFI_PATH, "взят черновик из рассуждения"
    assert outputs["sentiment"] == "negative"


def test_a_truncated_answer_says_it_ran_out_of_tokens() -> None:
    """Оборванное рассуждение — не поломка промпта, а потолок токенов.

    Проверено на настоящем звонке: 34 реплики, ответ 7310 символов, `<think>`
    без закрытия, потолок стоял 2048. Записав в лог «path is missing», мы
    отправили бы читателя искать ошибку в промпте.
    """
    raw = "<think>" + NEWLINE + "Долго думаю про ветки" + NEWLINE + ("рассуждение " * 400)

    problem = describe_raw_problem({"answer": raw})

    assert problem is not None
    assert "max_tokens" in problem
    assert "path" not in problem


def test_an_answer_without_json_is_named_as_such() -> None:
    """Модель ответила словами. Это тоже не «поля не хватает»."""
    problem = describe_raw_problem({"answer": "Не могу разобрать этот разговор."})

    assert problem is not None
    assert "JSON" in problem


def test_an_empty_workflow_answer_is_named_as_such() -> None:
    """Пустой выход воркфлоу — это отказ воркфлоу, а не ответ модели."""
    assert describe_raw_problem({}) == "the model returned no answer at all"
    assert describe_raw_problem({"answer": "   "}) == "the model returned no answer at all"


def test_a_good_answer_has_no_problem_to_describe() -> None:
    assert describe_raw_problem(_answer(_GOOD)) is None
    # Разобранная форма тоже не вызывает жалоб.
    assert describe_raw_problem(dict(_GOOD)) is None


def test_an_unclosed_reasoning_block_is_refused() -> None:
    """Обрыв ответа посреди рассуждения — это не ответ, а черновик."""
    raw = "<think>" + NEWLINE + "Подумаю…" + NEWLINE + json.dumps(_GOOD, ensure_ascii=False)

    with pytest.raises(ValueError):
        _require_shape(normalise_outputs({"answer": raw}))


@pytest.mark.parametrize(
    "wrapper",
    [
        "```json\n{body}\n```",
        "```\n{body}\n```",
        "Вот результат: {body}",
        "{body}\n\nЕсли нужно уточнить — скажите.",
        "  \n{body}\n  ",
    ],
)
def test_the_answer_is_found_inside_what_the_model_added_around_it(wrapper: str) -> None:
    """Модель оборачивает объект, хотя её просили не оборачивать.

    Снимать обёртку здесь дешевле, чем тратить на это вторую попытку этапа: у
    неё их всего две, и обе уйдут на одно и то же.
    """
    outputs = normalise_outputs(_answer(_GOOD, wrapper))

    _require_shape(outputs)
    assert outputs["path"] == WIFI_PATH


@pytest.mark.parametrize("raw", ["", "   ", "не могу ответить", "{не json}", "[1, 2, 3]", "null"])
def test_an_unusable_answer_is_refused_not_guessed(raw: str) -> None:
    """Пустой объект означает «модель не ответила», и этап это увидит.

    Подставив `neutral`, мы получили бы отчёт, в котором нейтральные звонки —
    смесь настоящих нейтральных и тех, что модель не разметила.
    """
    with pytest.raises(ValueError):
        _require_shape(normalise_outputs(_answer(raw)))


@pytest.mark.parametrize("value", ["neutral ", "NEGATIVE", "смешанная", "", None, 5])
def test_a_sentiment_outside_the_three_values_is_refused(value: Any) -> None:
    """Три значения — закрытый набор. Четвёртое не «почти подходит», а не подходит."""
    outputs = normalise_outputs(_answer({**_GOOD, "sentiment": value}))

    with pytest.raises(ValueError, match="sentiment"):
        _require_shape(outputs)


@pytest.mark.parametrize("value", [None, [], [""], ["  ", None], "Физ.лицо", {"level": "ШПД"}])
def test_a_missing_path_is_refused(value: Any) -> None:
    outputs = normalise_outputs(_answer({**_GOOD, "path": value}))

    with pytest.raises(ValueError, match="path"):
        _require_shape(outputs)


def test_the_already_parsed_form_is_left_alone() -> None:
    """Уже разобранный JSON совместим с прикладным разбором."""
    parsed = dict(_GOOD)

    assert normalise_outputs(parsed) is parsed


def test_roles_are_dropped_when_nobody_asked_for_them() -> None:
    """Атрибуция по энергии каналов — измерение, по тексту — мнение.

    Позволить модели переписать измерение значит обменять его на мнение.
    """
    outputs = normalise_outputs(_answer(_GOOD))

    assert _interpret(outputs, classifier=_index(), needs_roles=False).roles == ()


def test_a_broken_role_is_skipped_and_the_rest_survive() -> None:
    """Одна испорченная реплика не должна стоить атрибуции всему разговору."""
    outputs = normalise_outputs(
        _answer(
            {
                **_GOOD,
                "roles": [
                    {"seq": 1, "speaker": "operator"},
                    {"seq": "два", "speaker": "client"},
                    {"speaker": "client"},
                    {"seq": 4, "speaker": "робот"},
                    "мусор",
                    {"seq": 5, "speaker": "client"},
                ],
            }
        )
    )

    roles = _interpret(outputs, classifier=_index(), needs_roles=True).roles
    assert [seq for seq, _ in roles] == [1, 5]


def test_a_long_summary_is_trimmed_to_the_agreed_limit() -> None:
    outputs = normalise_outputs(_answer({**_GOOD, "summary": "а" * 900}))

    work = _interpret(outputs, classifier=_index(), needs_roles=False)
    assert work.summary is not None
    assert len(work.summary) == SUMMARY_LIMIT


def test_a_path_off_the_tree_keeps_what_was_found() -> None:
    """Уровень мимо дерева узлов не заводит: остаётся найденное выше."""
    outputs = normalise_outputs(_answer({**_GOOD, "path": ["Физ.лицо", "ШПД", "Выдумка"]}))

    work = _interpret(outputs, classifier=_index(), needs_roles=False)
    assert work.path == ("Физ.лицо", "ШПД")


def test_a_dash_after_the_last_level_is_no_level() -> None:
    """Прочерк пятым уровнем — «уточнений нет», как пишут операторы."""
    outputs = normalise_outputs(_answer({**_GOOD, "path": [*WIFI_PATH[:4], "-"]}))

    work = _interpret(outputs, classifier=_index(), needs_roles=False)
    assert work.path == tuple(WIFI_PATH[:4])


# --------------------------------------------------------------------------- #
# Тема по предложению ИИ помощника (SP-309, путём с SP-310)
# --------------------------------------------------------------------------- #


def _suggest(value: Any) -> tuple[tuple[str, ...], bool]:
    outputs = normalise_outputs(_answer({**_GOOD, "ai_path": value}))
    work = _interpret(outputs, classifier=_index(), needs_roles=False)
    return work.suggestion, work.suggestion_node_id is not None


def test_a_suggestion_along_the_tree_is_a_node_with_the_tree_labels() -> None:
    """Иначе одна ветка считалась бы в отчёте дважды — узлом и словами."""
    assert _suggest(["физ.лицо", "шпд", "консультация"]) == (
        ("Физ.лицо", "ШПД", "Консультация"),
        True,
    )


def test_a_suggestion_may_word_any_level_itself() -> None:
    """Формулировки нет в дереве — она живёт в звонке, а дерево не меняется."""
    wording = [*WIFI_PATH[:3], "Перенос номера к другому оператору"]

    assert _suggest(wording) == (tuple(wording), False)


def test_an_empty_level_in_a_suggestion_is_read_by_its_name() -> None:
    assert _suggest([EMPTY_LEVEL_LABEL, "Кошелек"]) == (("", "Кошелек"), True)


@pytest.mark.parametrize("value", [None, 5, [], {}, [""], ["  ", None], "Перенос номера"])
def test_a_missing_or_broken_suggestion_leaves_the_field_empty(value: Any) -> None:
    """Модель может поля не дать: разбор обязан работать и без него."""
    _require_shape(normalise_outputs(_answer({**_GOOD, "ai_path": value})))

    assert _suggest(value) == ((), False)


def test_a_long_wording_is_trimmed_and_its_whitespace_collapsed() -> None:
    suggestion, found = _suggest([*WIFI_PATH[:3], "  Очень\n длинная   тема " + "х" * 300])

    assert not found
    assert len(suggestion[-1]) == SUGGESTION_LABEL_LIMIT
    assert suggestion[-1].startswith("Очень длинная тема")


# --------------------------------------------------------------------------- #
# Причина обращения (S-32)
# --------------------------------------------------------------------------- #

_CODES = frozenset({"K05", "K91", "K99"})


def test_a_reason_and_its_code_are_read_from_the_answer() -> None:
    outputs = normalise_outputs(
        _answer(
            {
                **_GOOD,
                "call_reason": "Не работает домашний интернет, горит LOS.",
                "call_reason_code": "K05",
            }
        )
    )

    work = _interpret(outputs, classifier=_index(), needs_roles=False, reason_codes=_CODES)

    assert (work.call_reason, work.call_reason_code) == (
        "Не работает домашний интернет, горит LOS.",
        "K05",
    )


@pytest.mark.parametrize(
    "answered", ["K05 · Не работает домашний интернет", " k05", "K05.", "К05", "к05 · кириллицей"]
)
def test_a_code_with_its_label_or_in_another_case_is_still_the_code(answered: str) -> None:
    outputs = normalise_outputs(_answer({**_GOOD, "call_reason_code": answered}))

    work = _interpret(outputs, classifier=_index(), needs_roles=False, reason_codes=_CODES)

    assert work.call_reason_code == "K05"


@pytest.mark.parametrize("answered", ["K07", "K5", "Не работает интернет", "", None, 5])
def test_a_code_outside_the_reference_sent_is_not_kept(answered: Any) -> None:
    """Ни «похожего» кода, ни умолчания: мимо списка — пусто."""
    outputs = normalise_outputs(_answer({**_GOOD, "call_reason_code": answered}))

    work = _interpret(outputs, classifier=_index(), needs_roles=False, reason_codes=_CODES)

    assert work.call_reason_code is None


def test_an_answer_without_a_reason_is_still_a_marked_call() -> None:
    """Поля причины необязательны: без них разбор звонка не теряется."""
    outputs = normalise_outputs(_answer(_GOOD))

    _require_shape(outputs)
    work = _interpret(outputs, classifier=_index(), needs_roles=False, reason_codes=_CODES)

    assert (work.call_reason, work.call_reason_code) == (None, None)
    assert work.node_id is not None


def test_without_a_reference_the_reason_is_not_taken() -> None:
    """Справочник не загружен — модели его не показывали, и её «причину» брать не из чего."""
    outputs = normalise_outputs(
        _answer({**_GOOD, "call_reason": "Текст.", "call_reason_code": "K05"})
    )

    work = _interpret(outputs, classifier=_index(), needs_roles=False)

    assert (work.call_reason, work.call_reason_code) == (None, None)


def test_a_long_reason_is_trimmed_to_the_agreed_limit() -> None:
    from src.domain.calls.reasons import REASON_LIMIT  # noqa: PLC0415

    outputs = normalise_outputs(
        _answer({**_GOOD, "call_reason": "Очень " * 100, "call_reason_code": "K99"})
    )

    work = _interpret(outputs, classifier=_index(), needs_roles=False, reason_codes=_CODES)

    assert work.call_reason is not None
    assert len(work.call_reason) <= REASON_LIMIT
