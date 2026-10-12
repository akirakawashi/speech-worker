"""Воркфлоу Dify «Speech: анализ звонка» — договор и разбор ответа модели.

Проверяются две вещи вместе: файл, который импортируют в Dify, и наш разбор его
ответа. Порознь они бессмысленны — воркфлоу отдаёт текст, годность которого
решает наш код, и разойтись они могут от одной опечатки в промпте.

Разбор живёт у нас, а не узлом кода внутри Dify. Первая редакция была именно
такой, и узел не исполнился ни на боевом контуре (`403` от песочницы), ни на
локальном той же версии («operation not permitted»). Разбору там и не место: он
решает, годен ли ответ модели, — то есть отвечает за поведение этапа, — а полем
в интерфейсе он не покрыт ни типами, ни тестами, ни историей правок.
"""

from __future__ import annotations

import json
import re
import uuid
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.application.pipeline.analyze import (
    SUGGESTION_LABEL_LIMIT,
    SUMMARY_LIMIT,
    _interpret,
    _require_shape,
    describe_raw_problem,
    normalise_outputs,
)
from src.infrastructure.persistence.classifier import EMPTY_LEVEL_LABEL, ClassifierIndex, label_key

#: Файл импортируют в Dify руками, в образ он не попадает. Лежит в этом
#: репозитории (до 23.09.2026 — в репозитории документации, и без соседнего
#: checkout эти тесты молча пропускались).
DSL_PATH = Path(__file__).resolve().parents[1] / "dify" / "speech" / "speech-call-analysis.dify.yml"

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
    """Выход воркфлоу: ответ модели одним полем, как его отдаёт узел конца."""
    body = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
    return {"answer": wrapper.format(body=body)}


# --------------------------------------------------------------------------- #
# Файл, который импортируют в Dify
# --------------------------------------------------------------------------- #


def _graph() -> dict[str, Any]:
    loaded: dict[str, Any] = yaml.safe_load(DSL_PATH.read_text(encoding="utf-8"))
    graph: dict[str, Any] = loaded["workflow"]["graph"]
    return graph


def _node(kind: str) -> dict[str, Any]:
    node: dict[str, Any] = next(n["data"] for n in _graph()["nodes"] if n["data"]["type"] == kind)
    return node


def _prompts() -> str:
    return " ".join(part["text"] for part in _node("llm")["prompt_template"])


def test_the_workflow_needs_no_sandbox() -> None:
    """Узла кода в воркфлоу нет, и это не упрощение, а требование.

    Он не исполнился ни на боевом контуре, ни на локальном той же версии —
    зависит от службы песочницы, которую чинить можно не везде и не сразу.
    """
    kinds = [n["data"]["type"] for n in _graph()["nodes"]]

    assert kinds == ["start", "llm", "end"]
    assert "code" not in kinds


def test_the_end_returns_what_the_model_said() -> None:
    """Имя поля — часть договора: наш разбор ищет ответ по нему."""
    from src.application.pipeline.analyze import _RAW_ANSWER_KEYS  # noqa: PLC0415

    produced = [o["variable"] for o in _node("end")["outputs"]]

    assert produced == ["answer"]
    assert produced[0] in _RAW_ANSWER_KEYS


def test_the_input_is_exactly_what_the_stage_sends() -> None:
    """Логического типа у полей входа Dify нет, и флаг идёт числом; дерево и
    реплики — абзацами, потому что поля скалярные.

    Лишнее обязательное поле на входе — это `400` на каждый звонок: этап его не
    шлёт.
    """
    variables = {v["variable"]: v["type"] for v in _node("start")["variables"]}

    assert variables == {
        "call_id": "text-input",
        "needs_roles": "number",
        "classifier": "paragraph",
        "reasons": "paragraph",
        "segments": "paragraph",
    }


def test_the_reason_reference_is_optional_so_the_rollout_order_does_not_matter() -> None:
    """Справочник причин (S-32) — необязательный вход.

    Обязательный вход, которого этап не прислал, — `400` на каждый звонок и ночь
    в `dead`. Необязательный безопасен в обе стороны: новый этап со старой
    редакцией воркфлоу — Dify отбрасывает необъявленный вход; новая редакция со
    старым этапом — пустое значение, поля причины не заполняются.
    """
    reasons = next(v for v in _node("start")["variables"] if v["variable"] == "reasons")

    assert reasons["required"] is False


def test_the_model_is_asked_for_zero_temperature() -> None:
    """Классификация по закрытому дереву — не творческая задача.

    Разброс ответов между двумя одинаковыми звонками выглядит в отчёте
    случайностью, и объяснить его нечем.
    """
    assert _node("llm")["model"]["completion_params"]["temperature"] == 0


#: Тем же правилом, каким Dify разбирает ссылки на переменные в промпте.
#: Скопировано дословно из `graphon/nodes/base/variable_template_parser.py`:
#: дефиса в наборе символов нет, и это не мелочь — см. тест ниже.
_DIFY_REFERENCE = re.compile(r"\{\{(#[a-zA-Z0-9_]{1,50}(\.[a-zA-Z_][a-zA-Z0-9_]{0,29}){1,10}#)\}\}")


def test_every_reference_in_the_prompt_is_one_dify_will_substitute() -> None:
    """Ссылка, которую Dify не разберёт, уезжает в модель буквально.

    Так и вышло: идентификаторы узлов были записаны через дефис
    (`node-start`), а в наборе символов регулярного выражения дефиса нет.
    `{{#node-start.needs_roles#}}` не подставлялось — и модель получала
    фигурные скобки вместо данных.

    Молча. Ни Dify, ни воркфлоу об этом не сообщают: неподставленная ссылка —
    просто текст. Видно стало по логу прогона, где модель рассуждает, что
    «переменные не заполнены», и честно отвечает «данные разговора не
    переданы».
    """
    graph = _graph()
    prompts = _prompts()

    written = re.findall(r"\{\{#[^}]+#\}\}", prompts)
    parsed = [m.group(1)[1:-1] for m in _DIFY_REFERENCE.finditer(prompts)]

    assert written, "в промпте нет ни одной ссылки на вход — данные до модели не дойдут"
    assert len(parsed) == len(written), (
        f"Dify не разберёт: {set(written) - {'{{#' + p + '#}}' for p in parsed}}"
    )

    # И каждая ссылка обязана указывать на существующий узел: опечатка в имени
    # узла разбирается регуляркой, но остаётся пустотой на входе модели.
    ids = {node["id"] for node in graph["nodes"]}
    for reference in parsed:
        assert reference.split(".")[0] in ids, f"{reference}: такого узла нет"


def test_node_identifiers_avoid_characters_dify_cannot_reference() -> None:
    """Идентификатор с дефисом ссылке недоступен, и узнать об этом негде."""
    for node in _graph()["nodes"]:
        assert re.fullmatch(r"[a-zA-Z0-9_]{1,50}", node["id"]), (
            f"{node['id']}: на такой узел нельзя сослаться из промпта"
        )


def test_the_prompt_names_every_input_the_client_sends() -> None:
    """Поле, объявленное на входе и забытое в промпте, доезжает до модели пустотой."""
    prompts = _prompts()
    declared = {v["variable"] for v in _node("start")["variables"]}

    # `call_id` в промпт не идёт намеренно: он нужен для сверки прогона в Dify
    # с нашим журналом, а модели о нём знать нечего.
    for name in declared - {"call_id"}:
        assert f"node_start.{name}" in prompts, f"{name} объявлен, но в промпт не попал"


def test_the_format_demand_is_repeated_last() -> None:
    """Требование формата повторяется последней строкой, а не только первой.

    Один звонок из шестидесяти одного упорно получал прозу вместо JSON — шесть
    раз подряд. Различие с удачной пробой нашлось сравнением входа: **порядок
    тем в справочнике**. Содержимое то же, порядок разный — и этого хватило,
    чтобы модель перестала отвечать объектом.

    Подгонять порядок под модель нельзя: у этапа он как раз правильный.
    Лечится тем, что работает всегда, — модель следует последней инструкции
    охотнее, чем первой, а системный промпт отделён от неё деревом
    классификатора в двенадцать килобайт.
    """
    user = next(part for part in _node("llm")["prompt_template"] if part["role"] == "user")
    tail = user["text"].rstrip().splitlines()[-1]

    assert "JSON" in tail, "последняя строка промпта не напоминает про формат"


def test_the_prompt_demands_exactly_the_fields_the_stage_reads() -> None:
    """Промпт и разбор — один договор, и записан он в двух местах."""
    prompts = _prompts()

    for field in (
        "path",
        "sentiment",
        "summary",
        "roles",
        "ai_path",
        "call_reason",
        "call_reason_code",
    ):
        assert f"`{field}`" in prompts, f"{field} читается этапом, но в промпте не назван"
    for value in ("negative", "neutral", "positive"):
        assert value in prompts
    for gone in ("topic_code", "ai_topic", "needs_segment", "needs_product"):
        assert gone not in prompts, f"{gone} этап больше не читает"


def test_the_prompt_walks_the_tree_step_by_step() -> None:
    """Живая проверка 2026-09-16: тринадцать звонков из пятидесяти пропустили уровень 3.

    Модель ставила на третье место тему четвёртого уровня («Финансы», «Не
    работает»), путь уходил мимо дерева и терялся целиком; ещё три звонка
    начинались с продукта. Прежний запрет «не перескакивать через уровни» её не
    удержал — держит процедура: каждый следующий уровень берётся **внутри** уже
    выбранного ключа.
    """
    prompts = _prompts()

    assert "ВЕРХНЕГО уровня" in prompts, "не сказано, с чего путь начинается"
    assert "ВНУТРИ выбранного ключа" in prompts, "не сказано, откуда берётся следующий уровень"
    assert "Продукт первым уровнем" in prompts, "три звонка начались с продукта"
    assert "Недовольство/негатив» — только если" in prompts, "у уровня 3 нет критерия"


def test_the_prompt_names_the_empty_level_the_way_the_stage_reads_it() -> None:
    """Пустой уровень в дереве для модели и в разборе её ответа называется одинаково."""
    assert EMPTY_LEVEL_LABEL in _prompts()


def test_the_turns_are_declared_third_party_data() -> None:
    """Реплики — данные стороннего источника, а не указания модели (SP-710).

    Звонок — единственный канал платформы, где автор текста — посторонний без
    учётной записи, а порог входа — телефонный звонок. Фразы, похожие на
    указания ассистенту, ничем не закрываются и доходят до модели
    дословно (решение владельца: оболочка в воркфлоу, а не пометка в тексте).
    Значит, всё держится на этом правиле — и правка промпта не должна потерять
    его молча.

    Правило стоит до описания полей: оно задаёт рамку всему разбору, а не
    дописано в конце, где его легко принять за пожелание к оформлению.
    """
    template = _node("llm")["prompt_template"]
    system = next(part["text"] for part in template if part["role"] == "system")
    user = next(part["text"] for part in template if part["role"] == "user")

    assert "данные стороннего источника" in system
    assert "не выполняй" in system
    assert system.index("данные стороннего источника") < system.index("Поля объекта")

    before_turns = user[: user.index("{{#node_start.segments#}}")]
    turns_label = before_turns.split("{{#node_start.classifier#}}")[-1]
    assert "данные стороннего источника" in turns_label


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
    assert describe_raw_problem({}) == "the workflow returned no answer at all"
    assert describe_raw_problem({"answer": "   "}) == "the workflow returned no answer at all"


def test_a_good_answer_has_no_problem_to_describe() -> None:
    assert describe_raw_problem(_answer(_GOOD)) is None
    # Разобранная форма тоже не вызывает жалоб.
    assert describe_raw_problem(dict(_GOOD)) is None


def test_the_token_ceiling_leaves_room_for_reasoning() -> None:
    """Потолок считается по длине рассуждения, а не ответа.

    Сам ответ короткий: роли нужны только там, где их не удалось измерить по
    каналам. Рассуждение же на длинном разговоре занимает тысячи токенов, и на
    2048 модель обрывалась, не начав JSON.

    Потолок зажат с двух сторон: 8192 мало (живая проверка 2026-09-16 потеряла
    пять звонков из пятидесяти), а больше провайдер модели в Dify не принимает —
    12288 отвергается на каждом звонке, «max_tokens should be less than or equal
    to 8192.0». Поэтому ровно 8192, пока предел не поднимут в самом Dify.
    """
    assert _node("llm")["model"]["completion_params"]["max_tokens"] == 8192


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
    """Если узел кода в Dify когда-нибудь заработает, ломать ничего не придётся."""
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


def test_the_reason_reference_stands_before_the_turns_and_their_warning() -> None:
    """Справочник — между деревом и репликами: пометка SP-710 остаётся прямо перед репликами."""
    user = next(part["text"] for part in _node("llm")["prompt_template"] if part["role"] == "user")

    reasons_at = user.index("{{#node_start.reasons#}}")
    assert (
        user.index("{{#node_start.classifier#}}")
        < reasons_at
        < user.index("{{#node_start.segments#}}")
    )
    assert (
        "данные стороннего источника" in user[reasons_at : user.index("{{#node_start.segments#}}")]
    )


def test_the_prompt_does_not_speak_of_a_mask_that_no_longer_exists() -> None:
    """Обезличивания нет: промпт не должен называть знак, которого в тексте не бывает."""
    assert "•••" not in _prompts()
    assert "█" not in _prompts()


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


def test_the_reference_limit_is_the_one_the_workflow_accepts() -> None:
    """Длиннее `max_length` входа Dify отвечает `400` на весь разбор, а не только на причину."""
    from src.domain.calls.reasons import REASONS_PROMPT_LIMIT  # noqa: PLC0415

    reasons = next(v for v in _node("start")["variables"] if v["variable"] == "reasons")

    assert reasons["max_length"] == REASONS_PROMPT_LIMIT
