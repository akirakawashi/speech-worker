"""Этап `analyze`: путь по классификатору, тональность, резюме и роли от языковой модели.

Воркфлоу подставной. Проверяется не то, как модель думает, а то, как этап
обходится с её ответом — включая ответы, которых схема не допускает.
"""

from __future__ import annotations

import json
import uuid
from typing import Any, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.pipeline.analyze import MAX_OUTPUT_ATTEMPTS, SUMMARY_LIMIT, AnalyzeHandler
from src.application.pipeline.stages import StageDeps, StageError
from src.application.ports.queue import ClaimedJob
from src.di.pipeline import build_stage_deps
from src.domain.calls.contracts import (
    AnalysisStatus,
    JobErrorClass,
    JobKind,
    JobLane,
    ProcessingStatus,
    RoleMethod,
    Sentiment,
    Speaker,
)
from src.infrastructure.clients.analysis import AnalysisError, AnalysisNotConfigured
from src.infrastructure.persistence.classifier import EMPTY_LEVEL_LABEL, label_key
from src.infrastructure.persistence.stages.repository import PostgresStagePersistence

CALL_ID = uuid.UUID("44444444-5555-6666-7777-888888888888")

FIZ, SHPD, CONSULTATION, BROKEN, WIFI, OTHER = (uuid.uuid4() for _ in range(6))
B2B, B2B_MOBILE, EMPTY_ROOT, WALLET = (uuid.uuid4() for _ in range(4))
WIFI_PATH = ["Физ.лицо", "ШПД", "Консультация", "Не работает", "Wi-Fi"]


class _Rows:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> _Rows:
        return self

    def first(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None

    def __iter__(self):  # noqa: ANN204
        return iter(self._rows)


class _Session:
    """Отдаёт заготовленные наборы по порядку обращений и копит записи."""

    def __init__(
        self,
        call: dict[str, Any] | None,
        segments: list[dict[str, Any]],
        nodes: list[dict[str, Any]],
        reasons: list[dict[str, Any]] | None = None,
    ) -> None:
        self._call = [call] if call else []
        self._segments = segments
        self._nodes = nodes
        self._reasons = reasons or []
        self.writes: list[dict[str, Any]] = []
        self.queries: list[str] = []

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _Rows:
        sql = str(statement)
        self.queries.append(sql)
        if params and (
            "analysis_status" in params or "speaker" in params or "role_method" in params
        ):
            self.writes.append(params)
            return _Rows([])
        if "speech_classifier_nodes" in sql:
            return _Rows(self._nodes)
        if "speech_call_reasons" in sql:
            return _Rows(self._reasons)
        if "speech_transcript_segments" in sql:
            return _Rows(self._segments)
        return _Rows(self._call)

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *_exc: object) -> bool:
        return False


class _Workflow:
    def __init__(self, *answers: Any, error: Exception | None = None) -> None:
        self._answers = list(answers)
        self._error = error
        self.inputs: list[dict[str, Any]] = []

    async def run(self, inputs: dict[str, Any]) -> Any:
        self.inputs.append(inputs)
        if self._error is not None:
            raise self._error
        return self._answers.pop(0) if self._answers else {}


def _call(role_method: str = RoleMethod.STEREO_ENERGY.value):
    return {"id": CALL_ID, "role_method": role_method}


def _segments(*texts: str) -> list[dict[str, Any]]:
    return [
        {"seq": i, "speaker": Speaker.UNKNOWN.value, "text_raw": t} for i, t in enumerate(texts)
    ]


def _node(
    node_id: uuid.UUID,
    parent: uuid.UUID | None,
    level: int,
    label: str,
    order: int,
    *,
    active: bool = True,
) -> dict[str, Any]:
    return {
        "id": node_id,
        "parent_id": parent,
        "level": level,
        "label": label,
        "key": label_key(label),
        "sort_order": order,
        "is_active": active,
    }


def _nodes() -> list[dict[str, Any]]:
    """Дерево как его отдаёт база.

    Ветка «Физ.лицо › ШПД › Консультация» с двумя темами; выключенная ветка
    «В2В» — с включённым узлом под ней; «Кошелек» без уровня 1, как его
    ставят операторы.
    """
    return [
        _node(WIFI, BROKEN, 5, "Wi-Fi", 4),
        _node(FIZ, None, 1, "Физ.лицо", 0),
        _node(SHPD, FIZ, 2, "ШПД", 1),
        _node(CONSULTATION, SHPD, 3, "Консультация", 2),
        _node(BROKEN, CONSULTATION, 4, "Не работает", 3),
        _node(OTHER, CONSULTATION, 4, "Другое (обязателен комментарий)", 5),
        _node(B2B, None, 1, "В2В", 6, active=False),
        _node(B2B_MOBILE, B2B, 2, "Мобильная связь", 7),
        _node(EMPTY_ROOT, None, 1, "", 100_000),
        _node(WALLET, EMPTY_ROOT, 2, "Кошелек", 100_000),
    ]


def _deps(session: _Session, workflow: _Workflow | None) -> StageDeps:
    return build_stage_deps(
        settings=None, sessionmaker=lambda: cast(AsyncSession, session), analysis=workflow
    )


def _job() -> ClaimedJob:
    return ClaimedJob(
        id=uuid.uuid4(),
        call_id=CALL_ID,
        kind=JobKind.ANALYZE,
        lane=JobLane.DAILY,
        attempts=1,
        max_attempts=3,
        payload={},
        lease_seconds=300,
    )


def _ok(**overrides: Any) -> dict[str, Any]:
    answer: dict[str, Any] = {"path": WIFI_PATH, "sentiment": "negative", "summary": "Резюме."}
    answer.update(overrides)
    return answer


async def _work(answer: dict[str, Any], *, nodes: list[dict[str, Any]] | None = None):
    session = _Session(_call(), _segments("текст"), _nodes() if nodes is None else nodes)
    return await AnalyzeHandler().work(_job(), _deps(session, _Workflow(answer)))


# --------------------------------------------------------------------------- #
# Основной путь
# --------------------------------------------------------------------------- #


async def test_a_call_gets_its_path_sentiment_and_summary() -> None:
    work = await _work(_ok())

    assert work.node_id == WIFI
    assert work.path == tuple(WIFI_PATH)
    assert work.sentiment is Sentiment.NEGATIVE
    assert work.summary == "Резюме."


async def test_the_transcript_text_reaches_the_model_as_it_is() -> None:
    """Обезличивания нет: модель получает то же, что записал Whisper."""
    session = _Session(_call(), _segments("номер 89780001122"), _nodes())
    workflow = _Workflow(_ok())

    await AnalyzeHandler().work(_job(), _deps(session, workflow))

    sent = workflow.inputs[0]["segments"]
    assert sent == [{"seq": 0, "speaker": "unknown", "text": "номер 89780001122"}]


async def test_the_model_sees_the_switched_on_branches_as_a_tree() -> None:
    """Выключенный узел убирает всю ветку под собой; пустой уровень назван словами."""
    session = _Session(_call(), _segments("текст"), _nodes())
    workflow = _Workflow(_ok())

    await AnalyzeHandler().work(_job(), _deps(session, workflow))

    sent = workflow.inputs[0]
    assert sent["classifier"] == {
        "Физ.лицо": {
            "ШПД": {
                "Консультация": {
                    "Не работает": {"Wi-Fi": {}},
                    "Другое (обязателен комментарий)": {},
                }
            }
        },
        EMPTY_LEVEL_LABEL: {"Кошелек": {}},
    }
    assert set(sent) == {"call_id", "needs_roles", "classifier", "reasons", "segments"}, (
        "справочника тем и вопросов о сегменте и продукте во входе больше нет"
    )


async def test_storing_marks_the_call_done_and_ends_the_chain() -> None:
    session = _Session(_call(), _segments("текст"), _nodes())
    work = await AnalyzeHandler().work(_job(), _deps(session, _Workflow(_ok())))

    session.writes.clear()
    result = await AnalyzeHandler().store(PostgresStagePersistence(session), _job(), work)

    written = session.writes[0]
    assert written["analysis_status"] == AnalysisStatus.DONE.value
    assert written["ai_node_id"] == WIFI
    assert written["sentiment"] == "negative"
    assert (written["ai_suggestion"], written["ai_suggestion_node_id"]) == (None, None)
    assert result.processing_status is ProcessingStatus.DONE
    assert result.next_kind is None, "цепочка кончается"


# --------------------------------------------------------------------------- #
# Путь по дереву
# --------------------------------------------------------------------------- #


async def test_a_path_leaving_the_tree_keeps_what_was_found() -> None:
    """Уровень мимо дерева узлов не заводит: остаётся найденное выше.

    Завести ветку по ответу модели значило бы строить классификатор из её
    догадок — такой узел не видел ни оператор, ни владелец.
    """
    work = await _work(_ok(path=["Физ.лицо", "ШПД", "Консультация", "Выдуманная тема"]))

    assert work.node_id == CONSULTATION
    assert work.path == ("Физ.лицо", "ШПД", "Консультация")


async def test_a_path_off_the_tree_from_the_first_level_leaves_no_node() -> None:
    work = await _work(_ok(path=["Юрлицо", "ШПД"]))

    assert (work.node_id, work.path) == (None, ())
    assert (work.segment, work.product) == (None, None)
    assert work.sentiment is Sentiment.NEGATIVE, "тональность и резюме остаются"


async def test_labels_are_matched_by_key_and_kept_as_the_tree_writes_them() -> None:
    work = await _work(_ok(path=["физ.лицо", "шпд", "консультация", "НЕ  РАБОТАЕТ", "wi-fi"]))

    assert work.node_id == WIFI
    assert work.path == tuple(WIFI_PATH)


async def test_an_empty_level_is_chosen_by_its_name() -> None:
    work = await _work(_ok(path=[EMPTY_LEVEL_LABEL, "Кошелек"]))

    assert work.node_id == WALLET
    assert (work.segment, work.product) == (None, "wallet")


async def test_a_switched_off_branch_is_not_found() -> None:
    work = await _work(_ok(path=["В2В", "Мобильная связь"]))

    assert work.node_id is None


async def test_segment_and_product_are_levels_one_and_two_of_the_path() -> None:
    """Решение S-21 поверх SP-310: сегмент и продукт звонка — уровни 1 и 2 пути."""
    work = await _work(_ok())

    assert (work.segment, work.product) == ("individual", "broadband")


async def test_storing_passes_both_dimensions_to_the_update() -> None:
    """Знает ли их проект, решает SQL: он смотрит на источник в самой строке.

    Проверять условие в приложении значило бы полагаться на то, что между
    чтением и записью никто не сопоставил проект.
    """
    session = _Session(_call(), _segments("текст"), _nodes())
    work = await AnalyzeHandler().work(_job(), _deps(session, _Workflow(_ok())))

    session.writes.clear()
    await AnalyzeHandler().store(PostgresStagePersistence(session), _job(), work)

    written = session.writes[0]
    assert (written["segment"], written["product"]) == ("individual", "broadband")


async def test_an_empty_classifier_is_a_deployment_error_and_the_model_is_not_called() -> None:
    session = _Session(_call(), _segments("текст"), [])
    workflow = _Workflow(_ok())

    with pytest.raises(StageError) as raised:
        await AnalyzeHandler().work(_job(), _deps(session, workflow))

    assert raised.value.error_class is JobErrorClass.INVALID_OUTPUT
    assert "seed_classifier" in str(raised.value)
    assert workflow.inputs == []


async def test_a_long_summary_is_trimmed() -> None:
    work = await _work(_ok(summary="а" * (SUMMARY_LIMIT + 200)))

    assert work.summary is not None
    assert len(work.summary) == SUMMARY_LIMIT


# --------------------------------------------------------------------------- #
# Тема по предложению ИИ помощника
# --------------------------------------------------------------------------- #


async def test_a_suggestion_that_is_a_tree_path_is_that_node() -> None:
    work = await _work(_ok(ai_path=["физ.лицо", "ШПД", "Консультация", "Не работает"]))

    assert work.suggestion == ("Физ.лицо", "ШПД", "Консультация", "Не работает")
    assert work.suggestion_node_id == BROKEN


async def test_a_suggestion_with_a_new_wording_is_kept_in_words_and_stored() -> None:
    session = _Session(_call(), _segments("текст"), _nodes())
    wording = [*WIFI_PATH[:4], "Роутер не раздаёт Wi-Fi"]
    work = await AnalyzeHandler().work(_job(), _deps(session, _Workflow(_ok(ai_path=wording))))

    assert work.suggestion == tuple(wording)
    assert work.suggestion_node_id is None

    session.writes.clear()
    await AnalyzeHandler().store(PostgresStagePersistence(session), _job(), work)
    written = session.writes[0]
    assert json.loads(written["ai_suggestion"]) == wording
    assert written["ai_suggestion_node_id"] is None


# --------------------------------------------------------------------------- #
# Роли
# --------------------------------------------------------------------------- #


async def test_roles_from_the_model_are_ignored_when_they_were_measured() -> None:
    """Измерение энергии каналов надёжнее догадки по тексту.

    У стерео-записи роли уже проставлены, и позволить модели их переписать
    значило бы обменять измерение на мнение.
    """
    session = _Session(_call(role_method=RoleMethod.STEREO_ENERGY.value), _segments("а"), _nodes())
    workflow = _Workflow(_ok(roles=[{"seq": 0, "speaker": "client"}]))

    work = await AnalyzeHandler().work(_job(), _deps(session, workflow))

    assert work.roles == ()
    assert workflow.inputs[0]["needs_roles"] is False


async def test_roles_are_applied_when_nobody_measured_them() -> None:
    """Моно-запись: энергии каналов нет, и роли расставляет модель."""
    session = _Session(_call(role_method=RoleMethod.NONE.value), _segments("а", "б"), _nodes())
    workflow = _Workflow(
        _ok(roles=[{"seq": 0, "speaker": "operator"}, {"seq": 1, "speaker": "client"}])
    )

    work = await AnalyzeHandler().work(_job(), _deps(session, workflow))

    assert workflow.inputs[0]["needs_roles"] is True
    assert work.roles == ((0, Speaker.OPERATOR), (1, Speaker.CLIENT))


async def test_storing_roles_also_records_how_they_were_obtained() -> None:
    """Иначе транскрипт утверждал бы, что роли получены энергией каналов."""
    session = _Session(_call(role_method=RoleMethod.NONE.value), _segments("а"), _nodes())
    work = await AnalyzeHandler().work(
        _job(), _deps(session, _Workflow(_ok(roles=[{"seq": 0, "speaker": "client"}])))
    )

    session.writes.clear()
    await AnalyzeHandler().store(PostgresStagePersistence(session), _job(), work)

    methods = [w for w in session.writes if "role_method" in w]
    assert methods and methods[0]["role_method"] == RoleMethod.LLM_ATTRIBUTION.value


@pytest.mark.parametrize(
    ("roles", "dropped"),
    [([{"seq": 0, "speaker": "client"}], True), ([], False)],
)
async def test_storing_roles_drops_the_script_evaluation_made_on_the_old_ones(
    roles: list[dict[str, Any]], dropped: bool
) -> None:
    """Оценка по скрипту стояла на прежних ролях. Не записав ролей, разбор её не трогает."""
    session = _Session(_call(role_method=RoleMethod.NONE.value), _segments("а"), _nodes())
    work = await AnalyzeHandler().work(_job(), _deps(session, _Workflow(_ok(roles=roles))))

    session.queries.clear()
    await AnalyzeHandler().store(PostgresStagePersistence(session), _job(), work)

    deletes = [sql for sql in session.queries if "speech_call_evaluations" in sql]
    assert bool(deletes) is dropped
    # Последним действием: после ролей и метода, как берёт блокировки команда оценки.
    assert not dropped or session.queries[-1] == deletes[0]


async def test_a_nonsense_role_is_dropped_and_the_rest_survive() -> None:
    session = _Session(_call(role_method=RoleMethod.NONE.value), _segments("а", "б"), _nodes())
    workflow = _Workflow(
        _ok(roles=[{"seq": 0, "speaker": "мимо"}, {"seq": 1, "speaker": "client"}])
    )

    work = await AnalyzeHandler().work(_job(), _deps(session, workflow))

    assert work.roles == ((1, Speaker.CLIENT),)


# --------------------------------------------------------------------------- #
# Невалидный ответ
# --------------------------------------------------------------------------- #


async def test_the_model_gets_a_second_chance_before_the_job_dies() -> None:
    """Первый невалидный ответ языковой модели — обычное дело, а не поломка.

    `invalid_output` терминален: отдав его очереди сразу, мы похоронили бы
    задание после первого же промаха модели.
    """
    session = _Session(_call(), _segments("текст"), _nodes())
    workflow = _Workflow({"path": WIFI_PATH}, _ok())

    work = await AnalyzeHandler().work(_job(), _deps(session, workflow))

    assert len(workflow.inputs) == 2, "второй заход состоялся"
    assert work.sentiment is Sentiment.NEGATIVE


async def test_unusable_answers_end_the_job_without_returning_it_to_the_queue() -> None:
    """Попытки конечны, и исчерпав их, этап отдаёт терминальный класс.

    Проверяется договор, а не число: `MAX_OUTPUT_ATTEMPTS` менялось по
    измерению на корпусе, и тест, зашивший «ровно два вызова», падал бы при
    каждой такой правке, ничего не защищая.

    Существенно здесь другое: повтор живёт **внутри** одной попытки задания.
    Отдай этап `invalid_output` очереди с первого раза — задание умерло бы после
    первого промаха модели, а первый промах у языковой модели обычное дело.
    """
    session = _Session(_call(), _segments("текст"), _nodes())
    unusable = [{"path": ["x"]}] * (MAX_OUTPUT_ATTEMPTS + 2)
    workflow = _Workflow(*unusable)

    with pytest.raises(StageError) as raised:
        await AnalyzeHandler().work(_job(), _deps(session, workflow))

    assert raised.value.error_class is JobErrorClass.INVALID_OUTPUT
    assert len(workflow.inputs) == MAX_OUTPUT_ATTEMPTS, "число вызовов разошлось с договором"


async def test_a_late_good_answer_still_counts() -> None:
    """Промах не хоронит звонок, пока попытки не кончились.

    Ровно этот случай и стоил третьей попытки: на корпусе из шестидесяти одного
    звонка один дважды подряд получил прозу вместо JSON, а на следующем заходе
    разобрался.
    """
    session = _Session(_call(), _segments("текст"), _nodes())
    misses = [{"path": ["x"]}] * (MAX_OUTPUT_ATTEMPTS - 1)
    workflow = _Workflow(*misses, {"path": WIFI_PATH, "sentiment": "negative"})

    work = await AnalyzeHandler().work(_job(), _deps(session, workflow))

    assert work.node_id == WIFI
    assert len(workflow.inputs) == MAX_OUTPUT_ATTEMPTS


@pytest.mark.parametrize(
    "answer",
    [
        {"sentiment": "negative"},
        {"path": [], "sentiment": "negative"},
        {"path": ["  "], "sentiment": "negative"},
        {"path": "Физ.лицо", "sentiment": "negative"},
        {"path": WIFI_PATH, "sentiment": "восторг"},
        {"path": WIFI_PATH, "sentiment": None},
    ],
    ids=lambda a: str(a)[:40],
)
async def test_an_answer_off_the_schema_is_not_patched_with_defaults(answer: dict) -> None:
    """Подставить `neutral` вместо непонятной тональности нельзя.

    Отчёт, в котором нейтральные звонки — это смесь настоящих нейтральных и
    неразмеченных, хуже отчёта, в котором части звонков просто нет.
    """
    session = _Session(_call(), _segments("текст"), _nodes())

    with pytest.raises(StageError):
        await AnalyzeHandler().work(_job(), _deps(session, _Workflow(answer, answer, answer)))


# --------------------------------------------------------------------------- #
# Отказы
# --------------------------------------------------------------------------- #


async def test_a_timeout_keeps_its_class() -> None:
    session = _Session(_call(), _segments("текст"), _nodes())
    workflow = _Workflow(error=AnalysisError("timeout", retryable=True))

    with pytest.raises(StageError) as raised:
        await AnalyzeHandler().work(_job(), _deps(session, workflow))

    assert raised.value.error_class is JobErrorClass.TIMEOUT


async def test_a_rejected_request_is_not_retried_as_a_provider_hiccup() -> None:
    """4xx — неверный ключ или неверная форма входа; повтор даст то же самое."""
    session = _Session(_call(), _segments("текст"), _nodes())
    workflow = _Workflow(error=AnalysisError("http_401", retryable=False))

    with pytest.raises(StageError) as raised:
        await AnalyzeHandler().work(_job(), _deps(session, workflow))

    assert raised.value.error_class is JobErrorClass.PROVIDER_ERROR


async def test_an_unconfigured_workflow_is_a_deployment_error() -> None:
    session = _Session(_call(), _segments("текст"), _nodes())
    workflow = _Workflow(error=AnalysisNotConfigured("нет ключа"))

    with pytest.raises(StageError) as raised:
        await AnalyzeHandler().work(_job(), _deps(session, workflow))

    assert raised.value.error_class is JobErrorClass.INVALID_OUTPUT


async def test_a_call_without_segments_is_refused() -> None:
    session = _Session(_call(), [], _nodes())

    with pytest.raises(StageError) as raised:
        await AnalyzeHandler().work(_job(), _deps(session, _Workflow(_ok())))

    assert raised.value.error_class is JobErrorClass.NOT_FOUND


# --------------------------------------------------------------------------- #
# Причина обращения (S-32)
# --------------------------------------------------------------------------- #

_REASONS = [
    {"code": "K05", "label": "Не работает домашний интернет", "hint": "ШПД не работает совсем."},
    {"code": "K99", "label": "Другое", "hint": "Только если не подходит ни один код."},
]


async def test_the_active_reasons_go_to_the_model_as_a_reference() -> None:
    session = _Session(_call(), _segments("текст"), _nodes(), reasons=_REASONS)
    workflow = _Workflow(_ok())

    await AnalyzeHandler().work(_job(), _deps(session, workflow))

    assert workflow.inputs[0]["reasons"] == (
        "K05 · Не работает домашний интернет — ШПД не работает совсем.\n"
        "K99 · Другое — Только если не подходит ни один код."
    )
    reasons_query = next(q for q in session.queries if "speech_call_reasons" in q)
    assert "is_active" in reasons_query, "выключенный код модель видеть не должна"


async def test_a_reason_from_the_reference_is_stored_with_the_call() -> None:
    session = _Session(_call(), _segments("текст"), _nodes(), reasons=_REASONS)
    answer = _ok(call_reason="Не работает домашний интернет.", call_reason_code="K05")
    work = await AnalyzeHandler().work(_job(), _deps(session, _Workflow(answer)))

    session.writes.clear()
    await AnalyzeHandler().store(PostgresStagePersistence(session), _job(), work)

    written = session.writes[0]
    assert (written["call_reason"], written["call_reason_code"]) == (
        "Не работает домашний интернет.",
        "K05",
    )


async def test_without_a_reference_the_call_is_still_analysed() -> None:
    """Справочник не загружен: вход пустой, поля причины пустые, разбор цел."""
    session = _Session(_call(), _segments("текст"), _nodes())
    workflow = _Workflow(_ok(call_reason="Текст.", call_reason_code="K05"))

    work = await AnalyzeHandler().work(_job(), _deps(session, workflow))

    assert workflow.inputs[0]["reasons"] == ""
    assert (work.call_reason, work.call_reason_code) == (None, None)
    assert work.node_id == WIFI


async def test_a_reference_too_long_for_the_workflow_is_not_sent() -> None:
    """Справочник, правленный в обход загрузки: длиннее предела LLM отказал бы всему разбору."""
    huge = [{"code": f"K{n:02d}", "label": "Подпись", "hint": "Правило. " * 100} for n in range(30)]
    session = _Session(_call(), _segments("текст"), _nodes(), reasons=huge)
    workflow = _Workflow(_ok(call_reason="Текст.", call_reason_code="K05"))

    work = await AnalyzeHandler().work(_job(), _deps(session, workflow))

    assert workflow.inputs[0]["reasons"] == ""
    assert work.call_reason_code is None
    assert work.node_id == WIFI, "разбор звонка цел"
