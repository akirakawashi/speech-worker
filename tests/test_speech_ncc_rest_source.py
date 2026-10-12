"""Источник суточного пакета поверх REST API NCC.

Цепочка, ради которой он написан: кейсы проекта за диапазон дат → `sessionId`
в кейсе → запись по `sessionId`. Транспорт подставной, формы ответов взяты из
руководства NCC — проверить их на живой площадке нельзя до того, как появится
UUID проекта, а его в API не отдаёт ни один эндпойнт.

Отсюда и форма проверок: разбор написан так, чтобы пережить обе документированные
формы ответа, и здесь проверяется именно это, а не одна угаданная.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pytest

from src.infrastructure.sources.ncc.rest import session_ids_in
from src.infrastructure.sources.ncc.source import RecordingNotAvailable, RestCasesSource

SESSION_A = "node_1_domain_2_nauss_0_1766092829_1138986"
SESSION_B = "node_2_domain_1_nauss_1_1766092823_1603045"
PROJECT = "corebo00000000000n63prtlf4p883fo"


class _Client:
    """Клиент, отдающий заготовленные страницы кейсов."""

    def __init__(self, pages: list[list[dict[str, Any]]]) -> None:
        self.pages = pages
        self.asked: list[tuple[str, str, str, int]] = []

    async def cases(
        self, project: str, *, since: datetime, until: datetime, page: int = 0
    ) -> list[dict[str, Any]]:
        self.asked.append((project, since.isoformat(), until.isoformat(), page))
        return self.pages[page] if page < len(self.pages) else []

    async def download(self, session_id: str) -> Any:
        # Асинхронный генератор, как у настоящего клиента: вызов возвращает
        # итератор сразу, без ожидания. Сделай его корутиной — и подделка
        # разойдётся с тем, что проверяет.
        yield b"ID3"
        yield session_id.encode()


def _source(pages: list[list[dict[str, Any]]], **kwargs: Any) -> RestCasesSource:
    return RestCasesSource(
        _Client(pages),  # type: ignore[arg-type]  # подставной клиент того же вида
        kwargs.pop("projects", [PROJECT]),
        timezone=kwargs.pop("timezone", "Europe/Moscow"),
    )


# --------------------------------------------------------------------------- #
# Поиск идентификатора сессии в кейсе
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "case",
    [
        pytest.param({"sessionId": SESSION_A}, id="в корне"),
        pytest.param({"customForm": {"default": {"sessionId": SESSION_A}}}, id="в анкете"),
        pytest.param({"sessionId": {"value": SESSION_A}}, id="в обёртке v1"),
        pytest.param({"a": {"b": {"c": {"session_id": SESSION_A}}}}, id="в глубине"),
    ],
)
def test_the_session_identifier_is_found_wherever_the_manual_puts_it(case: dict) -> None:
    """Руководство показывает `sessionId` в разных местах ответа.

    Какой именно формой он придёт в списке кейсов конкретной установки,
    проверить нельзя до появления доступа к проекту. Фиксированный путь был бы
    догадкой, которая молча вернёт пусто, — а пусто здесь означает «за сутки
    звонков не было».
    """
    assert session_ids_in(case) == [SESSION_A]


def test_nothing_is_invented_when_there_is_no_identifier() -> None:
    """Не найдено — значит не найдено. Пустой список, а не выдумка."""
    assert session_ids_in({"uuid": "ocpcas0000", "title": "кейс", "operator": "ivanov"}) == []


def test_a_hostile_answer_cannot_spin_the_search_forever() -> None:
    """Ответ приходит снаружи, и рекурсия по чужим данным обязана иметь предел."""
    deep: dict[str, Any] = {"sessionId": SESSION_A}
    for _ in range(50):
        deep = {"nested": deep}

    assert session_ids_in(deep) == []


# --------------------------------------------------------------------------- #
# Перечисление суток
# --------------------------------------------------------------------------- #


async def test_the_day_is_asked_in_the_call_centre_timezone() -> None:
    """«За вчера» для человека — это его вчера.

    В UTC сутки съезжают на три часа, то есть три часа вечерних звонков уезжают
    в соседний день — и не попадают ни в один суточный прогон.
    """
    source = _source([[{"sessionId": SESSION_A}]])

    await source.list_calls(date(2026, 9, 10))

    project, since, until, page = source._client.asked[0]  # noqa: SLF001
    assert project == PROJECT
    assert since == "2026-09-10T00:00:00+03:00"
    assert until == "2026-09-11T00:00:00+03:00"
    assert page == 0


async def test_pages_are_walked_until_they_run_out() -> None:
    """Полторы тысячи звонков в день — это пятнадцать страниц по сто."""
    source = _source([[{"sessionId": SESSION_A}], [{"sessionId": SESSION_B}], []])

    refs = await source.list_calls(date(2026, 9, 10))

    assert {ref.session_id for ref in refs} == {SESSION_A, SESSION_B}
    assert [asked[3] for asked in source._client.asked] == [0, 1, 2]  # noqa: SLF001


async def test_the_same_session_twice_is_one_call() -> None:
    """Кейс может прийти дважды — страницами или из разных проектов.

    Дубль на этом уровне превратился бы в два звонка с одним идентификатором, и
    дальше его ловит только уникальный ключ в базе — то есть отказом приёма.
    """
    source = _source([[{"sessionId": SESSION_A}, {"customForm": {"sessionId": SESSION_A}}], []])

    refs = await source.list_calls(date(2026, 9, 10))

    assert len(refs) == 1


async def test_rubbish_instead_of_an_identifier_is_not_taken_for_a_call() -> None:
    """Форма идентификатора проверяется до того, как он станет звонком."""
    source = _source([[{"sessionId": "../../etc/passwd"}, {"sessionId": SESSION_A}], []])

    refs = await source.list_calls(date(2026, 9, 10))

    assert [ref.session_id for ref in refs] == [SESSION_A]


async def test_a_case_created_on_another_day_is_not_counted_as_todays_call() -> None:
    """Диапазон фильтрует по последнему изменению, а не по времени звонка.

    Кейс, созданный позавчера и тронутый вчера, попадёт в выборку. Считать его
    вчерашним звонком нельзя: карточка покажет неверную дату, а суточный отчёт
    — неверное число.
    """
    source = _source(
        [
            [
                {"sessionId": SESSION_A, "creationDate": "2026-09-10T11:00:00+03:00"},
                {"sessionId": SESSION_B, "creationDate": "2026-09-08T11:00:00+03:00"},
            ],
            [],
        ]
    )

    refs = await source.list_calls(date(2026, 9, 10))

    assert [ref.session_id for ref in refs] == [SESSION_A]
    assert refs[0].started_at is not None


async def test_a_case_without_a_creation_date_still_becomes_a_call() -> None:
    """Неизвестное время — не повод терять звонок.

    Карточка покажет дату без времени, и это честнее, чем полночь у всех
    подряд, и несравнимо лучше, чем потерянный разговор.
    """
    source = _source([[{"sessionId": SESSION_A, "creationDate": "не дата"}], []])

    refs = await source.list_calls(date(2026, 9, 10))

    assert len(refs) == 1
    assert refs[0].started_at is None


async def test_the_operator_and_the_project_travel_with_the_call() -> None:
    """Оператор и очередь — половина фильтров в интерфейсе."""
    source = _source([[{"sessionId": SESSION_A, "operator": "ivanov"}], []])

    refs = await source.list_calls(date(2026, 9, 10))

    assert refs[0].operator_login == "ivanov"
    assert refs[0].project_hint == PROJECT


async def test_a_source_without_projects_refuses_to_be_built() -> None:
    """Источник без проектов перечислял бы пустоту.

    Пустое перечисление неотличимо от выходного дня, и обнаруживается это тем,
    что за неделю в поиске ничего не появилось.
    """
    with pytest.raises(RuntimeError, match="SPEECH_NCC_PROJECTS"):
        _source([[]], projects=[])


async def test_audio_comes_from_the_client_by_session_id() -> None:
    """Локатор записи и есть идентификатор сессии: другого адреса у неё нет."""
    source = _source([[{"sessionId": SESSION_A}], []])
    refs = await source.list_calls(date(2026, 9, 10))

    chunks = [chunk async for chunk in await source.open_audio(refs[0])]

    assert chunks[0] == b"ID3"
    assert chunks[1] == SESSION_A.encode()


async def test_a_missing_recording_passes_through_as_itself() -> None:
    """Отличать «записи нет» от «источник сломан» — обязанность этапа.

    Подменить здесь одно другим значило бы пометить звонок как безнадёжный из-за
    сбоя сети.

    Отказ приходит на **первом куске**, а не при открытии: REST отвечает
    заголовками раньше тела, и `404` виден только когда поток тронут. Этап это
    предусматривает — `_next_chunk` пропускает `RecordingNotAvailable` наверх
    как есть, — и проверка идёт тем же путём, каким пойдёт конвейер.
    """

    class _Missing(_Client):
        async def download(self, session_id: str) -> Any:
            raise RecordingNotAvailable(session_id)
            yield b""  # pragma: no cover - делает функцию генератором

    source = RestCasesSource(
        _Missing([[{"sessionId": SESSION_A}], []]),  # type: ignore[arg-type]
        [PROJECT],
        timezone="Europe/Moscow",
    )
    refs = await source.list_calls(date(2026, 9, 10))

    stream = await source.open_audio(refs[0])
    with pytest.raises(RecordingNotAvailable):
        await anext(stream)
