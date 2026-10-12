"""Обход счётчика сессий: список звонков за сутки без перечисляющего эндпойнта.

Живой API здесь не трогается: доказать обход на нём значило бы простучать
тысячи сессий чужого прода. Механика счётчика снята с площадки отдельно
(запись в журнале 2026-09-12), а здесь на синтетической «вселенной» звонков
проверяется логика: правильные сутки, дрожание дат у границы, дырки в
нумерации, поиск опорной точки.

«Вселенная» — словарь `seq -> (плечо, дата)`. Клиент подставной, но отвечает
как настоящий: `HEAD` существующей сессии даёт дату, несуществующей — «нет».
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from src.infrastructure.sources.ncc.rest import RecordingHead
from src.infrastructure.sources.ncc.sequence import (
    SequenceNotConfigured,
    SequenceWalkSource,
    SessionTemplate,
)
from src.infrastructure.sources.ncc.source import RecordingNotAvailable

EPOCH = "1778844752"
BASE = datetime(2026, 9, 1, tzinfo=UTC)
INTERVAL = 65  # секунд на seq — примерно как на живой площадке
FIRST_SEQ = 100_000


def _universe(count: int) -> dict[int, tuple[int, datetime]]:
    """`seq -> (domain, дата)` для непрерывного ряда звонков.

    Дата растёт с `seq`, но с детерминированным дрожанием ±минута: `seq`
    присваивается в начале звонка, дата (Last-Modified) — в конце, разговоры
    разной длины. Так у соседей порядок дат может нарушаться — ровно то, что на
    живой площадке.
    """
    universe: dict[int, tuple[int, datetime]] = {}
    for i in range(count):
        seq = FIRST_SEQ + i
        jitter = (seq * 37 % 120) - 60
        moment = BASE + timedelta(seconds=i * INTERVAL + jitter)
        universe[seq] = (seq % 4, moment)
    return universe


class _FakeClient:
    """Подставной клиент NCC над синтетической вселенной."""

    def __init__(self, universe: dict[int, tuple[int, datetime]]) -> None:
        self.universe = universe
        self.heads = 0

    async def head_recording(self, session_id: str) -> RecordingHead:
        self.heads += 1
        template, seq = SessionTemplate.parse(session_id)
        # Плечо кодируется в идентификаторе; вытащим его тем же разбором формы.
        domain = int(session_id.split("_domain_")[1].split("_")[0])
        entry = self.universe.get(seq)
        if entry is None or entry[0] != domain:
            return RecordingHead(exists=False)
        return RecordingHead(exists=True, modified=entry[1])

    async def download(self, session_id: str) -> AsyncIterator[bytes]:
        _, seq = SessionTemplate.parse(session_id)
        if seq not in self.universe:
            raise RecordingNotAvailable(session_id)
        yield b"ID3"
        yield session_id.encode()


def _source(
    universe: dict[int, tuple[int, datetime]], *, anchor_seq: int, **kw: Any
) -> tuple[SequenceWalkSource, _FakeClient]:
    client = _FakeClient(universe)
    anchor = f"node_domain_{anchor_seq % 4}_nauss_0_{EPOCH}_{anchor_seq}"
    source = SequenceWalkSource(
        client,  # type: ignore[arg-type]
        anchor_session_id=kw.pop("anchor", anchor),
        timezone=kw.pop("timezone", "UTC"),
        **kw,
    )
    return source, client


def _expected(universe: dict[int, tuple[int, datetime]], day: date, tz: str = "UTC") -> set[str]:
    from zoneinfo import ZoneInfo

    zone = ZoneInfo(tz)
    out: set[str] = set()
    for seq, (domain, moment) in universe.items():
        if moment.astimezone(zone).date() == day:
            out.add(f"node_domain_{domain}_nauss_0_{EPOCH}_{seq}")
    return out


# --------------------------------------------------------------------------- #
# Разбор и построение идентификатора
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "session_id",
    [
        "node_domain_1_nauss_0_1778844752_15533498",  # живая площадка
        "node_domain_0_nauss_0_1659488423_2",  # пример руководства
        "node_1_domain_2_nauss_0_1766092829_1138986",  # форма старой выгрузки
    ],
)
def test_the_anchor_is_parsed_and_rebuilt_without_loss(session_id: str) -> None:
    """Опорный идентификатор разбирается и собирается обратно тем же.

    Обе формы — с номером узла и без — законны; шаблон обязан пережить любую,
    иначе обход не построит соседей.
    """
    template, seq = SessionTemplate.parse(session_id)
    domain = int(session_id.split("_domain_")[1].split("_")[0])
    nauss = int(session_id.split("_nauss_")[1].split("_")[0])

    assert template.build(domain=domain, nauss=nauss, seq=seq) == session_id


def test_a_blank_anchor_is_refused() -> None:
    """Без опорной точки обходить нечего — отказ на сборке, а не пустой обход."""
    with pytest.raises(SequenceNotConfigured, match="SPEECH_NCC_SESSION_ANCHOR"):
        SequenceWalkSource(object(), anchor_session_id="  ", timezone="UTC")  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# Список звонков за сутки
# --------------------------------------------------------------------------- #


async def test_a_day_in_the_middle_is_collected_exactly() -> None:
    """Ровно звонки нужных суток — не соседних.

    Опорная точка вверху ряда, запрашиваются средние сутки. Проверяется точное
    совпадение множества, а не только количество: лишний сосед и потерянный
    звонок — разные ошибки, и обе видны сравнением множеств.
    """
    universe = _universe(4000)  # ~3 суток по 65 с на seq
    anchor_seq = FIRST_SEQ + 3990
    source, _client = _source(universe, anchor_seq=anchor_seq)

    day = (BASE + timedelta(days=1)).date()
    refs = await source.list_calls(day)

    assert {r.session_id for r in refs} == _expected(universe, day)


async def test_every_returned_call_actually_belongs_to_the_day() -> None:
    """Дрожание у границы не должно протащить чужие сутки.

    Строгая фильтрация по дате — вот что отделяет «идём с запасом» от «берём
    лишнее»: обходим за границу суток, но в результат кладём только попавших в
    сутки.
    """
    universe = _universe(4000)
    anchor_seq = FIRST_SEQ + 3990
    source, _client = _source(universe, anchor_seq=anchor_seq)

    day = (BASE + timedelta(days=1)).date()
    refs = await source.list_calls(day)

    for ref in refs:
        assert ref.started_at is not None
        assert ref.started_at.date() == day


async def test_a_gap_in_the_numbering_does_not_end_the_day() -> None:
    """Одиночные дырки бывают — счётчик плотный, но не сплошной.

    Дырка не должна обрывать сбор: сотни звонков за ней потерялись бы молча.
    """
    universe = _universe(4000)
    day = (BASE + timedelta(days=1)).date()
    # Выкалываем несколько seq в середине нужных суток.
    victims = sorted(s for s, (_, m) in universe.items() if m.date() == day)[50:56]
    for seq in victims:
        del universe[seq]
    source, _client = _source(universe, anchor_seq=FIRST_SEQ + 3990)

    refs = await source.list_calls(day)

    assert {r.session_id for r in refs} == _expected(universe, day)
    assert all(int(r.session_id.rsplit("_", 1)[1]) not in victims for r in refs)


async def test_a_quiet_day_returns_nothing_without_hanging() -> None:
    """Пустые сутки — законный ответ, а не бесконечный обход.

    За краем данных стоит череда несуществующих seq; `MAX_GAP` обязан признать
    в ней конец, а не искать вечно.
    """
    universe = _universe(2000)
    source, client = _source(universe, anchor_seq=FIRST_SEQ + 1990)

    # Сутки заведомо позже всех данных.
    empty = (BASE + timedelta(days=30)).date()
    refs = await source.list_calls(empty)

    assert refs == ()
    # Не найдя данных рядом с прикидкой, обход не идёт «вверх на всякий случай»:
    # это `MAX_GAP` пустых проб ради заведомо пустого ответа.
    assert client.heads < 30_000, "обход пустых суток не должен стоить бесконечности"


async def test_the_timezone_decides_which_day_a_midnight_call_belongs_to() -> None:
    """«Сутки» — в поясе колл-центра, а не в UTC.

    Звонок в 23:30 по Москве — это московское сегодня; в UTC он в 20:30 тех же
    суток, но звонок в 01:30 МСК в UTC уже вчера. Пояс обязан решать границу,
    иначе три часа звонков каждый день уезжают в соседние сутки.
    """
    universe = _universe(4000)
    source, _client = _source(universe, anchor_seq=FIRST_SEQ + 3990, timezone="Europe/Moscow")

    day = (BASE + timedelta(days=1)).date()
    refs = await source.list_calls(day)

    assert {r.session_id for r in refs} == _expected(universe, day, tz="Europe/Moscow")


async def test_the_first_probes_land_near_the_target_not_at_the_anchor() -> None:
    """Прикидка по скорости счётчика, а не линейный спуск от опорной точки.

    Без интерполяции обход средних суток шёл бы от вершины ряда вниз тысячами
    проб. Проверяется, что число проб соразмерно суткам, а не расстоянию до
    опоры.
    """
    universe = _universe(4000)
    source, client = _source(universe, anchor_seq=FIRST_SEQ + 3990)

    day = BASE.date()  # первые сутки — дальше всего от опоры
    await source.list_calls(day)

    # Сутки ~1330 seq; с перебором плеч и запасами — тысячи, но не десятки тысяч
    # (что означало бы обход всего ряда от опоры).
    assert client.heads < 20_000


async def test_the_anchor_must_exist() -> None:
    """Опорная точка, которой нет на площадке, — отказ, а не тихий пустой обход.

    Так и выглядит протухшая `epoch`: идентификатор разобрался, а сессии нет.
    """
    universe = _universe(1000)
    missing = FIRST_SEQ + 999_999
    source, _client = _source(universe, anchor_seq=missing)

    with pytest.raises(SequenceNotConfigured, match="опорная сессия"):
        await source.list_calls(BASE.date())


def _sparse(days: int, per_day: int, every: int) -> dict[int, tuple[int, datetime]]:
    """Разреженная вселенная: записи островами, как на живой площадке.

    `every` — шаг счётчика между доступными нам записями: остальные номера
    принадлежат чужим сессиям и отдают `404`.
    """
    universe: dict[int, tuple[int, datetime]] = {}
    step = 86400 // per_day
    for d in range(days):
        for i in range(per_day):
            seq = FIRST_SEQ + (d * per_day + i) * every
            jitter = (seq * 37 % 120) - 60
            universe[seq] = (seq % 4, BASE + timedelta(days=d, seconds=i * step + jitter))
    return universe


async def test_a_stale_anchor_still_finds_a_day_far_from_it() -> None:
    """Опорная точка недельной давности обязана годиться.

    Одна прикидка по средней скорости промахивается тем сильнее, чем дальше
    искомые сутки: скорость неравномерна (день/ночь). На разреженном счётчике
    промах в сотни тысяч номеров означает, что локальный поиск в него не попадёт
    вовсе — и сутки «не найдутся», хотя они есть.

    Поэтому поиск итеративный: каждый шаг считает прыжок от **даты найденного**
    номера. Здесь опора на последних сутках, а спрашиваются первые.
    """
    universe = _sparse(days=10, per_day=120, every=17)
    anchor_seq = max(universe)
    source, _client = _source(universe, anchor_seq=anchor_seq)

    day = BASE.date()
    refs = await source.list_calls(day)

    assert {r.session_id for r in refs} == _expected(universe, day)


async def test_a_night_lull_does_not_end_the_day_early() -> None:
    """Между звонками бывает длинный провал — он не конец суток.

    Порог «конца данных» обязан его пережить, иначе вечерние звонки теряются
    молча: обход остановится на ночной паузе и отчитается о неполном дне.
    """
    universe = _sparse(days=3, per_day=60, every=11)
    day = (BASE + timedelta(days=1)).date()
    # Вырезаем середину суток — провал шире любой обычной дырки.
    doomed = [s for s, (_, m) in universe.items() if m.date() == day and 6 <= m.hour < 14]
    for seq in doomed:
        del universe[seq]
    source, _client = _source(universe, anchor_seq=max(universe))

    refs = await source.list_calls(day)

    assert {r.session_id for r in refs} == _expected(universe, day)
    assert refs, "сутки не должны схлопнуться из-за дневного провала"


# --------------------------------------------------------------------------- #
# Скачивание
# --------------------------------------------------------------------------- #


async def test_audio_streams_by_session_id() -> None:
    universe = _universe(2000)
    source, _client = _source(universe, anchor_seq=FIRST_SEQ + 1990)
    day = (BASE + timedelta(hours=1)).date()
    refs = await source.list_calls(day)
    assert refs

    chunks = [chunk async for chunk in await source.open_audio(refs[0])]

    assert chunks[0] == b"ID3"
    assert chunks[1] == refs[0].session_id.encode()


# --------------------------------------------------------------------------- #
# Проводка: источник должен собираться фабрикой, а не только руками
# --------------------------------------------------------------------------- #


def test_the_factory_actually_builds_this_source() -> None:
    """Источник, не подключённый к фабрике, не существует для приложения.

    Эта проверка появилась после того, как воркер упал на запуске: класс был
    написан и покрыт шестнадцатью тестами, но ветки `sequence` в
    `build_daily_batch_source` не было вовсе, и фабрика отказывала словами «вид
    источника объявлен, но не реализован» — предлагая при этом использовать
    `sequence`.

    Тесты проверяли части и не проверяли сборку. Эта — про сборку.
    """
    from src.infrastructure.sources.ncc.source import build_daily_batch_source

    class _Settings:
        speech_ncc_source_kind = "sequence"
        speech_ncc_session_anchor = "node_domain_1_nauss_0_1778844752_15533498"
        speech_display_timezone = "Europe/Moscow"
        speech_ncc_base_url = "https://ncc.example/api/v2"
        speech_ncc_username = "user"
        speech_ncc_api_token = "token"
        speech_ncc_timeout_seconds = 30.0

    source = build_daily_batch_source(_Settings())  # type: ignore[arg-type]

    assert source.name == "ncc_sequence"
    assert hasattr(source, "list_calls") and hasattr(source, "open_audio")


def test_the_factory_refuses_a_sequence_source_without_an_anchor() -> None:
    """Без опорной точки обходить счётчик не от чего — отказ на старте.

    Воркер, поднявшийся с источником, который всегда вернёт пусто, — это
    «звонков не было» каждую ночь.
    """
    from src.infrastructure.sources.ncc.sequence import SequenceNotConfigured
    from src.infrastructure.sources.ncc.source import build_daily_batch_source

    class _Settings:
        speech_ncc_source_kind = "sequence"
        speech_ncc_session_anchor = "   "
        speech_display_timezone = "Europe/Moscow"
        speech_ncc_base_url = "https://ncc.example/api/v2"
        speech_ncc_username = "user"
        speech_ncc_api_token = "token"
        speech_ncc_timeout_seconds = 30.0

    with pytest.raises(SequenceNotConfigured, match="SPEECH_NCC_SESSION_ANCHOR"):
        build_daily_batch_source(_Settings())  # type: ignore[arg-type]
