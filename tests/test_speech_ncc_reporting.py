"""Источник `reporting_view`: список звонков из представлений отчётной базы NCC.

Живая база здесь не трогается — у неё нет TLS, и ходить в чужую базу из тестов
незачем. Форма строк снята с неё 2026-09-15 (журнал разработки, «SP-001a:
представление поставщика»): те же имена полей, секунды с долями, наивное
московское время, повтор `session_id` внутри суток. Значения синтетические.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import asyncpg  # type: ignore[import-untyped]
import pytest

from src.infrastructure.sources.ncc import reporting as reporting
from src.infrastructure.sources.ncc.reporting import (
    ReportingNotConfigured,
    ReportingView,
    ReportingViewSource,
    asyncpg_fetcher,
    build_query,
    parse_views,
)
from src.infrastructure.sources.ncc.source import (
    DailyBatchSource,
    RecordingNotAvailable,
    build_daily_batch_source,
)
from src.setting.config import Settings

MSK = ZoneInfo("Europe/Moscow")
VOLNA = ReportingView(name="calls_detail", project="Волна")
TEL7 = ReportingView(name="tss_7tel_calls_detail", project="7Телеком")
SID_A = "node_domain_1_nauss_0_1778844752_17265503"
SID_B = "node_domain_1_nauss_0_1778844752_17265505"
SID_C = "node_domain_0_nauss_0_1778844752_17266458"


def _row(session_id: str, **fields: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "session_id": session_id,
        "enqueued_time": datetime(2026, 9, 14, 10, 0, 0),
        "nextended": datetime(2026, 9, 14, 10, 2, 30),
        "speakingtime": Decimal("150.54"),
        "nextid": "operator1",
        "uroven_1": "Физ.лицо",
        "uroven_2": None,
        "uroven_3": None,
        "uroven_4": None,
        "uroven_5": None,
    }
    row.update(fields)
    return row


class _Fetch:
    """Подставная выборка: строки по имени представления, запросы записываются."""

    def __init__(self, rows: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, Sequence[object]]] = []

    async def __call__(self, query: str, args: Sequence[object]) -> Sequence[Mapping[str, Any]]:
        self.calls.append((query, args))
        for view, rows in self.rows.items():
            if f'"{view}"' in query:
                return rows
        return []


class _Client:
    async def download(self, session_id: str) -> AsyncIterator[bytes]:
        if session_id == SID_C:
            raise RecordingNotAvailable(session_id)
        yield b"ID3"
        yield session_id.encode()


def _source(
    rows: Mapping[str, Sequence[Mapping[str, Any]]],
    views: Sequence[ReportingView] = (VOLNA,),
    min_talk_seconds: int = 10,
) -> tuple[ReportingViewSource, _Fetch]:
    fetch = _Fetch(rows)
    source = ReportingViewSource(
        fetch=fetch,
        client=_Client(),  # type: ignore[arg-type]
        views=views,
        timezone="Europe/Moscow",
        min_talk_seconds=min_talk_seconds,
    )
    return source, fetch


# --------------------------------------------------------------------------- #
# Настройка представлений
# --------------------------------------------------------------------------- #


def test_views_are_parsed_with_their_projects() -> None:
    assert parse_views(" calls_detail=Волна , tss_7tel_calls_detail=7Телеком ,") == (VOLNA, TEL7)


def test_no_views_is_an_empty_tuple() -> None:
    assert parse_views("") == ()


@pytest.mark.parametrize(
    ("raw", "fragment"),
    [
        ("calls_detail", "needs a project"),
        ("calls_detail=", "needs a project"),
        ('calls_detail";drop=x', "not a plain view name"),
        ("Calls_Detail=Волна", "not a plain view name"),
        ("calls_detail=Волна,calls_detail=Ещё", "listed twice"),
    ],
)
def test_a_bad_views_setting_is_refused(raw: str, fragment: str) -> None:
    """Имя представления уходит в текст запроса, поэтому проверяется строго."""
    with pytest.raises(ReportingNotConfigured, match=fragment):
        parse_views(raw)


def test_a_source_without_views_is_refused() -> None:
    with pytest.raises(ReportingNotConfigured, match="SPEECH_NCC_REPORTING_VIEWS"):
        _source({}, views=())


# --------------------------------------------------------------------------- #
# Запрос
# --------------------------------------------------------------------------- #


def test_the_query_names_its_fields_and_never_reads_personal_data() -> None:
    """Телефоны абонента и перевода и комментарий оператора не выбираются."""
    query = build_query(VOLNA)

    assert "*" not in query
    for personal in ("abonent", "transfer_number", "kommentarii"):
        assert personal not in query
    assert 'FROM public."calls_detail"' in query
    assert "enqueued_time >= $1 AND enqueued_time < $2" in query, "запрос обязан быть оконным"


async def test_the_window_is_the_day_on_the_view_clock_and_the_threshold_is_seconds() -> None:
    source, fetch = _source({"calls_detail": []}, min_talk_seconds=10)

    await source.list_calls(date(2026, 9, 14))

    ((_, args),) = fetch.calls
    assert args == (datetime(2026, 9, 14), datetime(2026, 9, 15), Decimal(10))


# --------------------------------------------------------------------------- #
# Строки в звонки
# --------------------------------------------------------------------------- #


async def test_a_row_becomes_a_call_with_what_the_view_knows() -> None:
    source, _ = _source(
        {
            "calls_detail": [
                _row(SID_A, nextid="  operator1 ", uroven_2="ШПД", uroven_3="  ", uroven_4="Баланс")
            ]
        }
    )

    (ref,) = await source.list_calls(date(2026, 9, 14))

    assert ref.session_id == SID_A
    assert ref.audio_locator == SID_A
    assert ref.day == date(2026, 9, 14)
    assert ref.started_at == datetime(2026, 9, 14, 10, 0, tzinfo=MSK)
    assert ref.ended_at == datetime(2026, 9, 14, 10, 2, 30, tzinfo=MSK)
    assert ref.duration_s == 151, "секунды с долями округляются"
    assert ref.operator_login == "operator1"
    assert ref.project_hint == "Волна"
    assert ref.project_key == "reporting_view:calls_detail", "проект опознаётся представлением"
    assert ref.disposition_path == ("Физ.лицо", "ШПД", "", "Баланс"), (
        "пустой уровень в середине остаётся на своём месте, пустой хвост снимается"
    )


async def test_unknown_values_stay_none() -> None:
    source, _ = _source(
        {
            "calls_detail": [
                _row(SID_A, nextended=None, speakingtime=None, nextid=None, uroven_1=None)
            ]
        }
    )

    (ref,) = await source.list_calls(date(2026, 9, 14))

    assert ref.ended_at is None
    assert ref.duration_s is None
    assert ref.operator_login is None
    assert ref.disposition_path == ()


async def test_each_view_brings_its_own_project() -> None:
    source, fetch = _source(
        {"calls_detail": [_row(SID_A)], "tss_7tel_calls_detail": [_row(SID_C)]},
        views=(VOLNA, TEL7),
    )

    refs = await source.list_calls(date(2026, 9, 14))

    assert {(r.session_id, r.project_hint, r.project_key) for r in refs} == {
        (SID_A, "Волна", "reporting_view:calls_detail"),
        (SID_C, "7Телеком", "reporting_view:tss_7tel_calls_detail"),
    }
    assert len(fetch.calls) == 2


async def test_a_repeated_session_is_one_call() -> None:
    """Порт запрещает дубли; в представлении повтор бывает — и внутри, и между."""
    first = _row(SID_A, nextid="first")
    source, _ = _source(
        {
            "calls_detail": [first, _row(SID_A, nextid="second"), _row(SID_B)],
            "tss_7tel_calls_detail": [_row(SID_A, nextid="third")],
        },
        views=(VOLNA, TEL7),
    )

    refs = await source.list_calls(date(2026, 9, 14))

    assert [r.session_id for r in refs] == [SID_A, SID_B]
    assert refs[0].operator_login == "first", "остаётся первая, самая ранняя строка"
    assert refs[0].project_hint == "Волна"


async def test_a_row_without_a_valid_session_id_is_skipped() -> None:
    source, _ = _source({"calls_detail": [_row(""), _row("not-a-session"), _row(SID_B)]})

    refs = await source.list_calls(date(2026, 9, 14))

    assert [r.session_id for r in refs] == [SID_B]


async def test_a_day_without_calls_is_empty_not_an_error() -> None:
    source, _ = _source({"calls_detail": []})
    assert await source.list_calls(date(2026, 9, 14)) == ()


async def test_a_database_failure_is_raised_not_turned_into_an_empty_day() -> None:
    async def broken(query: str, args: Sequence[object]) -> Sequence[Mapping[str, Any]]:
        raise ConnectionError("pooler said no")

    source = ReportingViewSource(
        fetch=broken,
        client=_Client(),  # type: ignore[arg-type]
        views=(VOLNA,),
        timezone="Europe/Moscow",
        min_talk_seconds=10,
    )
    with pytest.raises(ConnectionError):
        await source.list_calls(date(2026, 9, 14))


async def test_an_empty_level_inside_the_path_keeps_the_levels_in_place() -> None:
    """У «Кошелька» оператор оставляет пустым уровень 1 — уровни не сдвигаются (SP-310)."""
    source, _ = _source(
        {
            "calls_detail": [
                _row(
                    SID_A,
                    uroven_1=None,
                    uroven_2="Кошелек",
                    uroven_3="Консультация",
                    uroven_4="Финансы",
                )
            ]
        }
    )

    (ref,) = await source.list_calls(date(2026, 9, 14))

    assert ref.disposition_path == ("", "Кошелек", "Консультация", "Финансы")


# --------------------------------------------------------------------------- #
# Классификации для пополнения классификатора (SP-310)
# --------------------------------------------------------------------------- #


def test_the_classification_query_reads_only_levels_and_counts() -> None:
    query = reporting.build_classification_query(VOLNA)

    assert "*" not in query.replace("count(*)", "")
    for field in ("session_id", "nextid", "abonent", "transfer_number", "kommentarii"):
        assert field not in query
    assert "GROUP BY uroven_1, uroven_2, uroven_3, uroven_4, uroven_5" in query
    assert "enqueued_time >= $1 AND enqueued_time < $2" in query, "запрос обязан быть оконным"
    assert "speakingtime" not in query, "порога нет: пополняется всем, что ставят операторы"


async def test_classifications_are_summed_across_views() -> None:
    wifi = {
        "uroven_1": "Физ.лицо",
        "uroven_2": "ШПД",
        "uroven_3": "Консультация",
        "uroven_4": "Не работает",
        "uroven_5": "Wi-Fi",
        "calls": 7,
    }
    wallet = {
        "uroven_1": None,
        "uroven_2": "Кошелек",
        "uroven_3": "Консультация",
        "uroven_4": " ",
        "uroven_5": None,
        "calls": 2,
    }
    unclassified = dict.fromkeys(("uroven_1", "uroven_2", "uroven_3", "uroven_4", "uroven_5"))
    source, fetch = _source(
        {
            "calls_detail": [wifi, wallet, {**unclassified, "calls": 40}],
            "tss_7tel_calls_detail": [wifi],
        },
        views=(VOLNA, TEL7),
    )

    counts = await source.list_classifications(date(2026, 9, 14))

    assert counts == {
        ("Физ.лицо", "ШПД", "Консультация", "Не работает", "Wi-Fi"): 14,
        ("", "Кошелек", "Консультация"): 2,
    }, "звонки без классификации пути не дают"
    assert [args for _, args in fetch.calls] == [(datetime(2026, 9, 14), datetime(2026, 9, 15))] * 2


# --------------------------------------------------------------------------- #
# Запись
# --------------------------------------------------------------------------- #


async def test_audio_comes_from_rest_by_session_id() -> None:
    source, _ = _source({"calls_detail": [_row(SID_A)]})
    (ref,) = await source.list_calls(date(2026, 9, 14))

    chunks = [chunk async for chunk in await source.open_audio(ref)]

    assert b"".join(chunks) == b"ID3" + SID_A.encode()


async def test_a_missing_recording_is_reported_as_such() -> None:
    source, _ = _source({"calls_detail": [_row(SID_C)]})
    (ref,) = await source.list_calls(date(2026, 9, 14))

    with pytest.raises(RecordingNotAvailable):
        async for _ in await source.open_audio(ref):
            pass


def test_the_source_satisfies_the_port() -> None:
    source, _ = _source({})
    assert isinstance(source, DailyBatchSource)


# --------------------------------------------------------------------------- #
# Соединение
# --------------------------------------------------------------------------- #


class _Transaction:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    async def __aenter__(self) -> _Transaction:
        self.connection.events.append("begin")
        return self

    async def __aexit__(self, *exc: object) -> bool:
        self.connection.events.append("end")
        return False


class _Connection:
    def __init__(self, *, fail: bool = False) -> None:
        self.events: list[object] = []
        self.readonly: bool | None = None
        self.fail = fail

    def transaction(self, *, readonly: bool) -> _Transaction:
        self.readonly = readonly
        return _Transaction(self)

    async def execute(self, query: str) -> str:
        self.events.append(query)
        return "SET"

    async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]:
        if self.fail:
            raise ConnectionError("gone")
        self.events.append(("fetch", args))
        return [{"session_id": SID_A}]

    async def close(self) -> None:
        self.events.append("close")


async def test_the_fetcher_reads_in_a_read_only_transaction_without_prepared_statements(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Пулер перед базой отвергает параметры запуска и не держит подготовленных запросов."""
    connection = _Connection()
    seen: dict[str, Any] = {}

    async def connect(**kwargs: Any) -> _Connection:
        seen.update(kwargs)
        return connection

    monkeypatch.setattr(asyncpg, "connect", connect)
    fetch = asyncpg_fetcher("postgresql://reader@ncc.example:5432/rep", password="secret")

    rows = await fetch("SELECT 1", (1, 2))

    assert rows == [{"session_id": SID_A}]
    assert seen["dsn"] == "postgresql://reader@ncc.example:5432/rep"
    assert seen["password"] == "secret"
    assert seen["statement_cache_size"] == 0
    assert "server_settings" not in seen, "пулер отвергает параметры запуска"
    assert connection.readonly is True
    assert connection.events[0] == "begin"
    assert "statement_timeout" in str(connection.events[1])
    assert connection.events[-1] == "close"


async def test_the_fetcher_closes_the_connection_when_the_query_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(fail=True)

    async def connect(**kwargs: Any) -> _Connection:
        return connection

    monkeypatch.setattr(asyncpg, "connect", connect)
    fetch = asyncpg_fetcher("postgresql://reader@ncc.example:5432/rep", password="secret")

    with pytest.raises(ConnectionError):
        await fetch("SELECT 1", ())
    assert connection.events[-1] == "close"


# --------------------------------------------------------------------------- #
# Сборка по настройкам
# --------------------------------------------------------------------------- #

REST = {
    "speech_ncc_base_url": "https://ncc.example:8443/api/v2",
    "speech_ncc_username": "reader",
    "speech_ncc_api_token": "token",
}


def test_the_factory_names_every_missing_setting() -> None:
    settings = Settings(  # type: ignore[call-arg]
        _env_file=None, speech_ncc_source_kind="reporting_view", **REST
    )
    with pytest.raises(RuntimeError) as error:
        build_daily_batch_source(settings)
    for name in (
        "SPEECH_NCC_REPORTING_DATABASE_URL",
        "SPEECH_NCC_REPORTING_DB_PASSWORD",
        "SPEECH_NCC_REPORTING_VIEWS",
    ):
        assert name in str(error.value)


def test_the_factory_builds_the_reporting_view_source() -> None:
    settings = Settings(  # type: ignore[call-arg]
        _env_file=None,
        speech_ncc_source_kind="reporting_view",
        speech_ncc_reporting_database_url="postgresql://reader@ncc.example:5432/rep",
        speech_ncc_reporting_db_password="secret",
        speech_ncc_reporting_views="calls_detail=Волна,tss_7tel_calls_detail=7Телеком",
        **REST,
    )
    source = build_daily_batch_source(settings)

    assert isinstance(source, ReportingViewSource)
    assert source.name == "ncc_reporting_view"


def test_the_module_does_not_import_the_driver_at_import_time() -> None:
    """Драйвер импортируется при первой выборке: модуль нужен и там, где базы нет."""
    assert "asyncpg" not in vars(reporting)
