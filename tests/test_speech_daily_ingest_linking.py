"""Оператор и проект звонка из суточного приёма — на настоящем Postgres.

Источник подставной, но знает то же, что `reporting_view`: логин оператора и
ключ проекта. Проверяется, что приём заводит их сам — справочники NCC ни того,
ни другого не перечисляют, — дозаполняет звонки, заведённые до связывания, и
соблюдает выключенный сбор проекта.

Нужен `SPEECH_POSTGRES_TEST_URL` с применённой схемой.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import UTC, date, datetime
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.application.dto.daily_ingest import DailyRunReport
from src.di.ingestion import run_day
from src.infrastructure.queue.postgres import PostgresJobQueue
from src.infrastructure.sources.ncc.source import CallRef
from tests.speech_db_support import speech_test_database_url

_TEST_DATABASE_URL = speech_test_database_url()
pytestmark = pytest.mark.skipif(
    not _TEST_DATABASE_URL,
    reason="SPEECH_POSTGRES_TEST_URL is required",
)

DAY = date(2026, 9, 14)
VOLNA = "reporting_view:calls_detail"
TEL7 = "reporting_view:tss_7tel_calls_detail"
SID_A = "node_domain_1_nauss_0_1778844752_17265503"
SID_B = "node_domain_1_nauss_0_1778844752_17265505"
SID_C = "node_domain_0_nauss_0_1778844752_17266458"


def _at(hour: int) -> datetime:
    return datetime(2026, 9, 14, hour, 0, tzinfo=UTC)


def _ref(
    session_id: str,
    *,
    login: str | None = "operator1",
    key: str | None = VOLNA,
    title: str | None = "Волна",
    started: datetime | None = None,
) -> CallRef:
    return CallRef(
        session_id=session_id,
        audio_locator=session_id,
        day=DAY,
        started_at=started,
        operator_login=login,
        project_hint=title,
        project_key=key,
    )


class _Source:
    """Сутки из заготовленных звонков. Скачивание суточному прогону не нужно."""

    name = "test"

    def __init__(self, *refs: CallRef) -> None:
        self._refs = refs

    async def list_calls(self, day: date) -> Sequence[CallRef]:
        return self._refs

    async def open_audio(self, ref: CallRef) -> AsyncIterator[bytes]:  # pragma: no cover
        raise AssertionError("суточный прогон записи не скачивает")
        yield b""


Env = tuple[async_sessionmaker[AsyncSession], PostgresJobQueue]


@pytest.fixture()
async def env() -> AsyncIterator[Env]:
    assert _TEST_DATABASE_URL is not None
    engine = create_async_engine(_TEST_DATABASE_URL)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async def _clean() -> None:
        async with sessions() as session, session.begin():
            await session.execute(text("DELETE FROM speech_jobs"))
            await session.execute(text("DELETE FROM speech_calls"))
            await session.execute(text("DELETE FROM speech_ingest_cursors"))
            await session.execute(text("DELETE FROM speech_operators"))
            await session.execute(text("DELETE FROM speech_ncc_projects"))

    await _clean()
    yield sessions, PostgresJobQueue(sessions, engine=engine)
    await _clean()
    await engine.dispose()


async def _run(env: Env, *refs: CallRef) -> DailyRunReport:
    sessions, queue = env
    return await run_day(DAY, source=_Source(*refs), sessionmaker=sessions, queue=queue)


async def _rows(env: Env, sql: str, **params: Any) -> list[dict[str, Any]]:
    sessions, _ = env
    async with sessions() as session:
        return [dict(row) for row in (await session.execute(text(sql), params)).mappings()]


async def _execute(env: Env, sql: str, **params: Any) -> None:
    sessions, _ = env
    async with sessions() as session, session.begin():
        await session.execute(text(sql), params)


async def test_a_call_gets_its_operator_and_project(env: Env) -> None:
    report = await _run(
        env,
        _ref(SID_A, login="operator1", started=_at(10)),
        _ref(SID_B, login="operator2", started=_at(11)),
        _ref(SID_C, login="operator1", started=_at(9)),
    )

    assert report.enqueued == 3
    operators = await _rows(
        env, "SELECT login, full_name, ncc_uuid FROM speech_operators ORDER BY login"
    )
    assert operators == [
        {"login": "operator1", "full_name": "operator1", "ncc_uuid": None},
        {"login": "operator2", "full_name": "operator2", "ncc_uuid": None},
    ], "имени источник не знает — вместо имени логин, как у синхронизации"
    (project,) = await _rows(
        env,
        "SELECT id, source_key, ncc_uuid, title, direction, ingest_enabled, last_call_at "
        "FROM speech_ncc_projects",
    )
    assert (project["source_key"], project["ncc_uuid"], project["title"]) == (VOLNA, None, "Волна")
    assert (project["direction"], project["ingest_enabled"]) == ("unknown", True)
    assert project["last_call_at"] == _at(11), "последний — самый поздний, а не последний в списке"
    calls = await _rows(
        env,
        "SELECT c.session_id, o.login, c.project_id FROM speech_calls c "
        "JOIN speech_operators o ON o.id = c.operator_id",
    )
    assert {(c["session_id"], c["login"]) for c in calls} == {
        (SID_A, "operator1"),
        (SID_B, "operator2"),
        (SID_C, "operator1"),
    }
    assert {c["project_id"] for c in calls} == {project["id"]}


async def test_two_views_are_two_projects(env: Env) -> None:
    await _run(env, _ref(SID_A, key=VOLNA, title="Волна"), _ref(SID_C, key=TEL7, title="7Телеком"))

    rows = await _rows(
        env,
        "SELECT c.session_id, p.title FROM speech_calls c "
        "JOIN speech_ncc_projects p ON p.id = c.project_id",
    )
    assert {(r["session_id"], r["title"]) for r in rows} == {(SID_A, "Волна"), (SID_C, "7Телеком")}


async def test_a_rerun_links_calls_taken_before_linking(env: Env) -> None:
    """Звонки, заведённые до связывания, дозаполняет повтор суток — без новой обработки."""
    await _run(env, _ref(SID_A, login=None, key=None), _ref(SID_B, login=None, key=None))

    report = await _run(env, _ref(SID_A), _ref(SID_B, login="operator2"))

    assert (report.enqueued, report.already_known) == (0, 2)
    rows = await _rows(
        env,
        "SELECT c.session_id, o.login, p.source_key FROM speech_calls c "
        "LEFT JOIN speech_operators o ON o.id = c.operator_id "
        "LEFT JOIN speech_ncc_projects p ON p.id = c.project_id",
    )
    assert {(r["session_id"], r["login"], r["source_key"]) for r in rows} == {
        (SID_A, "operator1", VOLNA),
        (SID_B, "operator2", VOLNA),
    }
    (jobs,) = await _rows(env, "SELECT count(*) AS n FROM speech_jobs")
    assert jobs["n"] == 2, "повтор суток заданий заново не ставит"


async def test_a_linked_call_is_not_relinked(env: Env) -> None:
    """Повтор с другими оператором и проектом уже связанный звонок не переписывает."""
    await _run(env, _ref(SID_A, login="operator1"))

    await _run(env, _ref(SID_A, login="operator2", key=TEL7, title="7Телеком"))

    (row,) = await _rows(
        env,
        "SELECT o.login, p.source_key FROM speech_calls c "
        "JOIN speech_operators o ON o.id = c.operator_id "
        "JOIN speech_ncc_projects p ON p.id = c.project_id",
    )
    assert (row["login"], row["source_key"]) == ("operator1", VOLNA)


async def test_a_switched_off_project_is_not_collected(env: Env) -> None:
    """Рубильник в админке (ING-08) — действие, а не подпись: звонки проекта не заводятся."""
    await _execute(
        env,
        "INSERT INTO speech_ncc_projects (id, source_key, title, direction, ingest_enabled, "
        "created_at, updated_at) "
        "VALUES (gen_random_uuid(), :key, '7Телеком', 'unknown', FALSE, now(), now())",
        key=TEL7,
    )

    report = await _run(
        env,
        _ref(SID_A, key=VOLNA, title="Волна"),
        _ref(SID_B, key=TEL7, title="7Телеком"),
        _ref(SID_C, key=TEL7, title="7Телеком"),
    )

    assert (report.enqueued, report.collection_off) == (1, 2)
    assert await _rows(env, "SELECT session_id FROM speech_calls") == [{"session_id": SID_A}]
    (cursor,) = await _rows(env, "SELECT last_processed_day FROM speech_ingest_cursors")
    assert cursor["last_processed_day"] == DAY, "выключенный проект — решение, а не сбой суток"


async def test_the_title_follows_the_setting_and_the_markup_stays(env: Env) -> None:
    """Название — из настройки («Волна» уточнится); сегмент и продукт — решение администратора."""
    await _run(env, _ref(SID_A, title="Волна"))
    await _execute(env, "UPDATE speech_ncc_projects SET segment = 'individual', product = 'mobile'")

    await _run(env, _ref(SID_B, title="Волна Мобайл"))

    assert await _rows(env, "SELECT title, segment, product FROM speech_ncc_projects") == [
        {"title": "Волна Мобайл", "segment": "individual", "product": "mobile"}
    ]


async def test_an_operator_with_a_synced_name_keeps_it(env: Env) -> None:
    await _execute(
        env,
        "INSERT INTO speech_operators (id, login, full_name, removed, created_at, updated_at) "
        "VALUES (gen_random_uuid(), 'operator1', 'Оператор Первый', FALSE, now(), now())",
    )

    await _run(env, _ref(SID_A, login="operator1"))

    assert await _rows(
        env,
        "SELECT o.full_name FROM speech_calls c JOIN speech_operators o ON o.id = c.operator_id",
    ) == [{"full_name": "Оператор Первый"}]


async def test_a_source_without_operator_or_project_leaves_them_empty(env: Env) -> None:
    """Локальная выгрузка не знает ни того, ни другого — и справочники не засоряются."""
    await _run(env, _ref(SID_A, login=None, key=None, title=None))

    assert await _rows(env, "SELECT operator_id, project_id FROM speech_calls") == [
        {"operator_id": None, "project_id": None}
    ]
    assert await _rows(env, "SELECT id FROM speech_operators") == []
    assert await _rows(env, "SELECT id FROM speech_ncc_projects") == []
