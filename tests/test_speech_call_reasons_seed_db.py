"""Загрузка справочника причин на живой базе (S-32, SP-325): заводит, обновляет, выключает.

Нужен `SPEECH_POSTGRES_TEST_URL` с применённой схемой.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.application.catalogues.service import seed_call_reasons
from src.domain.calls.reasons import CallReason
from src.infrastructure.persistence.catalogues import PostgresReasonsCatalogue
from tests.speech_db_support import speech_test_database_url

_TEST_DATABASE_URL = speech_test_database_url()
pytestmark = pytest.mark.skipif(
    not _TEST_DATABASE_URL,
    reason="SPEECH_POSTGRES_TEST_URL is required",
)

Sessions = async_sessionmaker[AsyncSession]

K05 = CallReason(code="K05", label="Не работает домашний интернет", hint="ШПД не работает совсем.")
K99 = CallReason(code="K99", label="Другое", hint="Только если не подходит ни один код.")


async def _clean(sessions: Sessions) -> None:
    async with sessions() as session, session.begin():
        await session.execute(text("UPDATE speech_calls SET call_reason_code = NULL"))
        await session.execute(text("DELETE FROM speech_call_reasons"))


@pytest.fixture()
async def sessions() -> AsyncIterator[Sessions]:
    assert _TEST_DATABASE_URL is not None
    engine = create_async_engine(_TEST_DATABASE_URL)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    await _clean(factory)
    yield factory
    await _clean(factory)
    await engine.dispose()


async def _rows(sessions: Sessions) -> dict[str, tuple[str, bool]]:
    async with sessions() as session:
        rows = await session.execute(text("SELECT code, label, is_active FROM speech_call_reasons"))
        return {code: (label, active) for code, label, active in rows}


async def test_a_dry_run_writes_nothing(sessions: Sessions) -> None:
    async with sessions() as session, session.begin():
        report = await seed_call_reasons(PostgresReasonsCatalogue(session), [K05, K99], apply=False)

    assert (report.created, report.updated) == (2, 0)
    assert await _rows(sessions) == {}


async def test_codes_are_created_then_left_alone_then_switched_off_not_deleted(
    sessions: Sessions,
) -> None:
    async with sessions() as session, session.begin():
        first = await seed_call_reasons(PostgresReasonsCatalogue(session), [K05, K99], apply=True)
    async with sessions() as session, session.begin():
        again = await seed_call_reasons(PostgresReasonsCatalogue(session), [K05, K99], apply=True)
    renamed = CallReason(code="K05", label="Нет домашнего интернета", hint=K05.hint)
    async with sessions() as session, session.begin():
        edited = await seed_call_reasons(PostgresReasonsCatalogue(session), [renamed], apply=True)

    assert (first.created, again.created, again.updated) == (2, 0, 0)
    assert (edited.updated, edited.disabled) == (1, ("K99",))
    assert await _rows(sessions) == {
        "K05": ("Нет домашнего интернета", True),
        "K99": ("Другое", False),
    }, "код, которого нет в файле, выключен, а не удалён: на него могут ссылаться звонки"
